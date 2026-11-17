#!/usr/bin/env python3
"""
端到端自测（真实 PostgreSQL + 真实 HTTP API + 真实进程崩溃）：

迁移：
  1. 慢速复制 + 并发直接 SQL 增删改 → 切换后父表内容与期望逐行一致
  2. 一次只迁一个月（并发启动第二个 → 409）
  3. 已完成月份再次迁移 → 409 且数据不变
  4. copying / finalizing 阶段故障注入 → 父表完整可查 → 重试完成
  5. SIGKILL 应用进程（复制进行中）→ 父表仍完整可查可写 → 重启后重试完成
  6. 收尾检查：默认分区无残留、无重复主键、触发器/增量清理干净

观测交付：
  7. 提交受理 → ready → 下载 CSV 与冻结时点快照**字节级**一致（排序/行数/SHA-256）
  8. 同号同月重复提交与并发提交复用原快照；同号换月 409；ready 后重试 409
  9. 冻结后篡改现行观测 → 已交付 CSV 字节不变（不随后续更正变化）
 10. 慢生成进行中：同月迁移照样完成、采集写入不报错；CSV 仍等于冻结快照
 11. generating 故障注入 → failed → 改现行观测 → 重试 → 仍用原固定数据
 12. 生成中进程硬退出 → 停机期间改观测 → 重启自动续跑 → CSV 等于冻结快照；
     已完成交付重启后下载字节不变；生成中下载被 409 拒绝

运行：DATABASE_URL=... python3 scripts/selftest.py
（脚本自己拉起/杀掉 uvicorn 进程；要求 DATABASE_URL 指向已初始化的库）
"""

from __future__ import annotations

import hashlib
import os
import random
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import psycopg
import requests

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8010")
DSN = os.environ.get(
    "DATABASE_URL", "postgresql://envmon@127.0.0.1:5544/envmon"
)
APP_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "app"))
APP_PORT = int(os.environ.get("APP_PORT", "8010"))
STATIONS = [f"ST{i:03d}" for i in range(1, 9)]

_failures: list[str] = []


def check(name: str, cond: bool, detail: str = ""):
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f" — {detail}" if detail else ""), flush=True)
    if not cond:
        _failures.append(f"{name}: {detail}")


def db():
    return psycopg.connect(DSN, autocommit=True, options="-c timezone=UTC")


def month_range(month: str):
    lo = datetime.fromisoformat(month + "-01").replace(tzinfo=timezone.utc)
    hi = (lo.replace(day=28) + timedelta(days=8)).replace(day=1)
    return lo, hi


def snapshot(month: str) -> dict:
    """父表该月全部内容（ground truth，直接 SQL）。"""
    lo, hi = month_range(month)
    with db() as c:
        rows = c.execute(
            "SELECT station_id, ts, value FROM observations"
            " WHERE ts >= %s AND ts < %s",
            (lo, hi),
        ).fetchall()
    return {(r[0], r[1]): r[2] for r in rows}


def default_count(month: str) -> int:
    lo, hi = month_range(month)
    with db() as c:
        return c.execute(
            "SELECT count(*) FROM observations_default WHERE ts >= %s AND ts < %s",
            (lo, hi),
        ).fetchone()[0]


def run_row(month: str):
    with db() as c:
        return c.execute(
            "SELECT status, copied_rows, applied_delta, error"
            " FROM migration_runs WHERE month = %s::date", (month + "-01",)
        ).fetchone()


def partition_name(month: str) -> str:
    return "observations_" + month.replace("-", "_")


def partition_exists(month: str) -> bool:
    with db() as c:
        return bool(
            c.execute(
                "SELECT 1 FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid"
                " WHERE i.inhparent = 'observations'::regclass AND c.relname = %s",
                (partition_name(month),),
            ).fetchone()
        )


def partition_count(month: str) -> int:
    with db() as c:
        return c.execute(
            f"SELECT count(*) FROM {partition_name(month)}"
        ).fetchone()[0]


class ChaosWriter(threading.Thread):
    """独立采集连接：直接 SQL 随机 新增/更正/删除，并维护期望结果。"""

    def __init__(self, month: str, expected: dict):
        super().__init__(daemon=True)
        self.month = month
        self.expected = expected
        self.stop_event = threading.Event()
        self.ops = 0
        self.errors: list[str] = []
        self.lo, self.hi = month_range(month)

    def run(self):
        try:
            conn = psycopg.connect(DSN, autocommit=True,
                                   options="-c timezone=UTC")
        except Exception as e:  # noqa: BLE001
            self.errors.append(f"connect: {e!r}")
            return
        with conn:
            while not self.stop_event.is_set():
                op = random.choices(["insert", "update", "delete"],
                                    weights=[5, 3, 2])[0]
                try:
                    self._do(conn, op)
                    self.ops += 1
                except Exception as e:  # noqa: BLE001
                    self.errors.append(f"{op}: {e!r}")
                time.sleep(random.uniform(0.01, 0.06))

    def _do(self, conn, op: str):
        if op == "insert":
            station = random.choice(STATIONS)
            span = int((self.hi - self.lo).total_seconds())
            ts = self.lo + timedelta(seconds=random.randrange(span))
            value = random.randint(0, 100)
            row = conn.execute(
                "INSERT INTO observations (station_id, ts, value)"
                " VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"
                " RETURNING station_id, ts, value",
                (station, ts, value),
            ).fetchone()
            if row:
                self.expected[(row[0], row[1])] = row[2]
        elif op == "update":
            row = conn.execute(
                "SELECT station_id, ts FROM observations"
                " WHERE ts >= %s AND ts < %s ORDER BY random() LIMIT 1",
                (self.lo, self.hi),
            ).fetchone()
            if row:
                value = random.randint(0, 100)
                conn.execute(
                    "UPDATE observations SET value = %s"
                    " WHERE station_id = %s AND ts = %s",
                    (value, row[0], row[1]),
                )
                self.expected[(row[0], row[1])] = value
        else:
            row = conn.execute(
                "SELECT station_id, ts FROM observations"
                " WHERE ts >= %s AND ts < %s ORDER BY random() LIMIT 1",
                (self.lo, self.hi),
            ).fetchone()
            if row:
                conn.execute(
                    "DELETE FROM observations WHERE station_id = %s AND ts = %s",
                    (row[0], row[1]),
                )
                self.expected.pop((row[0], row[1]), None)


# ------------------------------------------------------------------ app 进程
_app_proc: subprocess.Popen | None = None


def start_app() -> subprocess.Popen:
    global _app_proc
    os.makedirs("/tmp/pg-runtime", exist_ok=True)
    logf = open("/tmp/pg-runtime/app.log", "ab")
    _app_proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "main:app",
         "--host", "127.0.0.1", "--port", str(APP_PORT)],
        cwd=APP_DIR, stdout=logf, stderr=subprocess.STDOUT,
        env={**os.environ, "DATABASE_URL": DSN,
             "DELIVERY_DIR": os.environ.get(
                 "DELIVERY_DIR", "/tmp/pg-runtime/deliveries")},
    )
    wait_healthy()
    return _app_proc


def crash_app():
    """通过调试端点让应用硬退出（os._exit，等价于宕机/SIGKILL 的效果）。"""
    global _app_proc
    try:
        requests.post(f"{BASE}/api/debug/crash", timeout=3)
    except requests.RequestException:
        pass
    if _app_proc:
        try:
            _app_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
    _app_proc = None
    # 等端口彻底拒绝连接
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            requests.get(f"{BASE}/api/state", timeout=1)
            time.sleep(0.3)
        except requests.RequestException:
            return
    raise RuntimeError("应用进程未能退出")


def wait_healthy(timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(f"{BASE}/api/state", timeout=2).ok:
                return
        except requests.RequestException:
            pass
        time.sleep(0.3)
    raise RuntimeError("应用未在预期时间内就绪")


def api_migrate(month: str, **kw) -> requests.Response:
    body = {"month": month, "batch_size": 200, "sleep_ms": 80}
    body.update(kw)
    return requests.post(f"{BASE}/api/migrate", json=body, timeout=10)


def wait_status(month: str, want: set[str], timeout=180) -> str:
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = run_row(month)
        if row and row[0] in want:
            return row[0]
        time.sleep(0.4)
    raise RuntimeError(f"{month} 等待状态 {want} 超时，当前 {run_row(month)}")


# ------------------------------------------------------------------ 交付辅助
def api_delivery_submit(no: str, month: str, **kw) -> requests.Response:
    body = {"delivery_no": no, "month": month, "sleep_ms": 0}
    body.update(kw)
    return requests.post(f"{BASE}/api/deliveries", json=body, timeout=30)


def delivery_row(no: str) -> dict | None:
    r = requests.get(f"{BASE}/api/deliveries/{no}", timeout=10)
    return r.json() if r.ok else None


def wait_delivery(no: str, want: set[str], timeout=120) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = delivery_row(no)
        if st and st["status"] in want:
            return st
        time.sleep(0.4)
    raise RuntimeError(f"交付 {no} 等待状态 {want} 超时，当前 {delivery_row(no)}")


def download(no: str) -> requests.Response:
    return requests.get(f"{BASE}/api/deliveries/{no}/download", timeout=30)


def expected_csv_bytes(snap: dict) -> bytes:
    """冻结快照 dict → 期望的 CSV 字节（与应用生成格式严格一致）。"""
    lines = ["station_id,ts,value"]
    for (station, ts), value in sorted(snap.items(),
                                       key=lambda kv: (kv[0][0], kv[0][1])):
        lines.append(f"{station},{ts.isoformat()},{value}")
    return ("\n".join(lines) + "\n").encode("utf-8")


def check_csv(name: str, body: bytes, snap: dict, reg: dict):
    want = expected_csv_bytes(snap)
    check(f"{name}: CSV 字节与冻结快照完全一致（按站号,时刻排序）",
          body == want,
          f"{len(body)} vs {len(want)} 字节")
    check(f"{name}: 登记行数与实际一致",
          reg["csv_rows"] == len(snap) and reg["row_count"] == len(snap),
          f"csv_rows={reg['csv_rows']} row_count={reg['row_count']} 期望={len(snap)}")
    check(f"{name}: 登记 SHA-256 与文件实际一致",
          reg["csv_sha256"] == hashlib.sha256(body).hexdigest(),
          reg["csv_sha256"][:16])
    check(f"{name}: 登记字节数与实际一致",
          reg["csv_bytes"] == len(body),
          f"{reg['csv_bytes']} vs {len(body)}")


def mutate_month(month: str, tag: str):
    """直接 SQL 篡改该月现行观测（增/改/删各若干），模拟后续更正。"""
    lo, hi = month_range(month)
    with db() as c:
        u = c.execute(
            "UPDATE observations SET value = value + 1000"
            " WHERE (station_id, ts) IN (SELECT station_id, ts FROM observations"
            "  WHERE ts >= %s AND ts < %s ORDER BY station_id, ts LIMIT 3)",
            (lo, hi),
        ).rowcount
        d = c.execute(
            "DELETE FROM observations"
            " WHERE (station_id, ts) IN (SELECT station_id, ts FROM observations"
            "  WHERE ts >= %s AND ts < %s ORDER BY station_id DESC, ts DESC LIMIT 2)",
            (lo, hi),
        ).rowcount
        i = c.execute(
            "INSERT INTO observations (station_id, ts, value)"
            " VALUES ('ST999', %s, 777) ON CONFLICT DO NOTHING",
            (lo + timedelta(hours=1),),
        ).rowcount
    print(f"  篡改现行观测（{tag}）：更新{u} 删除{d} 新增{i}", flush=True)



def verify_month(month: str, expected: dict, label: str):
    """迁移完成后的一致性校验（全部用直接 SQL 独立验证）。"""
    got = snapshot(month)
    check(f"{label}: 父表内容与期望逐行一致", got == expected,
          f"父表 {len(got)} 行 vs 期望 {len(expected)} 行"
          + ("" if got == expected else
             f"，差异样例 {list((got.items() ^ expected.items()))[:3]}"))
    check(f"{label}: 默认分区无残留", default_count(month) == 0,
          f"剩余 {default_count(month)}")
    check(f"{label}: 月分区已挂载", partition_exists(month))
    if partition_exists(month):
        check(f"{label}: 分区行数 == 父表行数",
              partition_count(month) == len(got),
              f"{partition_count(month)} vs {len(got)}")
    with db() as c:
        dup = c.execute(
            "SELECT count(*) - count(DISTINCT (station_id, ts))"
            " FROM observations WHERE ts >= %s AND ts < %s",
            month_range(month),
        ).fetchone()[0]
        trg = c.execute(
            "SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'obs_delta%'"
        ).fetchone()[0]
        delta = c.execute(
            "SELECT count(*) FROM migration_delta WHERE month = %s::date",
            (month + "-01",),
        ).fetchone()[0]
    check(f"{label}: 父表主键无重复", dup == 0, f"重复 {dup}")
    check(f"{label}: 增量触发器已清理", trg == 0, f"剩余 {trg}")
    check(f"{label}: 增量日志已清理", delta == 0, f"剩余 {delta}")
    r = requests.get(f"{BASE}/api/verify", params={"month": month}, timeout=10)
    check(f"{label}: /api/verify 判定一致", r.ok and r.json()["consistent"],
          r.text[:200])


# ---------------------------------------------------------------------- 用例
def test_happy_path(month: str):
    print(f"\n== 用例1：慢速复制 + 并发直接SQL增删改（{month}）==", flush=True)
    expected = snapshot(month)
    base_rows = len(expected)
    r = api_migrate(month, batch_size=200, sleep_ms=120)
    check("迁移启动成功", r.ok, r.text[:200])
    # 一次只迁一个月
    r2 = api_migrate(NEXT_MONTHS[1])
    check("迁移进行中再启动另一个月被拒绝(409)", r2.status_code == 409,
          f"rc={r2.status_code}")
    writer = ChaosWriter(month, expected)
    writer.start()
    status = wait_status(month, {"done", "failed"})
    writer.stop_event.set()
    writer.join(timeout=10)
    check("迁移完成", status == "done", f"status={status} err={run_row(month)}")
    check("并发写入无错误", not writer.errors,
          "; ".join(writer.errors[:3]))
    print(f"  并发写入 {writer.ops} 次；迁移前 {base_rows} 行 → 期望 {len(expected)} 行")
    verify_month(month, expected, "用例1")
    # 幂等：已完成月份再次操作不再搬迁
    before = snapshot(month)
    r3 = api_migrate(month)
    check("已完成月份再次迁移被拒绝(409)", r3.status_code == 409, f"rc={r3.status_code}")
    check("重复操作后数据不变", snapshot(month) == before)


def test_fail_and_retry(month: str, fail_at: str):
    print(f"\n== 用例：{fail_at} 阶段故障注入后重试（{month}）==", flush=True)
    expected = snapshot(month)
    r = api_migrate(month, fail_at=fail_at, sleep_ms=40)
    check("迁移启动成功", r.ok, r.text[:200])
    status = wait_status(month, {"failed", "done"}, timeout=120)
    check("故障注入后状态为 failed", status == "failed", f"status={status}")
    # 父表在失败后仍完整可查
    got = snapshot(month)
    check("失败后父表内容完整不变", got == expected,
          f"{len(got)} vs {len(expected)}")
    check("失败后默认分区仍持有全部数据", default_count(month) == len(expected))
    # 重试（无故障注入）+ 并发写入
    r2 = api_migrate(month, sleep_ms=60)
    check("重试启动成功", r2.ok, r2.text[:200])
    # 等重试 worker 接管（状态脱离上一轮的 failed），避免读到过期状态
    deadline = time.time() + 30
    while time.time() < deadline:
        row = run_row(month)
        if row and row[0] != "failed":
            break
        time.sleep(0.2)
    writer = ChaosWriter(month, expected)
    writer.start()
    status = wait_status(month, {"done", "failed"})
    writer.stop_event.set()
    writer.join(timeout=10)
    check("重试后迁移完成", status == "done", f"status={status}")
    check("重试期间并发写入无错误", not writer.errors,
          "; ".join(writer.errors[:3]))
    verify_month(month, expected, f"重试({fail_at})")


def test_process_kill(month: str):
    print(f"\n== 用例：复制进行中应用进程硬退出（{month}）==", flush=True)
    expected = snapshot(month)
    r = api_migrate(month, batch_size=150, sleep_ms=150)
    check("迁移启动成功", r.ok, r.text[:200])
    # 等复制真正开始
    deadline = time.time() + 60
    while time.time() < deadline:
        row = run_row(month)
        if row and row[1] and row[1] > 0:
            break
        time.sleep(0.2)
    row = run_row(month)
    check("复制已开始（copied_rows>0）", bool(row and row[1] and row[1] > 0),
          f"run={row}")
    crash_app()
    time.sleep(1)
    # 应用已死：父表仍完整可查，且采集写入（直接 SQL）仍可提交
    got = snapshot(month)
    check("进程被杀后父表内容完整", got == expected,
          f"{len(got)} vs {len(expected)}")
    probe = ChaosWriter(month, expected)
    probe.start()
    time.sleep(2)
    probe.stop_event.set()
    probe.join(timeout=10)
    check("应用停机期间直接 SQL 写入正常", probe.ops > 0 and not probe.errors,
          f"ops={probe.ops} err={probe.errors[:2]}")
    # 重启应用，重试迁移
    start_app()
    r2 = api_migrate(month, sleep_ms=60)
    check("重启后重试启动成功", r2.ok, r2.text[:200])
    writer = ChaosWriter(month, expected)
    writer.start()
    status = wait_status(month, {"done", "failed"})
    writer.stop_event.set()
    writer.join(timeout=10)
    check("崩溃恢复后迁移完成", status == "done", f"status={status}")
    verify_month(month, expected, "崩溃恢复")


# ------------------------------------------------------------------ 交付用例
def test_delivery_happy(month: str) -> bytes:
    """用例7/8/9：受理→字节级一致→幂等复用/换月拒绝→冻结后篡改不变。返回 D1 字节。"""
    print(f"\n== 用例7：交付受理与字节级一致（{month}，单号 DEL-T1）==", flush=True)
    snap0 = snapshot(month)
    r = api_delivery_submit("DEL-T1", month)
    check("提交受理成功", r.ok, r.text[:200])
    st = r.json()
    check("受理返回即已固定快照（受理成功前持久固定）",
          st["row_count"] == len(snap0) and st["frozen_at"],
          f"row_count={st.get('row_count')} 期望={len(snap0)}")
    st = wait_delivery("DEL-T1", {"ready", "failed"})
    check("生成完成", st["status"] == "ready", f"status={st['status']} err={st['error']}")
    r = download("DEL-T1")
    check("ready 后可下载", r.ok, f"rc={r.status_code}")
    check("下载响应头携带 SHA-256 且与登记一致",
          r.headers.get("X-Content-SHA256") == st["csv_sha256"])
    t1_csv = r.content  # DEL-T1 的 CSV 字节（后续比较用，r 会被复用）
    check_csv("用例7", t1_csv, snap0, delivery_row("DEL-T1"))

    print("\n== 用例8：同号同月复用 / 换月拒绝 / 并发提交 ==", flush=True)
    r = api_delivery_submit("DEL-T1", month)
    check("同号同月重复提交成功且标记复用", r.ok and r.json()["reused"],
          f"rc={r.status_code} {r.text[:160]}")
    st2 = delivery_row("DEL-T1")
    check("复用后冻结时点与 SHA-256 不变",
          st2["frozen_at"] == st["frozen_at"]
          and st2["csv_sha256"] == st["csv_sha256"])
    other = NEXT_MONTHS[1] if month != NEXT_MONTHS[1] else NEXT_MONTHS[0]
    r = api_delivery_submit("DEL-T1", other)
    check("同号换月被拒绝(409)", r.status_code == 409, f"rc={r.status_code}")
    # 并发提交同一新单号
    results: list[requests.Response] = []

    def _submit():
        results.append(api_delivery_submit("DEL-T8", month))

    ts = [threading.Thread(target=_submit) for _ in range(2)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(timeout=60)
    check("并发同号同月提交均受理", all(x.ok for x in results),
          "; ".join(f"{x.status_code}" for x in results))
    created = sum(1 for x in results if not x.json()["reused"])
    check("并发提交恰好一个实际冻结、其余复用", created == 1, f"created={created}")
    st8 = wait_delivery("DEL-T8", {"ready", "failed"})
    check("并发提交交付生成完成", st8["status"] == "ready")
    r8 = download("DEL-T8")
    check("并发交付 CSV 与同月冻结快照一致", r8.ok and r8.content == t1_csv)
    r = requests.post(f"{BASE}/api/deliveries/DEL-T1/retry", timeout=10)
    check("已完成交付重试被拒绝(409)", r.status_code == 409, f"rc={r.status_code}")
    r = download("DEL-NOPE")
    check("不存在的单号下载 404", r.status_code == 404, f"rc={r.status_code}")

    print("\n== 用例9：冻结后篡改现行观测，交付字节不变 ==", flush=True)
    before = download("DEL-T1").content
    mutate_month(month, "后续更正")
    check("现行观测已被篡改（与冻结快照不同）", snapshot(month) != snap0)
    after = download("DEL-T1").content
    check("已交付 CSV 字节不变（不随后续更正变化）", after == before)
    return before


def test_delivery_fail_retry(month: str):
    """用例11：generating 故障注入 → failed → 篡改现行观测 → 重试仍用原固定数据。"""
    print(f"\n== 用例11：生成失败重试且不重读现行观测（{month}，单号 DEL-T2）==",
          flush=True)
    snap0 = snapshot(month)
    r = api_delivery_submit("DEL-T2", month, sleep_ms=150, fail_at="generating")
    check("提交受理成功", r.ok, r.text[:200])
    st = wait_delivery("DEL-T2", {"failed", "ready"}, timeout=60)
    check("故障注入后状态 failed", st["status"] == "failed",
          f"status={st['status']} err={st['error']}")
    mutate_month(month, "失败后更正")
    r = requests.post(f"{BASE}/api/deliveries/DEL-T2/retry", timeout=10)
    check("重试受理成功", r.ok, r.text[:200])
    st = wait_delivery("DEL-T2", {"ready", "failed"}, timeout=60)
    check("重试后生成完成", st["status"] == "ready", f"err={st['error']}")
    r = download("DEL-T2")
    check("重试 CSV 仍等于原冻结快照（未重读现行观测）",
          r.ok and r.content == expected_csv_bytes(snap0))
    check_csv("用例11", r.content, snap0, delivery_row("DEL-T2"))


def test_delivery_slow_gen_no_block(month: str):
    """用例10：慢生成进行中，同月迁移照样完成、采集写入不报错。"""
    print(f"\n== 用例10：慢生成不妨碍月迁移与采集提交（{month}，单号 DEL-T3）==",
          flush=True)
    snap0 = snapshot(month)
    expected = dict(snap0)
    r = api_delivery_submit("DEL-T3", month, sleep_ms=800)
    check("慢生成交付受理成功", r.ok, r.text[:200])
    wait_delivery("DEL-T3", {"generating", "ready"}, timeout=30)
    st = delivery_row("DEL-T3")
    check("交付处于慢生成中", st["status"] in ("generating", "ready"),
          f"status={st['status']}")
    # 生成进行中：发起同月迁移 + 并发采集写入
    r = api_migrate(month, batch_size=200, sleep_ms=60)
    check("慢生成进行中迁移启动成功", r.ok, r.text[:200])
    writer = ChaosWriter(month, expected)
    writer.start()
    status = wait_status(month, {"done", "failed"})
    check("慢生成进行中迁移照样完成", status == "done",
          f"status={status} err={run_row(month)}")
    st = wait_delivery("DEL-T3", {"ready", "failed"}, timeout=120)
    writer.stop_event.set()
    writer.join(timeout=10)
    check("慢生成完成", st["status"] == "ready", f"err={st['error']}")
    check("期间采集写入无错误", not writer.errors, "; ".join(writer.errors[:3]))
    r = download("DEL-T3")
    check("慢生成 CSV 仍等于冻结时点快照（不受迁移/改写影响）",
          r.ok and r.content == expected_csv_bytes(snap0))
    verify_month(month, expected, "用例10(迁移)")


def test_delivery_crash_restart(month: str, t1_bytes: bytes):
    """用例12：生成中硬退出 → 停机期间篡改 → 重启自动续跑 → 仍等于冻结快照。"""
    print(f"\n== 用例12：生成中进程退出，重启后续跑（{month}，单号 DEL-T4）==",
          flush=True)
    snap0 = snapshot(month)
    r = api_delivery_submit("DEL-T4", month, sleep_ms=800)
    check("提交受理成功", r.ok, r.text[:200])
    # 等生成真正开始
    deadline = time.time() + 30
    st = None
    while time.time() < deadline:
        st = delivery_row("DEL-T4")
        if st and st["status"] == "generating" and st["generated_rows"] > 0:
            break
        time.sleep(0.2)
    check("生成已开始（generated_rows>0）",
          bool(st and st["generated_rows"] > 0), f"st={st and st['status']}")
    r = download("DEL-T4")
    check("生成中下载被拒绝(409)", r.status_code == 409, f"rc={r.status_code}")
    crash_app()
    time.sleep(1)
    mutate_month(month, "停机期间更正")
    start_app()
    st = wait_delivery("DEL-T4", {"ready", "failed"}, timeout=120)
    check("重启后自动续跑并完成", st["status"] == "ready", f"err={st['error']}")
    r = download("DEL-T4")
    check("续跑 CSV 等于冻结快照（停机篡改未被纳入）",
          r.ok and r.content == expected_csv_bytes(snap0))
    check_csv("用例12", r.content, snap0, delivery_row("DEL-T4"))
    r = download("DEL-T1")
    check("已完成交付重启后下载字节不变", r.ok and r.content == t1_bytes)



def main():
    global NEXT_MONTHS
    random.seed(20261116)
    with db() as c:
        months = [r[0].isoformat()[:7] for r in c.execute(
            "SELECT (date_trunc('month', ts AT TIME ZONE 'UTC'))::date AS m,"
            "       count(*) FROM observations_default"
            " GROUP BY 1 ORDER BY 1"
        )]
    check("种子数据至少 6 个月份", len(months) >= 6, f"months={months}")
    NEXT_MONTHS = months
    m1, m2, m3, m4, md, mg = months[:6]

    start_app()
    try:
        r = requests.get(f"{BASE}/api/state", timeout=10)
        check("GET /api/state 可用", r.ok)
        test_happy_path(m1)
        test_fail_and_retry(m2, "copying")
        test_fail_and_retry(m3, "finalizing")
        test_process_kill(m4)
        # 观测交付（md 不做迁移；mg 用于慢生成+迁移并发）
        t1_bytes = test_delivery_happy(md)
        test_delivery_fail_retry(md)
        test_delivery_slow_gen_no_block(mg)
        test_delivery_crash_restart(md, t1_bytes)
    finally:
        crash_app()

    print("\n" + "=" * 60)
    if _failures:
        print(f"自测失败 {len(_failures)} 项：")
        for f in _failures:
            print("  -", f)
        sys.exit(1)
    print("全部自测通过 ✓")


if __name__ == "__main__":
    main()
