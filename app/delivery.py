"""
观测交付：月度复核的取数依据留存。

核心保证
--------
1. 受理即固定：提交时在**单个事务**内完成
     INSERT 登记 + CREATE TABLE 快照表 AS SELECT（单条 SELECT 的 MVCC 快照，
     即同一已提交时点的完整观测）+ 状态置 frozen
   事务提交后 HTTP 才返回成功 —— 受理成功前数据已持久固定。
2. 不碍采集/迁移：冻结事务只做一次 CTAS（秒级）；之后的 CSV 生成只读
   **独立快照表**（分批短查询），下载只读文件 —— 都不碰 observations，
   不会长期占用源表快照等待文件处理。
3. 同号同月并发提交/断连重试复用原快照（delivery_no 主键 +
   ON CONFLICT DO NOTHING）；同号换月拒绝（409）。
4. 生成失败/进程退出：快照表持久保留，重启后从**原固定数据**续跑或重试，
   绝不重读现行 observations。
5. 下载门禁：CSV 先写临时文件 → fsync → 原子 rename → fsync 目录，
   然后才登记 status='ready'；下载时复核文件大小与 SHA-256 与登记一致。
   已完成交付重启后字节不变（ready 后文件不再被写）。

状态机：frozen（快照已固定，待生成）→ generating → ready / failed（可重试）
"""

from __future__ import annotations

import csv
import hashlib
import os
import re
import threading
import time
from datetime import date, datetime, timezone
from pathlib import Path

from db import connect, log
from migrator import next_month, parse_month

DELIVERY_DIR = Path(
    os.environ.get("DELIVERY_DIR", "/tmp/pg-runtime/deliveries")
)

NO_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
BATCH = 500  # 生成时每批读取/写入的行数

ST_FROZEN = "frozen"
ST_GENERATING = "generating"
ST_READY = "ready"
ST_FAILED = "failed"


class MonthMismatch(Exception):
    """同号换月：交付单号已用于其他月份。"""


class DeliveryNotFound(Exception):
    """交付单号不存在。"""


class NotReady(Exception):
    """尚未生成完成，不能下载。"""


class NotRetryable(Exception):
    """当前状态不允许重试（如已完成）。"""


class InjectedFailure(Exception):
    """故障注入（仅演示/自测）。"""


# --------------------------------------------------------------------- 工具
def parse_delivery_no(no: str) -> str:
    if not no or not NO_RE.match(no):
        raise ValueError(
            "交付单号应为 1-64 位字母/数字/下划线/中划线，且以字母或数字开头，"
            f"收到: {no!r}"
        )
    return no


def _slug(no: str) -> str:
    """交付单号 → 安全的标识符片段（表名/文件名用），带哈希后缀防碰撞。"""
    base = re.sub(r"[^a-z0-9]+", "_", no.lower()).strip("_")[:40] or "x"
    digest = hashlib.sha1(no.encode("utf-8")).hexdigest()[:8]  # noqa: S324 - 非安全用途
    return f"{base}_{digest}"


def snap_table_for(no: str) -> str:
    return f"delivery_snap_{_slug(no)}"


def csv_path_for(no: str) -> Path:
    return DELIVERY_DIR / f"obs_delivery_{_slug(no)}.csv"


def tmp_path_for(no: str) -> Path:
    return DELIVERY_DIR / f"obs_delivery_{_slug(no)}.csv.tmp"


def _row_to_state(r) -> dict:
    return {
        "delivery_no": r[0],
        "month": r[1].isoformat()[:7],
        "status": r[2],
        "snapshot_table": r[3],
        "row_count": r[4],
        "generated_rows": r[5],
        "csv_rows": r[6],
        "csv_sha256": r[7],
        "csv_bytes": r[8],
        "file_path": r[9],
        "sleep_ms": r[10],
        "frozen_at": r[11].isoformat() if r[11] else None,
        "error": r[12],
        "updated_at": r[13].isoformat() if r[13] else None,
    }


_SELECT_COLS = (
    "delivery_no, month, status, snapshot_table, row_count, generated_rows,"
    " csv_rows, csv_sha256, csv_bytes, file_path, sleep_ms, frozen_at,"
    " error, updated_at"
)


def get_delivery(no: str, conn=None) -> dict | None:
    sql = f"SELECT {_SELECT_COLS} FROM deliveries WHERE delivery_no = %s"
    if conn is not None:
        row = conn.execute(sql, (no,)).fetchone()
        return _row_to_state(row) if row else None
    with connect() as c:
        row = c.execute(sql, (no,)).fetchone()
        return _row_to_state(row) if row else None


def list_deliveries() -> list[dict]:
    with connect() as c:
        rows = c.execute(
            f"SELECT {_SELECT_COLS} FROM deliveries ORDER BY created_at DESC"
        ).fetchall()
    return [_row_to_state(r) for r in rows]


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# --------------------------------------------------------------------- 受理
def submit_delivery(
    no: str, month_str: str, sleep_ms: int = 0, fail_at: str | None = None
) -> dict:
    """受理交付：同事务固定快照后返回；同号同月复用，换月拒绝。"""
    no = parse_delivery_no(no)
    month = parse_month(month_str)
    lo, hi = month.isoformat(), next_month(month).isoformat()
    sleep_ms = max(0, min(int(sleep_ms), 10000))
    snap = snap_table_for(no)
    created = False
    with connect() as conn:
        with conn.transaction():
            row = conn.execute(
                "INSERT INTO deliveries (delivery_no, month, status,"
                "                snapshot_table, sleep_ms)"
                " VALUES (%s, %s, %s, %s, %s)"
                " ON CONFLICT (delivery_no) DO NOTHING"
                " RETURNING delivery_no",
                (no, month, ST_FROZEN, snap, sleep_ms),
            ).fetchone()
            if row:
                # 单条 SELECT 的 MVCC 快照 = 同一已提交时点的完整观测；
                # CTAS 与登记在同一事务，崩溃则整体回滚，不留半成品。
                conn.execute(
                    f"CREATE TABLE {snap} AS"
                    f" SELECT station_id, ts, value FROM observations"
                    f" WHERE ts >= %s::date AND ts < %s::date",
                    (lo, hi),
                )
                conn.execute(
                    f"ALTER TABLE {snap} ADD PRIMARY KEY (station_id, ts)"
                )
                cnt = conn.execute(
                    f"SELECT count(*) FROM {snap}"
                ).fetchone()[0]
                conn.execute(
                    "UPDATE deliveries SET row_count = %s, frozen_at = now(),"
                    " updated_at = now() WHERE delivery_no = %s",
                    (cnt, no),
                )
                created = True
        state = get_delivery(no, conn)
    if created:
        log(
            f"[deliver {no}] 受理成功：{month_str} 同一时点快照已固定 "
            f"{state['row_count']} 行 → {snap}（此后不再读 observations）"
        )
    else:
        if state["month"] != month_str:
            raise MonthMismatch(
                f"交付单号 {no} 已用于月份 {state['month']}，"
                f"拒绝换月为 {month_str}"
            )
        log(
            f"[deliver {no}] 同号同月重复提交/断连重试："
            f"复用已固定快照（{state['row_count']} 行，状态 {state['status']}）"
        )
    # 已固定未完成的（含并发复用、断连重试、上次生成失败）确保有生成线程；
    # 重跑也只读原固定快照表，绝不重读 observations。
    if state["status"] in (ST_FROZEN, ST_GENERATING, ST_FAILED):
        ensure_worker(no, fail_at=fail_at)
    state["reused"] = not created
    return state


# ------------------------------------------------------------------- 生成
class _HashingWriter:
    """包在文本文件外：csv.writer 写的内容同步计入 SHA-256。"""

    def __init__(self, raw, h):
        self._raw = raw
        self._h = h

    def write(self, s: str):
        self._h.update(s.encode("utf-8"))
        return self._raw.write(s)


class DeliveryWorker(threading.Thread):
    """从固定快照表生成 CSV。每个交付单号同一时刻至多一个实例。"""

    def __init__(self, no: str, fail_at: str | None = None):
        super().__init__(daemon=True, name=f"deliver-{no}")
        self.no = no
        self.fail_at = fail_at  # 故障注入点：generating

    def _log(self, msg: str):
        log(f"[deliver {self.no}] {msg}")

    def _set(self, conn, status: str, **fields):
        sets = ["status = %s", "updated_at = now()"]
        params: list = [status]
        for k, v in fields.items():
            sets.append(f"{k} = %s")
            params.append(v)
        params.append(self.no)
        conn.execute(
            f"UPDATE deliveries SET {', '.join(sets)} WHERE delivery_no = %s",
            params,
        )

    def run(self):
        try:
            self._generate()
        except Exception as exc:  # noqa: BLE001 - 记录到库里供重试
            self._log(f"生成中断：{exc!r}（快照表保留，可重试/重启续跑）")
            try:
                with connect() as conn:
                    self._set(conn, ST_FAILED, error=str(exc))
            except Exception:  # noqa: BLE001
                pass
        finally:
            worker_finished(self)

    def _generate(self):
        DELIVERY_DIR.mkdir(parents=True, exist_ok=True)
        with connect() as conn:
            st = get_delivery(self.no, conn)
            if st is None:
                raise RuntimeError("交付登记不存在")
            if st["status"] == ST_READY:
                self._log("已完成，跳过生成")
                return
            snap = st["snapshot_table"]
            sleep_ms = st["sleep_ms"]
            total = st["row_count"]
            self._set(conn, ST_GENERATING, generated_rows=0, error=None)
        tmp = tmp_path_for(self.no)
        final = csv_path_for(self.no)
        h = hashlib.sha256()
        written = 0
        last_station = ""
        last_ts = datetime.min.replace(tzinfo=timezone.utc)
        t0 = time.monotonic()
        # 只读固定快照表（独立表，分批短查询）——不碰 observations，
        # 慢生成不会占用源表快照，也不妨碍采集写入与月迁移。
        with open(tmp, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(_HashingWriter(f, h), lineterminator="\n")
            writer.writerow(["station_id", "ts", "value"])
            while True:
                with connect() as conn:
                    rows = conn.execute(
                        f"SELECT station_id, ts, value FROM {snap}"
                        f" WHERE station_id > %s"
                        f"    OR (station_id = %s AND ts > %s)"
                        f" ORDER BY station_id, ts LIMIT %s",
                        (last_station, last_station, last_ts, BATCH),
                    ).fetchall()
                if not rows:
                    break
                for station, ts, value in rows:
                    writer.writerow([station, ts.isoformat(), value])
                written += len(rows)
                last_station, last_ts = rows[-1][0], rows[-1][1]
                with connect() as conn:
                    self._set(conn, ST_GENERATING, generated_rows=written)
                # 故障注入：生成约 1/3 时人为失败，验证重试仍用原快照
                if self.fail_at == "generating" and total and written >= total / 3:
                    raise InjectedFailure("故障注入：generating 阶段人为失败")
                if sleep_ms:
                    time.sleep(sleep_ms / 1000)
            f.flush()
            os.fsync(f.fileno())  # 文件内容先持久化
        digest = h.hexdigest()
        nbytes = tmp.stat().st_size
        os.replace(tmp, final)  # 同目录原子改名
        # fsync 目录，确保改名本身落盘
        dfd = os.open(DELIVERY_DIR, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
        # 文件完整持久化之后才登记 ready —— 登记一致才开放下载
        with connect() as conn:
            self._set(
                conn, ST_READY,
                csv_rows=written, csv_sha256=digest, csv_bytes=nbytes,
                file_path=str(final), generated_rows=written, error=None,
            )
        self._log(
            f"生成完成：{written} 行，{nbytes} 字节，"
            f"sha256={digest[:16]}…，耗时 {time.monotonic() - t0:.1f}s"
        )


# --------------------------------------------------------------------- 控制
_workers: dict[str, DeliveryWorker] = {}
_workers_lock = threading.Lock()


def ensure_worker(no: str, fail_at: str | None = None) -> bool:
    """若该交付没有存活的生成线程则启动一个。返回是否新启动。"""
    with _workers_lock:
        w = _workers.get(no)
        if w and w.is_alive():
            return False
        w = DeliveryWorker(no, fail_at=fail_at)
        _workers[no] = w
        w.start()
        return True


def worker_finished(worker: DeliveryWorker):
    with _workers_lock:
        if _workers.get(worker.no) is worker:
            _workers.pop(worker.no, None)


def running_deliveries() -> list[str]:
    with _workers_lock:
        return [no for no, w in _workers.items() if w.is_alive()]


def retry_delivery(no: str) -> dict:
    """失败/中断后重试：只用原固定快照表，绝不重读 observations。"""
    no = parse_delivery_no(no)
    with _workers_lock:
        w = _workers.get(no)
        if w and w.is_alive():
            raise NotRetryable("该交付正在生成中，请稍候")
    with connect() as conn:
        st = get_delivery(no, conn)
        if st is None:
            raise DeliveryNotFound(f"交付单号不存在: {no}")
        if st["status"] == ST_READY:
            raise NotRetryable("该交付已完成，无需重试（下载即可）")
        snap_exists = conn.execute(
            "SELECT 1 FROM pg_class WHERE relname = %s AND relkind = 'r'",
            (st["snapshot_table"],),
        ).fetchone()
        if not snap_exists:
            raise NotRetryable(
                f"固定快照表 {st['snapshot_table']} 缺失，无法按原数据重试"
            )
        conn.execute(
            "UPDATE deliveries SET status = %s, error = NULL,"
            " generated_rows = 0, updated_at = now() WHERE delivery_no = %s",
            (ST_FROZEN, no),
        )
    tmp_path_for(no).unlink(missing_ok=True)
    ensure_worker(no)
    log(f"[deliver {no}] 重试：沿用原固定快照 {st['snapshot_table']} 重新生成")
    return get_delivery(no)


def resume_incomplete():
    """应用启动时恢复：未完成的从原快照续跑；ready 的校验文件与登记一致。"""
    DELIVERY_DIR.mkdir(parents=True, exist_ok=True)
    with connect() as conn:
        rows = conn.execute(
            f"SELECT {_SELECT_COLS} FROM deliveries ORDER BY created_at"
        ).fetchall()
    states = [_row_to_state(r) for r in rows]
    for st in states:
        no = st["delivery_no"]
        if st["status"] in (ST_FROZEN, ST_GENERATING):
            tmp_path_for(no).unlink(missing_ok=True)
            log(
                f"[deliver {no}] 启动恢复：状态 {st['status']}，"
                f"从原固定快照 {st['snapshot_table']} 续跑（不读 observations）"
            )
            ensure_worker(no)
        elif st["status"] == ST_READY:
            p = Path(st["file_path"] or "")
            ok = (
                p.is_file()
                and p.stat().st_size == st["csv_bytes"]
                and _sha256_file(p) == st["csv_sha256"]
            )
            if not ok:
                with connect() as conn:
                    conn.execute(
                        "UPDATE deliveries SET status = %s,"
                        " error = '启动校验：文件缺失或与登记不一致',"
                        " updated_at = now() WHERE delivery_no = %s",
                        (ST_FAILED, no),
                    )
                log(f"[deliver {no}] 启动校验失败：文件缺失或与登记不一致，置 failed")
    if states:
        log(f"[deliver] 启动恢复完成，共检查 {len(states)} 个交付")


# ------------------------------------------------------------------- 下载
def verified_file(no: str) -> Path:
    """下载门禁：登记 ready 且文件与登记（大小+SHA-256）一致才放行。"""
    no = parse_delivery_no(no)
    st = get_delivery(no)
    if st is None:
        raise DeliveryNotFound(f"交付单号不存在: {no}")
    if st["status"] != ST_READY:
        raise NotReady(f"交付尚未生成完成（当前状态 {st['status']}），暂不能下载")
    p = Path(st["file_path"] or "")
    if not p.is_file():
        raise NotReady("登记为 ready 但文件缺失，与登记不一致")
    if p.stat().st_size != st["csv_bytes"]:
        raise NotReady("文件大小与登记不一致")
    if _sha256_file(p) != st["csv_sha256"]:
        raise NotReady("文件 SHA-256 与登记不一致")
    return p
