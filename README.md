# 环境监测数仓 · 默认分区 → 月分区在线迁移 + 观测交付演示

两个能力：

1. **月分区在线迁移**：把 `observations` 父表**默认分区**中的历史观测，按自然月（UTC）
   在线搬迁到新建的月分区。复制期间采集链路（独立连接）对该月的**新增 / 测值更正 / 删除**
   不中断；仅最终切换瞬间短暂锁表；失败或进程崩溃后可断点续迁；已完成月份重复操作不再搬迁。
2. **观测交付（月度复核取数留存）**：监测分析员按"交付单号 + UTC 月份"受理交付，
   受理成功前把该月**同一已提交时点**的完整观测持久固定到独立快照表，后台按
   **站号、时刻**排序生成 CSV 并登记**行数与 SHA-256**；交付内容不随后续更正变化。

## 数据模型

```sql
observations (station_id text, ts timestamptz, value integer,
              PRIMARY KEY (station_id, ts)) PARTITION BY RANGE (ts);
observations_default  -- 初始唯一分区（DEFAULT），历史数据都在这
```

元数据表：`migration_runs`（每月迁移状态机 + 进度断点）、
`migration_delta`（迁移期间触发器捕获的增量变更）、
`deliveries`（交付登记：状态机 / 快照表名 / 行数 / SHA-256 / 文件路径）。
每个交付一张 `delivery_snap_*` 快照表（受理时与同事务创建，之后只读它）。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

`db` 服务首次启动会执行 `db/init.sql`：建父表/默认分区/元数据表，
并写入最近 6 个自然月 × 8 站点 × 每小时一条的整数测值（约 3 万行，全部在默认分区）。

### 本地（无 docker）

需要本机有 PostgreSQL 16 服务端二进制与 Python 3.11+，然后：

```bash
pip install -r app/requirements.txt
PGHOME=/path/to/pgsql PY=python3 bash scripts/dev_up.sh   # 初始化DB+种子+启动应用
```

## 网页操作

1. **月份总览**：各月在默认分区的行数、已挂载月分区、迁移状态。
2. **迁移控制**：选月份 → 设批大小/批间休眠（慢复制）→ 开始迁移；进度条实时刷新。
   一次只迁一个月（进行中再发起会收到 409）。
3. **采集写入演示**：对选定月份手动或自动（可调间隔）做直接 SQL 增/改/删——
   每次都用**独立连接**写父表，模拟真实采集链路。
4. **父表查询**：迁移任意时刻查询父表，行数、主键重复数（恒为 0）、样本行。
5. **一致性校验**：父表行数 / 默认分区剩余（完成后为 0）/ 月分区行数三方核对。
6. **观测交付**：填交付单号 + UTC 月份 → 提交受理（可设"生成延迟"演示慢生成、
   故障注入演示失败重试）→ 列表实时显示状态/生成进度/行数/SHA-256 →
   ready 后下载 CSV；"退出重启演示"按钮让进程硬退出，重启后未完成的交付
   自动从原固定快照续跑，已完成交付字节不变。

迁移流程（`app/migrator.py`）：

| 阶段 | 做什么 | 是否锁写 |
|---|---|---|
| preparing | 建带 CHECK 的影子表、默认分区上挂增量触发器、登记运行记录 | 仅 DDL 瞬间 |
| waiting | 等触发器创建前开启的老事务结束（轮询 pg_stat_activity） | 否 |
| copying | 键集分页分批 `INSERT..ON CONFLICT` 复制，进度落库，可 sleep 放慢 | 否 |
| applying | 按 seq 顺序回放 `migration_delta`（insert/update→upsert，delete→删） | 否 |
| finalizing | **单事务**：SHARE 锁分区树 → 回放剩余增量 → 摘触发器 → 搬漏网行 → `ATTACH PARTITION` → 清理 → 置 done | **仅此短暂** |

正确性要点：

- 影子表在 `ATTACH` 前不属于父表，父表查询任何时刻都只见到一份数据——不重复、不缺失；
- 复制期间的所有变更由触发器落 `migration_delta`，回放幂等，可重复执行；
- 最终切换是**一个事务**，要么全部生效要么整体回滚，崩溃不会留下半吊子状态；
- 任意时刻进程死亡：影子表/增量/进度都在库里，重试从断点继续（影子表不清空、复制按
  最后主键续传、增量继续累积）；
- `migration_runs.status='done'` 的月份再次迁移直接拒绝（409），数据不变。

## 观测交付流程（`app/delivery.py`）

| 阶段 | 做什么 | 关键保证 |
|---|---|---|
| 受理（同步） | **单事务**：`INSERT deliveries` + `CREATE TABLE delivery_snap_* AS SELECT ... WHERE ts 在本月` + 置 `frozen` | 单条 SELECT 的 MVCC 快照 = 同一已提交时点；事务提交后才返回受理成功 |
| 生成（后台） | 从**快照表**键集分页 `ORDER BY station_id, ts` 流式写 CSV，边写边算 SHA-256，可按批休眠（慢生成演示） | 只读独立快照表，不碰 `observations`：慢生成/下载不妨碍采集提交与月迁移，不长期占用源表快照 |
| 落盘 | 写 `.tmp` → `fsync` → 原子 `rename` → `fsync` 目录 → 登记 `ready`（行数/SHA-256/字节/路径） | 文件完整持久化且登记一致才开放下载 |
| 下载 | 复核文件存在、字节数、SHA-256 与登记一致 | 不一致返回 409，可重试修复 |

- **同号同月并发提交/断连重试**：`delivery_no` 主键 + `ON CONFLICT DO NOTHING`，
  并发下恰好一个事务真正冻结，其余复用原快照；重复提交返回相同结果（`reused=true`）。
- **同号换月拒绝**：已登记月份与请求不一致 → 409。
- **失败/退出恢复**：快照表持久保留；`generating` 故障注入或进程硬退出后，
  应用重启自动从**原固定快照**续跑（`frozen`/`generating` 状态），`failed` 可手动重试——
  全程不重读现行 `observations`；启动时还会校验 `ready` 交付的文件与登记一致，
  不一致置 `failed` 以便按原快照修复。
- **已完成交付重启后字节不变**：CSV 文件存于持久卷（compose 的 `deliveries` 卷），
  `ready` 后不再重写；compose 配置 `restart: on-failure`，崩溃演示时容器自动拉起。

CSV 格式（UTF-8、`\n` 行尾、首行表头，时刻为 UTC ISO8601）：

```csv
station_id,ts,value
ST001,2026-06-01T00:00:00+00:00,61
```


## 自测

```bash
# 需要已初始化的库；脚本自己拉起/崩溃/重启应用进程
DATABASE_URL=postgresql://envmon@127.0.0.1:5544/envmon python3 scripts/selftest.py
```

迁移覆盖：慢复制+并发直接 SQL 增删改后父表逐行一致；一次只迁一个月；已完成月份幂等；
copying/finalizing 故障注入后重试成功；复制进行中进程硬退出（`os._exit`）后
父表仍完整可查可写、重启后续迁完成；触发器/增量/默认分区残留清理检查。

交付覆盖：受理即固定（响应时快照行数已确定）；下载 CSV 与冻结时点快照**字节级**一致
（排序/行数/SHA-256/字节数四方核对）；同号同月重复与并发提交复用原快照、同号换月 409；
冻结后篡改现行观测交付字节不变；慢生成进行中同月迁移照样完成且采集写入无错误；
generating 故障注入后重试仍用原固定数据；生成中进程硬退出、停机期间篡改观测，
重启自动续跑后 CSV 仍等于冻结快照；已完成交付重启后下载字节不变；生成中下载 409。

## API 摘要

- `GET  /api/state` 月份总览与迁移进度
- `POST /api/migrate` `{month, batch_size, sleep_ms, fail_at}` 启动迁移（409=进行中/已完成）
- `GET  /api/query?month=` 父表查询（行数/重复数/样本）
- `POST /api/demo/write` `{op, month}` 独立连接直接 SQL 增/改/删
- `POST /api/demo/auto` `{enabled, month, interval_ms}` 自动采集写入开关
- `GET  /api/verify?month=` 一致性校验
- `POST /api/deliveries` `{delivery_no, month, sleep_ms, fail_at}` 受理交付
  （同号同月复用 `reused=true`；同号换月 409）
- `GET  /api/deliveries` / `GET /api/deliveries/{no}` 交付列表/状态（行数、SHA-256、进度）
- `POST /api/deliveries/{no}/retry` 失败/中断后按原固定快照重试（ready 拒绝 409）
- `GET  /api/deliveries/{no}/download` 下载 CSV（未 ready 或登记不一致 409；
  响应头 `X-Content-SHA256`）
- `POST /api/debug/crash` 进程硬退出（仅自测/演示用）

## 已知边界（如实说明）

- 单应用进程内用互斥锁保证"一次只迁一个月"、每个交付单号一个生成线程；
  多副本部署需改用数据库咨询锁。
- `ATTACH PARTITION` 需对父表短暂 `ACCESS EXCLUSIVE`，并会顺序扫描默认分区
  校验无越界行（演示数据量下为毫秒级）；默认分区极大时该扫描会变长，
  生产上可先把数据搬空再挂载以缩短。
- `CREATE TRIGGER` 与 `ATTACH` 等 DDL 本身需要瞬间锁，无法完全避免；
  除此之外复制/回放阶段完全不锁写。
- 交付冻结用单条 `SELECT ... INTO` 的 MVCC 快照，读取的是语句开始时点的
  已提交数据；冻结事务本身只做一次 CTAS（秒级），不长期持有源表快照。
- 每个交付保留一张完整快照表 + 一个 CSV 文件，磁盘占用随交付数线性增长；
  快照表目前不自动清理（留作审计依据），需要时由运维手动 DROP。
- 下载时逐次重算文件 SHA-256 与登记比对（演示数据量下为毫秒级）；
  文件很大时可改为启动时校验 + 内存缓存。
- 种子数据与月份边界均按 UTC；父表查询与交付月份均按 `ts` 的 UTC 自然月切分；
  CSV 中时刻固定输出 UTC ISO8601。
