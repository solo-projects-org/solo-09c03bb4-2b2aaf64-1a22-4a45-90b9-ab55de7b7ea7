# 湖仓水位观测表 Delta Lake 压实 API

用 **FastAPI + PyArrow** 手工实现的 Delta Lake（读协议 v1 / 写协议 v2）小文件压实服务，
以 **双进程** 方式运行，面向一张无分区的水位观测表（非空字符串站号、UTC 秒与水位毫米
均为 int64）。压实把指定版本中的至少两份 Parquet 合成一份，保持原值、重复行、模式与
全部旧快照不变，并在外部写者持续提交的环境下用乐观并发控制（OCC）保证提交安全。

新增的 **站点时段隔离**：仪器停机时段以一条 `isolate` 日志动作原子提交（不改写任何
Parquet），把该站 `[start, end)` 内的观测从预报取数（可用观测查询）中隔离；原始值
在所有快照中保留，供复核与时间旅行。

## 目录结构

```
.
├── app/
│   ├── delta.py            # Delta 协议：JSON 日志、快照回放、原子提交、压实/OCC、隔离标记
│   ├── external_writer.py  # 外部写者：dataChange=true 的追加与删除（独立进程）
│   ├── sample_data.py      # 样例表引导（4 个小文件，幂等、并发安全）
│   └── main.py             # FastAPI 路由、Swagger、错误码映射
├── scripts/
│   ├── _demo_common.py
│   ├── demo_readback.py            # 演示 1：压实并读回新旧版本
│   ├── demo_concurrent_append.py   # 演示 2：压实期间外部并发追加
│   ├── demo_concurrent_delete.py   # 演示 3：压实期间外部删除 -> 409 冲突
│   ├── demo_isolation.py           # 演示 4：停机时段隔离与可用观测查询
│   ├── run_local_servers.sh        # 无 Docker 时本地双进程启动
│   └── run_all_demos.sh
├── tests/test_compaction.py        # 23 个端到端/并发/崩溃测试
├── tests/test_isolation.py         # 15 个隔离语义/并发/版本一致性测试
├── Dockerfile
├── docker-compose.yml              # api-1:8001、api-2:8002，共享一个数据卷
├── requirements.txt / requirements-dev.txt
```

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

两个副本共享命名卷 `delta-data` 中的同一张表，任一进程都可发起压实：

| 服务   | 地址              | Swagger UI                |
|--------|-------------------|---------------------------|
| api-1  | http://localhost:8001 | http://localhost:8001/docs |
| api-2  | http://localhost:8002 | http://localhost:8002/docs |

启动时自动创建样例表：版本 0 为 `protocol(1,2)+metaData`，版本 1–4 各追加一个
Parquet（共 24 行、4 个文件）。容器内表路径为 `/data/water_level`。

## API

| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/health` | 健康检查，返回最新版号 |
| POST | `/compact` | 压实：body `{"version": 4, "files": ["a.parquet", "b.parquet"]}`，`version` 省略时取最新版，`files` 至少 2 个 |
| POST | `/isolation` | 时段隔离：body `{"station": "ST-001", "start_utc_seconds": S, "end_utc_seconds": E}`，隔离 `[S, E)`（含起点、不含终点） |
| GET  | `/isolation?version=N` | 列出截至该版生效的隔离标记（不传为最新版） |
| GET  | `/observations/available?version=N` | 可用观测查询：该版原始 Parquet 减去截至该版的隔离时段，返回未隔离行与排除行数 |
| GET  | `/table/versions` | 全部连续版号 |
| GET  | `/table/snapshot?version=N` | 快照信息（不传为最新，支持时间旅行） |
| GET  | `/table/stats` | 行数、模式、孤儿清理结果 |

`POST /compact` 成功响应包含提交版号与文件数变化：

```json
{
  "committed_version": 5,
  "base_version": 4,
  "files_added": 1,
  "files_removed": 2,
  "output_file": "….parquet",
  "input_files": ["….parquet", "….parquet"],
  "rows": 12,
  "input_bytes": 2168,
  "output_bytes": 1131,
  "attempts": 1
}
```

错误码：400 请求非法（少于两文件、路径逃逸、隔离时段为空或倒置等）、404 版号或文件不存在、
409 并发冲突（所选文件已被他人移除）、503 表暂不可用。

### 示例

```bash
curl -s localhost:8001/table/snapshot | jq .active_files
curl -s -X POST localhost:8001/compact -H 'content-type: application/json' \
  -d '{"files": ["<f1>.parquet", "<f2>.parquet"]}'
curl -s "localhost:8001/table/snapshot?version=4"   # 旧快照时间旅行
```

### 时段隔离示例

```bash
# 数据员登记停机时段 [1731603600, 1731607200)：原子提交一条 isolate 日志动作
curl -s -X POST localhost:8001/isolation -H 'content-type: application/json' \
  -d '{"station": "ST-001", "start_utc_seconds": 1731603600, "end_utc_seconds": 1731607200}'

# 预报取数：未隔离的行 + 排除行数（按版号一致读取，可时间旅行）
curl -s "localhost:8002/observations/available" | jq '{available_count, excluded_count}'
curl -s "localhost:8002/observations/available?version=4"   # 标记前的旧版结果不变

# 复核：原始快照行数不变，标记列表可按版查看
curl -s localhost:8001/table/snapshot | jq .row_count
curl -s "localhost:8001/isolation" | jq .marks
```

## 演示脚本

脚本同时走 HTTP 与本地表路径（外部写者以独立 OS 进程直接提交 Delta 日志）：

```bash
pip install -r requirements-dev.txt
bash scripts/run_local_servers.sh            # 终端 A：本地起两个 uvicorn
bash scripts/run_all_demos.sh                # 终端 B：依次跑三个演示
```

* **演示 1（读回新旧版）**：压实后新版行数/模式不变、文件数 -2+1；`?version=` 读回
  旧版，文件集与数据完全不变。
* **演示 2（并发追加）**：外部进程在压实进行中连续提交 6 个 `dataChange=true` 追加，
  压实仍成功（无关文件变化不构成冲突），日志保持 0 起连续。
* **演示 3（并发删除）**：外部进程删除所选文件之一，压实提交时复检发现文件失效，
  返回 409 且不复活已删数据；改用仍有效的文件重试即成功。
* **演示 4（时段隔离）**：登记停机时段后，可用观测查询立即排除该段；迟到的同段
  补录观测同样被排除；原始快照行数不变，标记前旧版查询结果不变。

## 协议与正确性保证

**表结构**：无分区；协议固定 reader=1、writer=2；无删除向量；JSON 日志从 0 连续
编号（`_delta_log/00000000000000000000.json` …），每行一个 JSON action。

**数据不变量**

* 压实仅做 `concat`，不去重、不过滤、不改序语义；模式逐字段校验，非空约束校验。
* 新文件先写 `_staging/` 再 **硬链接** 到唯一最终路径（纳秒时间戳+UUID，路径永不复用），
  半成品不会出现在任何快照中、不可查询。
* 被移除的物理 Parquet 不删除，旧快照（时间旅行）始终可读。
* 写盘失败/进程中途退出：已提交快照不受影响；`_staging` 与 `.tmp-*` 孤儿由
  `/table/stats` 或启动时的 `cleanup_staging()` 按 mtime 安全回收。

**并发控制（OCC）**

* 日志提交 = 唯一临时文件 + `os.link` 原子改名：两个进程不可能得到同一版号，
  已提交日志绝不被覆盖或截断。
* 压实在提交前重新加载最新快照：**所选文件全部仍有效才替换**；任一文件已被并发
  移除则报 409 并清理新产物，绝不复活已删数据。
* 仅无关文件的增删不影响压实；日志版号竞争时自动退避重试（上限 25 次）。
* 压实提交的 add/remove 均带 `"dataChange": false`（add 另带 `tags.compaction=true`，
  `commitInfo.operation=OPTIMIZE` 与指标），外部追加/删除为 `dataChange=true`。

**时段隔离（`isolate` 动作）**

* 隔离标记是日志中的一条 `isolate` 动作（站号 + `[start, end)` UTC 秒），随
  Delta 日志**原子提交**：要么整份出现在某一版，要么完全不出现；发布失败最多
  留下被读取忽略的 `.tmp-*` 文件，绝不暴露半份标记。Parquet 一律不改写。
* 标记只增不减，按版生效：查询某版时，数据文件集与隔离标记来自**同一次快照
  回放**，绝不混用不同版的标记与观测；重启后重放日志得到同样结果。
* 标记按区间匹配，与行到达先后无关：先到的追加计入隔离结果，标记后补录的
  同段观测同样进不了可用查询；压实（只换文件、不动行）不影响隔离语义。
* 两副本并发标记重叠时段：OCC 退避重试让两份标记都落日志（不丢覆盖）；排除
  计数按**行的并集**计算，一行被多个标记覆盖也只排除、只计一次（不重复计数）。
* 可用观测查询 = 该版原始行 − 隔离并集，返回未隔离行与排除行数；原始快照
  （`/table/snapshot`、`read_table`）任何版本都完整可读，供复核。

## 测试

```bash
python -m pytest tests/ -q
```

覆盖：连续日志与协议号、行/重复行/模式保持、时间旅行、路径校验、跨进程同集/异集
压实竞争、并发删除 409 与不复活、崩溃后快照不变、staging 孤儿清理、双进程并发建表、
临时日志文件忽略；时段隔离的半开区间边界、版本一致性（不混用标记与观测）、重叠
标记不重复计数、双副本并发标记不丢覆盖、迟到同段观测被排除、压实/外部写者与标记
共存、发布失败无半份标记、重启后逐版结果一致，以及 HTTP 层全部状态码映射。
