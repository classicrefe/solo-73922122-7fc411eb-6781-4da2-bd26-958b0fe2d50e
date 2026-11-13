# Rainfall Correction Partition API

气象站补正小时雨量 CSV 的入湖分区服务。FastAPI + PyArrow + SQLite，
把按 **UTC 日期** 上传的「站号 / UTC 整点 / 十分之一毫米非负整数」CSV，
加工成按日分区、以 **毫米** 为单位的 Parquet，并通过修订账本保证旧任务
永远不会盖回错误读数。

## 核心保证

| 需求 | 实现 |
| --- | --- |
| 上传 ≤ 8 MiB CSV，按 UTC 日期受理 | 请求体硬限制 `8 MiB`（超出返回 413）；路径参数 `YYYY-MM-DD` 校验 |
| 站号、UTC 整点、非负整数十分之一毫米 | `app/validation.py` 严格校验：时区必须为 UTC、必须整点、雨量为非负整数 |
| 日期不符 / 同站同刻重复 → 拒绝 | API 预检 + 清洗子进程二次权威校验，返回 422 并带行号 |
| 输出毫米单位 Parquet | 清洗时 `/10` 转 `float64`，编码器写 zstd Parquet（`station/utc_hour/rain_mm`） |
| 清洗、编码分别在子进程执行，清洗成功才能编码 | `app/subprocs/clean.py`、`app/subprocs/encode.py` 两个独立 OS 进程，串行阶段门控 |
| 同日每次受理产生新修订，先前未发布任务立即作废 | `revisions` 表修订号递增；上传即把同日非终态修订置 `cancelled` |
| 迟到完成也不得发布 | 每个阶段前与发布前都查账本；终态修订立即中止，发布为条件事务 |
| 停止实际工作后才运行新版 | 同日单 worker 串行队列；旧修订门控中止、子进程不再产出后才轮到新修订 |
| 不同日期可并行 | 每个 UTC 日期一条独立 asyncio 队列与 worker |
| 处理中仍可下载上次成功分区 | 下载读 `publications` 指向的已发布文件，与暂存区完全隔离 |
| 新文件完整就绪后才随发布修订一起切换 | 先写暂存 Parquet → 备份旧分区 → `os.replace` 原子切换 → 条件提交发布行；失败自动回滚旧分区 |
| 重启后接续各日最新未完成修订 | 启动恢复：每日只重排队最新未完成修订（从保留的原始 CSV 重做），更旧的过期任务丢弃 |
| 不得发布残件或恢复过期任务 | 启动时删除半成品 Arrow/Parquet；无任何启动即发布路径 |
| 查询区分最新任务与当前发布修订 | `GET /status/{date}` 同时返回 `latest_task` 与 `current_publication` |
| 阶段暂停 / 强制退出演示 | `/admin/pause`、`/admin/resume`、`/admin/revisions/{id}/kill` |

## 目录结构

```
app/
  config.py            目录布局与 8 MiB 限制
  validation.py        CSV 解析与全部业务校验（API 与清洗子进程共用）
  db.py                SQLite 账本：修订、发布、事件、暂停开关
  pipeline.py          每日期串行 worker、阶段门控、子进程管理、原子发布
  subprocs/
    clean.py           清洗子进程：复核 -> 毫米换算 -> Arrow IPC
    encode.py          编码子进程：Arrow -> zstd Parquet（暂存）
  schemas.py           响应模型
  main.py              FastAPI 路由
samples/               可直接上传的样例（含两个坏文件）
tests/                 12 个端到端/单测（含真实 uvicorn 重启恢复）
```

数据盘（容器内 `/data`）：

```
rainfall.db                      SQLite 账本（WAL）
partitions/YYYY-MM-DD.parquet    已发布、下载用的不可变分区
tmp/inputs/YYYY-MM-DD-rN.csv     受理的原始上传（审计留痕）
tmp/work/revN.arrow              清洗中间产物
tmp/parquet/revN.parquet         编码暂存与回滚备份
```

## 启动

```bash
docker compose up --build
# Swagger UI:
open http://localhost:8000/docs
```

`RAIN_DEMO_DELAY`（compose 默认 2 秒）会在每个阶段门与子进程内部停留，
便于在 UI 上观察暂停/强杀窗口；生产环境设为 `0`。

## 在 /docs 里的演示流程

### 1. 上传首日数据

`POST /uploads/2026-11-10`，`file` 选择 `samples/2026-11-10.csv` →
202 返回 `revision_id=1`。随后：

```bash
curl -s http://localhost:8000/status/2026-11-10 | jq
# latest_task.status = published, current_publication.revision_id = 1
curl -OJ http://localhost:8000/download/2026-11-10   # rainfall-2026-11-10.parquet
```

### 2. 看拒绝规则

- `samples/bad-wrong-date.csv`（含前一天 23 点）→ 422 `does not belong to UTC date`
- `samples/bad-duplicate.csv`（同站同刻两行）→ 422 `duplicate measurement`

### 3. 补正上传（新修订替换旧分区）

`POST /uploads/2026-11-10` 选择 `samples/2026-11-10-corrected.csv` →
`revision_id=2`，完成后下载到的分区雨量变为修正后的 0.9 / 4.0 / 13.0 … 毫米，
修订 1 保留为历史，发布指针指向修订 2。

### 4. 暂停 / 恢复

```bash
curl -X POST http://localhost:8000/admin/pause
curl -X POST http://localhost:8000/uploads/2026-11-11 -F 'file=@samples/2026-11-11.csv'
# 任务停在 queued，/download/2026-11-11 返回 404
curl -X POST http://localhost:8000/admin/resume   # 继续跑完并发布
```

### 5. 强制退出演示

上传后立即轮询状态，待 `running=true` 时调用：

```bash
curl -X POST http://localhost:8000/admin/revisions/<id>/kill
# 修订转 failed；已发布的旧分区仍可原样下载，再次 kill 返回 409
```

### 6. 重启接续

```bash
docker compose restart
# 暂停态下未完成的修订被重新排队（状态 recovered），恢复后从原始 CSV 重做；
# 绝不会把重启前的半成品发布出去。
```

## Parquet 模式

```
station:  string not null
utc_hour: timestamp[us, tz=UTC] not null
rain_mm:  double not null          # 输入 rain_tenth_mm / 10
```

zstd 压缩，单文件即一个 UTC 日分区。

## 本地测试

```bash
pip install -r requirements.txt
pip install pytest httpx
pytest -q
```

> 若宿主内核禁止跨进程 `os.kill`（部分受限沙箱如此），强杀集成测试会自动
> skip，其分支由假进程单测覆盖；Docker 默认 seccomp 配置下该接口正常。
