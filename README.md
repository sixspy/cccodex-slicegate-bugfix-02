# 日切放行

> 遵守 `ops/底座生成前必读_效率与技术栈.md`。FastAPI + Vue 3 + SQLite，交付须 docker compose 一键起；库用 SQLite，不挂外置 Postgres。  
> **禁止同构换皮**：本仓主链路、表结构、接口与页面均独立，不得与其它仓共用同一套 `/api/items` 壳。

## 骨架一句话

业务日分区切片登记后裁决完整度，完整才放行下游，并与消费水位对账

## 基本信息

| 项 | 值 |
|----|-----|
| 产品名（首页） | 日切放行 |
| 目录 | `04-slicegate` |
| 领域建议 | 数仓与数据湖 |
| 接口 | http://127.0.0.1:10627 |
| 库 | `data/app.db` |
| 账号 | admin / 123456 |

## 为何能出多题

迟到切片、重切、跨分区依赖、部分完整冻结、消费水位回拨

## 避开

别做成作业拓扑编排换皮；别做成纯水位大盘

## 互斥切口候选

- 迟到切片整批拒收 vs 进待补齐冻结区
- 重切失败整笔回滚 vs 保留已完整分区
- 放行与水位同步闸 vs 异步放行后纠偏

## 切片号唯一冲突口径（已定死）

切片号 `slice_no` 的唯一约束是硬闸，**任何路径都不做覆盖式写入**（无 INSERT OR REPLACE / UPSERT）。
撞号一律显式失败（HTTP 409），旧正式行的行数、期望数、完整度、放行态保持不变，且冲突不推消费水位。

按**业务日**锁定一种口径，当天不得更换；两种口径不得混用于同一天：

| 口径 | 行为 |
|------|------|
| 乙 `merge_review`（默认） | 冲突**不覆盖**正式行，写入「登记冲突册」进入合并待审队列；人工确认后按合并规则生成派生新行，或驳回 |
| 甲 `reject` | 冲突直接拒绝登记，旧行不变，冲突原因与冲突编号随响应返回，并记入登记冲突册备查 |

- 正确性口径固定为 `row_ge_expected_v1`：`expected_count>0 且 row_count>=expected_count` 为「完整」；`row_count>0` 为「部分完整」；否则「待检」。
- 合并规则固定为 `same_identity_max_counts_revision_v1`：仅同业务日 + 同分区可合并；新行 `row_count/expected_count` 取旧行与本次两报的**较大值**；新切片号派生为 `<原切片号>-R<序号>`；旧行不动；合并后仍为「未放行」，需重新裁决并显式放行；合并不推水位。
- 当天换口径、按另一口径夹带登记、二次处置已结案冲突等偏离登记冲突册的动作，一律拒绝并写入「登记口径偏离册」。
- 同一切片号并发登记：进程内按切片号加锁排队，跨连接用 SQLite `BEGIN IMMEDIATE` 串行化；后手记入 `registration_attempts`（`contended=1`），不会悄悄落成。
- 登记列表、冲突册、分区详情、处置页全部读同一个 `data/app.db`，`/api/partitions/{biz_date}/{key}` 返回 `reconciles` 供逐行对账。

接口一览：`POST /api/slices`、`GET /api/slices`、`GET/POST /api/policies`、`GET /api/conflicts`、`POST /api/conflicts/{id}/resolve`、`GET /api/registration-attempts`、`GET /api/deviations`、`GET /api/partitions`、`GET /api/partitions/{biz_date}/{partition_key}`。

## 启动（一键）

`powershell
cd d:\document\cccodex\workspace\slicegate\slicegate-bugfix-02
docker compose up -d --build
`

浏览器：http://127.0.0.1:10627/  
健康检查：GET http://127.0.0.1:10627/api/health

本机开发（可选）：

`powershell
python -m pip install -r requirements.txt
python -m uvicorn app:app --app-dir backend --host 127.0.0.1 --port 10627
python -m pytest -q
`
