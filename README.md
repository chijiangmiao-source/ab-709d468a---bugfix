# 航迹导出服务（track-export）

海洋测绘队向协作方导出航迹前的遮蔽导出服务。值班员在页面上编辑字段遮蔽规则、
提交带稳定导出标识的少量 JSON 记录，并轮询查看处理阶段、冻结规则摘要与已发布
工件摘要。纯 Python 3.11 标准库实现（无第三方依赖）。

## 架构

```
┌────────────┐   HTTP    ┌─────────────────────────────┐
│  值班员页面 │ ───────▶ │ app (python -m app.server)   │
│ (真实API轮询)│ ◀─────── │  /api/rules /api/exports ... │
└────────────┘           └──────────┬──────────────────┘
                                    │ SQLite (WAL) + 工件目录（共享卷）
                         ┌──────────┴──────────────────┐
                         │ worker ×2 (app.worker)       │
                         │ 租约 → 暂存 → 核验 → 原子发布 │
                         │ 崩溃恢复：收敛 / 清理重排队   │
                         └─────────────────────────────┘
```

- **同一持久化裁决**：`POST /api/exports` 在单个 `IMMEDIATE` 事务中读取当前规则并
  冻结「规范化输入 + 规则快照 + 决策摘要 + 日志」。此后改动当前规则不影响该标识的导出。
- **工件身份只属于该导出**：最终工件只由该标识冻结的记录、规则快照和首次回执决定，
  并内嵌绑定这四者（export_id / input_digest / rules_digest / receipt_id）的
  `integrity_digest`。相同规则可服务多份导出，但绝不跨导出缓存或复用渲染结果——
  不会再出现“第二份导出下载到第一份的标识、输入摘要与遮蔽记录”。下载在内容摘要之外
  再核验内嵌身份，摘要合法但属于别的导出的字节一律拒绝投递。
- **错误发布状态安全收敛**：已是 `PUBLISHED` 但磁盘工件缺失、损坏或内嵌身份属于另一
  导出（历史缺陷遗留）时，worker 启动审计与周期审计会隔离旧文件作为证据、按冻结裁决
  重新渲染并原子替换，事务内更正工件摘要列；阶段始终保持 `PUBLISHED`（不倒退），任何
  时刻都不投递未核验内容，且原始证据保留在工件表（aborted）与隔离目录中。
- **幂等与冲突**：同一标识的业务等价重传（键序/空白/整型浮点/Unicode 组合差异）返回
  首次回执（HTTP 200，`replay: true`），不产生第二个工件；记录或规则快照不同则返回
  HTTP 409 并保留原有证据（原裁决行不被修改，冲突尝试记入日志）。
- **租约**：worker 必须持有有效租约（带 fencing token）才能处理；发布前复查租约。
  租约过期后可被其他 worker 接管，fencing 递增使旧持有者失效。
- **工件管线**：临时文件（fsync）→ 摘要登记 → 重读核验（内容摘要 + 内嵌身份）→
  `link(2)` 原子发布（不可覆盖）→ 事务内标记 `PUBLISHED`。下载接口只投递摘要与身份
  都核验通过的已发布工件。
- **崩溃恢复**：worker 启动及每个 tick 扫描未完成导出——暂存工件完整（摘要与日志、
  确定性重算、内嵌身份一致）则**收敛**到同一工件发布；否则**清理**残缺工件并重排队；
  孤儿临时文件定期清扫。`PUBLISHED` 为终态，任何路径都不能使其倒退。
- **唯一发布**：租约串行化 + `artifacts` 表部分唯一索引（每导出仅一条 published）
  + 不可覆盖链接 + 阶段 CAS，四重保证两个 worker 并行时同一导出只发布一次。

## 快速开始（Docker Compose）

```sh
./scripts/verify.sh        # 构建、启动 app+2×worker、运行一次性 verify、以退出码报告
```

或手动：

```sh
APP_PORT=8080 docker compose up -d --build app worker   # 可配置宿主端口
curl localhost:8080/healthz                              # 健康响应
docker compose run --rm verify                           # 一次性验收；echo $? 查看结果
docker compose down -v                                   # 重置全部状态
```

页面：<http://localhost:8080/>（每 2 秒轮询真实 API）。

## verify 验收内容（执行后退出，退出码即结果）

1. **构建检查**：`python -m compileall app verify tests`
2. **代码测试**：`python -m unittest discover -s tests`（57 个用例：规范化、遮蔽、
   裁决/幂等/冲突、阶段单调、租约 fencing、恢复收敛/清理、双 worker 竞态、
   同规则多导出身份隔离、已发布错误态自愈）
3. **API/HTTP 冒烟**：
   - **同规则两份导出（核心回归）**：先提交并等第一份发布，再用同一规则提交记录
     可明显区分的第二份；逐一下载核对各自的 export_id、输入摘要、遮蔽记录与工件
     摘要，且两份摘要互不相同。
   - 规则改动后，已冻结导出仍按冻结快照导出（E1 用 R1、E2 用 R2，互不影响）
   - **错误发布状态自愈**：植入历史错误态（第二份显示 PUBLISHED、下载却带第一份
     的工件），确认下载不泄露他人身份；随后通过挂载的 Docker socket **重启 app 与
     2× worker**，再确认第二份收敛到自己可下载、可核验的工件、阶段不倒退、证据被
     隔离保留，第一份不受影响。
   - 崩溃恢复：暂存完整后崩溃 → 收敛到同一完整工件；写一半崩溃 → 清理残缺并重处理
   - 业务等价重传 → 首次回执且无第二个工件；记录/规则快照不同 → 409 且证据保留
   - 下载接口在崩溃窗口内只返回 409，绝不暴露未核验内容

> verify 服务以只读方式挂载数据卷，并挂载 `/var/run/docker.sock` 仅用于本轮验收中
> 重启 app/worker 后复核；生产部署无需该挂载。

## 本地开发（无 Docker）

```sh
python3 -m unittest discover -s tests -t .     # 单元测试
./scripts/smoke-local.sh                        # 完整本地冒烟（server + 2 worker + verify）
```

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/healthz` | 健康响应 |
| GET/PUT | `/api/rules` | 查看 / 替换当前遮蔽规则（版本+摘要） |
| POST | `/api/exports` | 提交 `{export_id, records}` → 201 / 200(replay) / 409(conflict) |
| GET | `/api/exports` | 列表：阶段、冻结规则摘要、输入摘要、工件摘要 |
| GET | `/api/exports/{id}` | 详情 + 处理日志 + 当前租约 |
| GET | `/api/exports/{id}/artifact` | 下载已发布工件（摘要核验，否则 409/410/500） |
| POST | `/api/test/fault` | 故障注入（仅 `TEST_HOOKS=1`）：`crash_partial_write` / `crash_after_staged` |
| POST | `/api/test/corrupt` | 植入历史错误态（仅 `TEST_HOOKS=1`）：`{export_id, donor_id}` 把 victim 已发布工件替换为 donor 的字节，验证下载拒绝与重启自愈 |

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `APP_PORT` | `8080` | Compose 宿主端口（`APP_PORT=9090 docker compose up`） |
| `PORT` | `8080` | 容器内监听端口 |
| `DATA_DIR` | `./data` | SQLite 与工件根目录 |
| `LEASE_TTL_SECONDS` | `10` | 租约时长（崩溃接管延迟的上界） |
| `POLL_INTERVAL_SECONDS` | `0.5` | worker 轮询间隔 |
| `REPAIR_INTERVAL_SECONDS` | `30` | worker 周期审计已发布工件（缺失/损坏/串身份自愈）的间隔 |
| `TEST_HOOKS` | 关 | 置 `1` 开启故障注入端点（验收用，生产应关闭） |

## 遮蔽规则示例

```json
{"rules": [
  {"field": "depth_m",   "action": "redact", "replacement": "***"},
  {"field": "lat",       "action": "round",  "precision": 2},
  {"field": "vessel_id", "action": "hash",   "length": 12},
  {"field": "note",      "action": "drop"}
]}
```
