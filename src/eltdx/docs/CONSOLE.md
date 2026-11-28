# 本地控制台

`eltdx-console` 是 Easy-tdxquant 自带的本机管理控制台：单进程 axum 服务，内嵌数据面，
用一个 SQLite 文件保存采集任务、运行记录、原始载荷、新闻、MCP 工具状态与审计日志。

默认只监听 `127.0.0.1:8787`，供本机浏览器使用；它不是面向公网的多租户后台。

## 定位

- **采集编排**：把「代码 → 网关方法」拆成分片，按超时/重试/并发执行，落库并记录水位。
- **数据浏览**：查看原始载荷、K 线、快照、新闻，并导出 CSV。
- **运行治理**：调度、取消、保留清理、审计。
- **数据面代理**：登录后可调用与 HTTP 网关相同的 261 个方法。

协议细节仍由 `eltdx` / `eltdx-runtime` 负责，控制台不重复实现 7709 与 7615。

## 启动

```bash
cargo build -p eltdx-console --release
./target/release/eltdx-console --port 8787 --db data/console.db
```

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `--bind` | `127.0.0.1` | 监听地址 |
| `--port` | `8787` | 监听端口，同时写回 `settings.console.port` |
| `--db` | `data/console.db` | SQLite 文件，首次启动自动建表 |
| `--session-ttl-minutes` | `720` | 会话有效期（分钟） |
| `--secure-cookies` | 关闭 | 为 Cookie 增加 `Secure`，用于 HTTPS 反向代理后 |

环境变量 `ELTDX_CONSOLE_TICK_SECONDS` 覆盖调度器心跳（默认 30 秒，夹取 1–3600），
便于本地调试与测试。

首次访问会要求创建管理员，密码至少 12 个字符，使用 argon2id 存储。

## 安全模型

- 会话令牌：32 字节随机值，只在客户端 Cookie 中；数据库只存 SHA-256，泄露库文件无法重放。
- Cookie：`HttpOnly; SameSite=Strict; Path=/`，可选 `Secure`。
- 写接口只接受 `application/json`，跨站表单无法携带该请求；所有写操作要求已登录会话。
- 登录失败按「用户名@IP」限流：10 分钟窗口内 5 次失败锁定 5 分钟。
- 关键动作写入 `audit_log`：建管理员、登录/失败、登出、任务创建/启停/运行、取消、保留清理、MCP 启停。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/` | 控制台页面（概览/任务/数据/MCP/审计） |
| `GET` | `/api/health` | 版本、schema 版本、是否已建管理员 |
| `POST` | `/api/setup` | 首次创建管理员并登录（已存在返回 409） |
| `POST` | `/api/login` | 登录 |
| `POST` | `/api/logout` | 登出并删除会话 |
| `GET` | `/api/me` | 当前会话 |
| `GET` | `/api/overview` | 计数、最近运行、审计摘要 |
| `POST` | `/api/rpc` | 代理数据面 `{method, params}` |
| `GET` `POST` | `/api/tasks` | 任务列表 / 创建任务 |
| `POST` | `/api/tasks/{id}/toggle` | 启用或停用 |
| `POST` | `/api/tasks/{id}/run` | 立即运行，202 返回 `run_id`；已在运行返回 409 |
| `GET` | `/api/runs` | 最近运行（`limit`） |
| `POST` | `/api/runs/{id}/cancel` | 请求取消运行中的任务 |
| `GET` | `/api/data/payloads` | 原始载荷列表（`dataset` `code` `limit` `offset` `format=csv`） |
| `GET` | `/api/data/payloads/{id}` | 单条载荷原文 |
| `GET` | `/api/data/klines` | K 线（`code` `period` `format=csv`） |
| `GET` | `/api/data/quotes` | 快照（`code` `format=csv`） |
| `GET` | `/api/data/news` | 新闻（`format=csv`） |
| `GET` | `/api/audit` | 审计日志（`limit`） |
| `GET` | `/api/mcp/tools` | MCP 工具目录、启停状态与网关方法清单 |
| `POST` | `/api/mcp/tools/{name}/toggle` | 启停单个 MCP 工具 |
| `GET` | `/api/mcp/calls` | 工具调用记录 |
| `GET` | `/api/maintenance/policy` | 当前保留策略 |
| `POST` | `/api/maintenance/prune` | 立即执行保留清理并返回计数 |

CSV 导出带 UTF-8 BOM，字段按 RFC 4180 转义，便于 Excel 直接打开。

## 采集语义

- **分片**：`params.codes` 按 `shard_size` 切片，分片键为 `codes:<首个代码>`；没有 `codes` 时整体作为一片。
- **执行**：每片一次网关调用，`timeout_ms` 超时，失败按 `retry` 重试（250ms 递增退避）。
- **并发**：按 `concurrency` 分批并发，批次之间检查取消标志。
- **落库**：成功分片写 `collected_payloads` 与 `collection_checkpoints`；`rows` 由响应数组长度或
  `rows`/`items`/`records`/`bars`/`data`/`list` 信封推断。
- **收尾**：全部分片成功 → `success` 并更新 `dataset_state`（`full` 模式记 `last_full_at`）；
  任一分片失败 → `failed` 且记录 `n/m shards failed`。部分成功不记为成功。

## 调度语义

| `schedule_kind` | `schedule_expr` | 行为 |
| --- | --- | --- |
| `manual` | 忽略 | 只接受手动触发 |
| `interval` | `30s` `5m` `2h` `1d` 或纯秒 | 距上次开始时间超过间隔即到期 |
| `daily` | `HH:MM`（Asia/Shanghai） | 当日该时刻已过且当天未运行过 |

- 心跳默认 30 秒；同一任务由进程内登记表独占，重复触发会被拒绝。
- 存储的时间戳无法解析时**不会**触发（避免紧循环重启），手动触发仍然可用。
- 非法表达式不会静默忽略：写入 `schedule.invalid` 审计（每任务每进程一次）。
- 调度器每 6 小时自动执行一次保留清理。

## 保留策略

`settings` 中的键与默认值：

| 键 | 默认 | 作用 |
| --- | --- | --- |
| `data.retention.raw_days` | 7 | `collected_payloads` |
| `data.retention.snapshot_days` | 30 | `quote_snapshots` |
| `data.retention.news_days` | 365 | 新闻文章与正文 |
| `data.retention.audit_days` | 90 | `audit_log` |
| `data.retention.change_keep` | 10000 | `change_log` 保留条数 |

`days < 0` 表示该类别不清理；清理后执行 `PRAGMA incremental_vacuum`。

## 数据表

`schema_migrations`、`settings`、`admin_users`、`admin_sessions`、`collection_tasks`、
`collection_runs`、`dataset_state`、`collection_checkpoints`、`collected_payloads`、
`quote_snapshots`、`kline_bars`、`minute_bars`、`trade_ticks`、`f10_documents`、
`news_channels`、`news_articles`、`news_bodies`、`change_log`、`mcp_tools`、`mcp_call_log`、`audit_log`。

schema 版本当前为 2，迁移只向前。

## 已知边界

- **通用分页未实现**：一个分片一次调用。不同网关方法的翻页参数不同，凭响应形状猜测会静默丢页，
  因此通用分页等有明确口径后再加适配器；新闻分页由专门的新闻采集器承担。
- **取消是合作式的**：只在批次之间生效，在途请求会跑完并保留已取数据，run 记为 `cancelled`。
- **MCP 调用日志**：`mcp_call_log` 由实际执行 MCP 调用的一方写入；控制台自身只代理网关 RPC，
  不会自动产生工具调用记录。
- **单管理员模型**：没有多用户与 RBAC；面向本机单人使用。
- **仅回环**：默认绑定 `127.0.0.1`，未内置 TLS；如需远程访问请置于反向代理之后并开启 `--secure-cookies`。
