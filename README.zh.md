# logos-fs

Logos 数据内核 — 基于 URI 寻址的虚拟文件系统，通过 gRPC 提供 6 个 agent 原语，上层经 MCP (JSON-RPC 2.0) 适配后供 AI agent 使用。

```
AI Agent → MCP (JSON-RPC) → gRPC 内核 → VFS 路由表 → 命名空间
```

> [English](./README.md)

## 一、gRPC 原语

| RPC | 输入 | 输出 | 语义 |
|-----|------|------|------|
| **Read** | uri | content | URI 路由 → namespace.read |
| **Write** | uri, content | — | URI 路由 → namespace.write |
| **Patch** | uri, partial | — | URI 路由 → namespace.patch（通常 JSON deep merge） |
| **Exec** | command | stdout, stderr, exit_code | 沙箱容器执行 shell，命令中 `logos://` 自动翻译为容器路径 |
| **Call** | tool, params_json | result_json | proc 工具分发（内置或外部 git 项目） |
| **Complete** | summary, reply, anchor, task_log, sleep_reason, sleep_retry, resume_task_id, anchor_facts | reply, anchor_id | turn 终止符：写日志、建锚点、切任务、进睡眠 |

管理接口（runtime 调用，非 agent 调用）：

| RPC | 用途 |
|-----|------|
| **Handshake** | 一次性 token 消费 → 绑定 session |
| **RegisterToken** | runtime 预注册 token（token, task_id, role） |
| **RevokeToken** | 撤销未使用的 token |

## 二、命名空间总览

| # | 命名空间 | 后端 | 持久 | 职责 |
|---|---------|------|:----:|------|
| 1 | **memory** | SQLite (per-gid) + FTS5 | ✓ | 聊天消息、三层摘要、视图查询 |
| 2 | **system** | SQLite | ✓ | 任务状态机、锚点、搜索 |
| 3 | **sandbox** | 宿主 FS → Docker bind-mount | ✓ | 每任务工作目录 + 执行环境 |
| 4 | **users** | 文件系统 | ✓ | 用户档案、偏好、渐进式画像 |
| 5 | **proc** | 内存 call slot | ✗ | 工具注册表 + 调用会话 |
| 6 | **proc-store** | 文件系统 + git clone | ✓ | 外部工具声明与代码分发 |
| 7 | **services** | 内存（boot 时从 svc-store 恢复） | ✗ | 活跃服务注册表 |
| 8 | **svc-store** | 文件系统 | ✓ | 服务制品（compose.yaml 等） |
| 9 | **devices** | 内存驱动注册 | ✗ | 硬件/虚拟设备抽象 |
| 10 | **tmp** | 内存 HashMap | ✗ | 临时 KV，重启丢失 |

## 三、路径 × 操作矩阵

### 1. memory

| 路径 | Read | Write | Patch |
|------|------|-------|-------|
| `groups/{gid}/messages` | "null" | **插入**消息（ts/speaker/text/mentions） | — |
| `groups/{gid}/messages/{id}` | 单条消息 JSON | — | — |
| `groups/{gid}/summary/{layer}/latest` | 最新摘要 | — | — |
| `groups/{gid}/summary/{layer}/{period}` | 该期摘要 | 写入/覆盖 | **deep merge** |
| `groups/{gid}/views/{name}` | 查询视图 | — | — |

> layer = short(小时) / mid(日) / long(月)；period = ISO8601（`2026-03-20T10` / `2026-03-20` / `2026-03`）

### 2. system

| 路径 | Read | Write | Patch |
|------|------|-------|-------|
| `tasks` | 列出非 pending 任务 | 创建任务 | — |
| `tasks/{id}` | 任务详情 | — | — |
| `tasks/{id}/description` | — | 更新描述 | — |
| `anchors/{task_id}` | 该任务所有锚点 | — | — |
| `anchors/{task_id}/{anchor_id}` | 单个锚点 | — | — |

> 状态机：`pending → active ⇄ sleep → finished`

### 3. sandbox

| 路径 | Read | Write | Patch |
|------|------|-------|-------|
| `{task_id}/...` | 文件内容 / 目录列表 | 写文件（自动建父目录） | 覆盖 |
| `{task_id}/log` | 读日志 | — | **追加**（换行分隔） |
| `__system__/proc/{name}/` | 工具代码目录 | — | — |
| `__system__/svc/{name}/` | 服务制品目录 | — | — |

> 容器内映射：`logos://sandbox/{task_id}/...` → `/workspace/...`

### 4. users

| 路径 | Read | Write | Patch |
|------|------|-------|-------|
| `{uid}/...` | 文件 / 目录列表 | 写文件 | **deep merge** JSON |
| `{uid}/persona/short/{period}` | 读 | **追加** JSON 数组项 | — |
| `{uid}/persona/mid/` | 读 | 覆盖 | — |
| `{uid}/persona/long.md` | 读 | 覆盖 | — |

### 5. proc

| 路径 | Read | Write |
|------|------|-------|
| `/` | 列出所有工具名 | — |
| `{tool}` 或 `{tool}/.schema` | 工具 schema JSON | — |
| `{tool}/{call_id}/input` | 读回参数 | **提交参数 → 触发同步执行** |
| `{tool}/{call_id}/output` | 取结果（**取后清除 slot**） | — |
| `{tool}/{call_id}/error` | 错误 / "null" | — |

### 6. proc-store

| 路径 | Read | Write |
|------|------|-------|
| `/` | 列出工具名 | — |
| `{tool}/schema.json` | 工具声明 | **注册工具**（含 `run`、可选 `git`） |
| `{tool}/...` | 其他文件 | 写文件 |

> 有 `git` 字段 → 启动时 `git clone` 到 `sandbox/__system__/proc/{tool}/`，已有则 `git pull --ff-only`

### 7. services

| 路径 | Read | Write | Patch |
|------|------|-------|-------|
| `/` | 列出服务名 | — | — |
| `{name}` | ServiceEntry JSON | 注册/更新 | **deep merge** |

> ServiceEntry: name, source(builtin/agent), svc_type(oneshot/daemon), endpoint, status

### 8. svc-store

| 路径 | Read | Write |
|------|------|-------|
| `/` | 列出服务名 | — |
| `{name}/compose.yaml` | 服务清单 | 写 |
| `{name}/artifacts/...` | 构建制品 | 写 |

### 9. devices

| 路径 | Read | Write |
|------|------|-------|
| `/` | 列出设备名 | — |
| `{name}` 或 `{name}/capabilities` | 能力 JSON | — |
| `{name}/state` | `{volume, muted, dark_mode, battery_percent, battery_charging}` | — |
| `{name}/control` | — | 发命令：`set_volume`/`mute`/`unmute`/`screenshot`/`dark_mode` |

### 10. tmp

| 路径 | Read | Write | Patch |
|------|------|-------|-------|
| `/` | 列出所有 key | — | — |
| `{key/path}` | 取值 | 存值 | 覆盖 |

## 四、内置 Proc 工具

| 工具名 | 参数 | 功能 |
|--------|------|------|
| `memory.search` | chat_id, query, limit | FTS5 全文搜索 |
| `memory.range_fetch` | chat_id, ranges, limit, offset | 消息范围批量取 |
| `memory.view.by_speaker` | chat_id, speaker, limit | 按发言人过滤 |
| `memory.view.recent` | chat_id, limit | 最近 N 条 |
| `system.search_tasks` | query, limit | 任务+锚点多级检索 |
| `system.get_context` | chat_id, sender_uid, msg_id? | 自动注入 session+摘要+画像 |

## 五、内核子系统

| 子系统 | 存储 | 职责 |
|--------|------|------|
| **Token Registry** | 内存（24h TTL） | 一次性 token → session 绑定；role = User / Admin |
| **Session Clustering** | 内存 L0/L1 + LanceDB L2 | 基于 reply chain 的会话聚类，三层 LRU 淘汰 |
| **Cron Scheduler** | 内存 job 表 | 60s tick，cron 表达式匹配 → 创建 pending task |
| **Consolidator** | 注册 4 个 cron job | 渐进式记忆压缩：消息→小时摘要→日摘要；画像同理 |
| **Context Injector** | 无状态 | turn 开始前组装 session + summary + persona |
| **VFS Middleware** | — | JsonValidator：system/ 和 memory/ 写入前验证 JSON |

### Consolidator 定时任务

| Cron Job | 频率 | 做什么 |
|----------|------|--------|
| `consolidate-short-summary` | 每小时 :00 | 原始消息 → short 摘要 |
| `consolidate-short-persona` | 每小时 :05 | 原始消息 → 用户画像观察 |
| `consolidate-mid-summary` | 每天 03:00 | short 摘要 → mid 摘要 |
| `consolidate-mid-persona` | 每天 03:30 | short 画像 → 重写 mid 画像 |

### Session Clustering 三层 LRU

| 层 | 存储 | 用途 |
|---|------|------|
| L0 | 内存 HashMap | 活跃会话（最近有回复） |
| L1 | 内存 HashMap | 不活跃（从 L0 LRU 淘汰） |
| L2 | LanceDB | 归档（从 L1 淘汰时持久化，reply 时 page fault 加载回 L0） |

> 核心：reply chain 是硬绑定，语义相似度只是 fallback。

## 六、访问控制

| 规则 | 描述 |
|------|------|
| Sandbox 隔离 | task 只能访问 `sandbox/{own_task_id}/`，不能越界 |
| 只读命名空间 | User 角色不可写 `services/`、`system/`、`devices/` |
| 无 session = Admin | 管理接口调用（无 header）视为 admin |
| JSON 校验 | `system/`、`memory/` 的 write/patch 前必须是合法 JSON |

## 七、MCP 适配层

| MCP 工具 | 映射到 |
|----------|--------|
| `logos_read` | gRPC Read |
| `logos_write` | gRPC Write |
| `logos_exec` | gRPC Exec |
| `logos_call` | gRPC Call |
| `logos_complete` | gRPC Complete |

## 八、启动顺序

```
users → memory → system → tmp → sandbox(创建)
→ proc(内置工具) → proc-store(恢复外部工具 + git clone)
→ services(从 svc-store 恢复) → svc-store
→ devices(macOS 驱动) → sandbox(挂载) → table.open()
→ cron.start() + consolidator 注册 4 jobs
```

## 项目结构

```
logos-fs/
├── proto/logos.proto          # gRPC 服务定义
├── crates/
│   ├── logos-vfs/             # VFS 核心：Namespace trait, RoutingTable, URI 解析, Middleware
│   ├── logos-kernel/          # 内核：gRPC 实现, 命名空间挂载, Token, Cron, Consolidator, Context
│   ├── logos-mm/              # 记忆模块：消息存储, 摘要, 视图, FTS5
│   ├── logos-system/          # 系统模块：任务状态机, 锚点, 搜索, Complete 处理
│   ├── logos-session/         # 会话聚类：reply chain 拓扑, 三层 LRU, LanceDB L2
│   └── logos-mcp/             # MCP 适配器：gRPC → JSON-RPC 2.0
```
