# logos-fs

Logos 给 AI agent 提供了一套通过 URI 访问一切资源的内核：消息、记忆、任务、沙箱、工具、服务、设备，全部统一在 `logos://` 地址空间下，agent 只需要 5 个操作就能和整个系统交互。

```
Agent --logos_call/read/write/exec--> MCP adapter (JSON-RPC stdio)
  --> gRPC unix socket --> VFS 路由表 --> namespace handlers
```

> [English](./README.md)

## gRPC 服务

Proto: `proto/logos.proto`，package `logos.kernel.v1`。

### Agent 原语

```protobuf
rpc Read(ReadReq)   returns (ReadRes);    // uri -> content
rpc Write(WriteReq) returns (WriteRes);   // uri, content -> ()
rpc Patch(PatchReq) returns (PatchRes);   // uri, partial -> () (JSON deep merge)
rpc Exec(ExecReq)   returns (ExecRes);    // command -> stdout, stderr, exit_code
rpc Call(CallReq)   returns (CallRes);    // tool, params_json -> result_json
```

- `Read/Write/Patch` 是纯数据操作，无副作用。
- `Exec` 在 agent 的沙箱容器里跑 shell。命令中的 `logos://sandbox/` 和 `logos://services/` URI 会自动翻译成真实路径，其他命名空间不能通过 Exec 访问。
- `Call` 分发到 proc 工具（内置或外部）。agent 通过它调用 `system.complete`、`memory.search` 等。
- 五个原语都需要有效 session（通过 Handshake 建立），管理接口除外。

### 管理接口（runtime 调用，不是 agent 调用）

```protobuf
rpc Handshake(HandshakeReq)         returns (HandshakeRes);
rpc RegisterToken(RegisterTokenReq) returns (RegisterTokenRes);
rpc RevokeToken(RevokeTokenReq)     returns (RevokeTokenRes);
```

流程：runtime 调 `RegisterToken(token, task_id, agent_config_id, role)` → 启动 agent 并传入 token → agent 连接 UDS 发 `Handshake(token)` → kernel 绑定连接到 task，在 `x-logos-session` header 里返回 session key → 后续所有调用自动限定在该 task 范围内。

角色：`user`（对 system/services/devices 只读）或 `admin`（完全访问）。没有 session header 的连接视为 admin（供管理接口使用）。

## 命名空间

| 命名空间 | 后端 | 持久 | 存什么 |
|---------|------|:----:|--------|
| `memory` | SQLite per group + FTS5 | 是 | 消息、摘要（short/mid/long）、知识图谱 |
| `system` | SQLite | 是 | 任务、锚点、搜索索引 |
| `sandbox` | 宿主 FS，containerd + overlayfs | 是 | 每个 task 的工作目录、执行日志 |
| `users` | 文件系统 | 是 | 用户档案、persona（short/mid/long.md） |
| `proc` | 内存 | 否 | 工具注册表 + 调用 session slot |
| `proc-store` | 文件系统 + git | 是 | 外部工具声明和代码 |
| `services` | 内存（boot 时从 svc-store 恢复） | 否 | 运行中的服务注册表 |
| `svc-store` | 文件系统 | 是 | 服务制品（compose.yaml、二进制等） |
| `devices` | 内存 | 否 | 硬件/虚拟设备驱动 |
| `tmp` | 内存 HashMap | 否 | 临时 KV |

## 路径参考

### memory

| 路径 | Read | Write | Patch |
|------|------|-------|-------|
| `groups/{gid}/messages` | — | 插入消息 JSON（`ts`, `speaker`, `text`, `mentions`, `reply_to`） | — |
| `groups/{gid}/messages/{id}` | 单条消息 | — | — |
| `groups/{gid}/summary/{layer}/latest` | 最新摘要 | — | — |
| `groups/{gid}/summary/{layer}/{period}` | 该期摘要 | 写入/覆盖 | Deep merge |
| `groups/{gid}/graph/` | 列出所有 subject | — | — |
| `groups/{gid}/graph/{subject}` | 该 subject 的所有三元组 | 写入/upsert | 走 write |
| `groups/{gid}/views/{name}` | 查询视图（参数为 JSON） | — | — |
| `plugins/` | 列出已挂载插件及文档 | — | — |

`layer` = `short`（小时）/ `mid`（日）/ `long`（月）。`period` = ISO 8601 前缀。

默认挂载 `summary` 和 `graph` 两个插件，各自有独立的 URI 结构。通过 `logos://memory/plugins/` 发现更多。

### system

| 路径 | Read | Write | Patch |
|------|------|-------|-------|
| `tasks` | 列出所有非 pending 任务 | 创建任务（JSON：`task_id`, `description`, `chat_id` 等） | — |
| `tasks/{id}` | 任务 JSON（含 `plan_todo`, `plan_parent`） | — | Deep merge |
| `tasks/{id}/description` | — | 更新描述 | — |
| `anchors/{task_id}` | 该任务所有锚点 | — | — |
| `anchors/{task_id}/{anchor_id}` | 单个锚点 | — | — |

任务状态：`pending → active ⇄ sleep → finished`。pending 对 agent 不可见。所有状态转换走 `system.complete`，不能直接写。

任务表字段：`task_id`, `description`, `workspace`, `resource`, `status`, `chat_id`, `trigger`, `plan_todo`（JSON 字符串数组）, `plan_parent`（planner task ID）, `created_at`, `updated_at`。

### sandbox

| 路径 | Read | Write |
|------|------|-------|
| `{task_id}/...` | 文件内容 / 目录列表 | 写文件（自动建父目录） |
| `{task_id}/log` | 读日志 | 追加（通过 `system.complete`） |
| `__system__/proc/{name}/` | 工具代码 | — |
| `__system__/svc/{name}/` | 服务制品 | — |

容器按 `agent_config_id` 建，同一 agent 的不同 task 共用一个容器。`logos://sandbox/{task_id}/...` → 容器内 `/workspace/...`。

### users

| 路径 | Read | Write | Patch |
|------|------|-------|-------|
| `{uid}/...` | 文件 / 目录列表 | 写文件 | Deep merge JSON |
| `{uid}/persona/short/{period}` | 读 | **追加**到 JSON 数组 | — |
| `{uid}/persona/mid/` | 读 | 覆盖 | — |
| `{uid}/persona/long.md` | 读 | 覆盖 | — |

`persona/short` 是 append-only，每次写入往数组追加一条。文件损坏会报错（不会静默清零）。

### proc

| 路径 | Read | Write |
|------|------|-------|
| `/` | 列出所有工具名 | — |
| `{tool}` 或 `{tool}/.schema` | 工具 schema JSON | — |
| `{tool}/{call_id}/input` | 读回参数 | 提交参数 → 触发执行 |
| `{tool}/{call_id}/output` | 取结果（取后清除 slot） | — |
| `{tool}/{call_id}/error` | 错误或 "null" | — |

实际使用中 agent 直接调 `logos_call(tool, params)`，call_id 生命周期由内核管理。

### proc-store

| 路径 | Read | Write |
|------|------|-------|
| `/` | 列出工具名 | — |
| `{tool}/schema.json` | 工具声明 | 注册工具（含 `run` 命令、可选 `git`） |
| `{tool}/...` | 其他文件 | 写文件 |

有 `git` 字段 → boot 时 clone 到 `sandbox/__system__/proc/{tool}/`，已有则 `git pull --ff-only`。

### services

| 路径 | Read | Write | Patch |
|------|------|-------|-------|
| `/` | 列出服务名 | — | — |
| `{name}` | ServiceEntry JSON | 注册/更新 | Deep merge |

ServiceEntry: name, source (builtin/agent), svc_type (oneshot/daemon), endpoint, status。

### svc-store

| 路径 | Read | Write |
|------|------|-------|
| `/` | 列出服务名 | — |
| `{name}/compose.yaml` | 服务清单 | 写 |
| `{name}/artifacts/...` | 构建制品 | 写 |

### devices

| 路径 | Read | Write |
|------|------|-------|
| `/` | 列出设备名 | — |
| `{name}` 或 `{name}/capabilities` | 能力 JSON | — |
| `{name}/state` | 状态 JSON | — |
| `{name}/control` | — | 发命令 |

### tmp

| 路径 | Read | Write | Patch |
|------|------|-------|-------|
| `/` | 列出所有 key | — | — |
| `{key/path}` | 取值 | 存值 | 覆盖 |

## 内置 Proc 工具

boot 时注册，通过 `logos_call` 调用。

### system.complete

轮次终止符。每个 agent turn 必须以此调用结束。

```json
{
  "task_id": "task-001",
  "summary": "这个 turn 做了什么",
  "reply": "发给用户的消息（可选）",
  "anchor": true,
  "anchor_facts": "[{\"type\":\"decision\",\"topic\":\"字体\",\"value\":\"宋体\"}]",
  "task_log": "执行日志，追加到 sandbox/log",
  "sleep_reason": "awaiting_user（可选 — task 挂起而非完成）",
  "sleep_retry": false,
  "resume": "task-042（可选 — 放弃当前 task，激活这个）",
  "plan": {
    "todo": ["第一步描述", "第二步描述", "第三步描述"]
  }
}
```

三条互斥路径：

1. **完成**（默认）：task → `finished`。`anchor: true` 时创建锚点快照。
2. **挂起**：设了 `sleep_reason` 则 task → `sleep`。
3. **恢复**：设了 `resume` 则删除当前 task，激活目标 task。

**Plan 模式**：`plan.todo` 非空时，task → `sleep`，todo 列表存入任务表。Scheduler 取第一条建 executor task（`trigger: "plan"`, `plan_parent: "task-001"`），等它完成后取下一条。Planner 被唤醒时可以提交新 todo、修改计划或直接完成——计划不是死板清单，每次唤醒都是一次完整的重新决策。

执行后 `task_log` 写入 `logos://sandbox/{task_id}/log`。

### memory.search

```json
{ "chat_id": "group-1", "query": "搜索词", "limit": 10 }
```

FTS5 全文搜索。最多 50 条。

### memory.range_fetch

```json
{ "chat_id": "group-1", "ranges": [[100, 200], [300, 350]], "limit": 50, "offset": 0 }
```

按 ID 区间批量取消息。每次最多 200 条。

### system.search_tasks

```json
{ "query": "报错片段或关键词", "limit": 10 }
```

两级搜索：L1 = 锚点 facts 的 BM25 匹配，L2 = 任务 description 关键词匹配。返回 `{ "l1_hits": [...], "l2_hits": [...] }`。

### system.get_context

```json
{ "chat_id": "group-1", "sender_uid": "user-123", "msg_id": 456 }
```

组装 turn 上下文：当前 session、最新 short 摘要、发送者 persona 路径。给 runtime 在 agent 第一个 token 之前调用。

## Session 聚类

三层 LRU，拓扑优先：

| 层 | 存储 | 触发 |
|---|------|------|
| L0 | 内存 | 活跃 session |
| L1 | 内存 | 从 L0 淘汰 |
| L2 | LanceDB（向量库） | 从 L1 淘汰，带 embedding 持久化 |

消息路由：
1. 有 `reply_to`？→ 跟着回复链走（硬绑定）。目标 session 在 L2 则 page fault 回 L0。
2. 没有回复链？→ 在 L2 做 Ollama embedding 语义搜索（软绑定，L2 距离 < 0.5）。
3. 都没匹配？→ 建新 session。

Embedding：Ollama `qwen3-embedding:0.6b`，1024 维。可通过 `OLLAMA_URL`、`EMBED_MODEL`、`EMBED_DIM` 环境变量配置。

LanceDB 初始化失败时降级为纯内存（L0+L1），打 WARNING。此模式下 session 不会跨重启持久化。

## Cron 与 Consolidator

Scheduler 每 60 秒 tick。boot 时注册 5 个 consolidator job：

| Job | 频率 | 产出 |
|-----|------|------|
| `consolidate-short-summary` | 每小时 :00 | 消息 → short 摘要 |
| `consolidate-short-persona` | 每小时 :05 | 消息 → persona 观察 |
| `consolidate-graph` | 每小时 :10 | 消息 → 知识图谱三元组 |
| `consolidate-mid-summary` | 每天 03:00 | short 摘要 → mid 摘要 |
| `consolidate-mid-persona` | 每天 03:30 | short persona → 重写 mid persona |

Scheduler 同时负责 **plan 调度**：扫描带 `plan_todo` 的 sleeping task，取第一条建 executor task，一个 planner 同时只有一个 executor。

## 访问控制

- **Sandbox 隔离**：task owner 可读写自己的 sandbox，其他人只能读 finished 的。
- **命名空间权限**：`user` 角色对 `services/`、`system/`、`devices/` 只读。
- **Exec/Call**：需要有效 session。
- **无 session = admin**：管理接口调用（无 header）视为 admin。
- **JSON 校验**：中间件在 `system/`、`memory/` 写入前验证 JSON。

## 启动顺序

```
1. users/
2. memory/（+ session store，可选 LanceDB L2）
3. system/（SQLite）
4. tmp/
5. sandbox/（containerd + overlayfs）
6. proc/（注册内置工具：memory.search, memory.range_fetch,
         system.search_tasks, system.get_context, system.complete）
7. proc-store/（恢复外部工具，git clone/pull）
8. services/（从 svc-store 恢复）
9. svc-store/
10. devices/
11. sandbox/（最终挂载）
→ table.open() — 开始接受连接
→ cron.start() + 注册 5 个 consolidator job
```

## 项目结构

```
logos-fs/
├── proto/logos.proto            # gRPC 服务定义（5 原语 + 管理接口）
├── crates/
│   ├── logos-vfs/               # Namespace trait, RoutingTable, URI 解析, 中间件
│   ├── logos-kernel/            # gRPC 服务, 命名空间挂载, token, cron, consolidator,
│   │                            # 内置工具, sandbox (containerd), 上下文注入
│   ├── logos-mm/                # 记忆：消息、摘要、图谱、FTS5、视图插件
│   ├── logos-system/            # 任务、锚点、搜索、logos_complete 逻辑、plan 存储
│   ├── logos-session/           # Session 聚类：reply chain 拓扑、三层 LRU、
│   │                            # LanceDB L2 + Ollama embedding
│   └── logos-mcp/               # MCP 适配器：stdio JSON-RPC ↔ gRPC UDS
├── test.Dockerfile              # Debian slim 构建 + 测试环境
└── data/state/                  # 运行时数据（SQLite、sandbox root）
```

## 环境变量

| 变量 | 默认值 | 用途 |
|-----|--------|------|
| `VFS_USERS_ROOT` | `../../data/state/entities` | users 命名空间根目录 |
| `VFS_MEMORY_ROOT` | `../../data/state/memory` | 记忆 DB 根目录 |
| `VFS_SYSTEM_DB` | `../../data/state/system.db` | system SQLite 路径 |
| `VFS_SANDBOX_ROOT` | `../../data/state/sandbox` | sandbox 根目录 |
| `VFS_PROC_STORE_ROOT` | `../../data/state/proc-store` | proc-store 根目录 |
| `VFS_SVC_STORE_ROOT` | `../../data/state/svc-store` | svc-store 根目录 |
| `VFS_LISTEN` | `unix://{sandbox_root}/logos.sock` | gRPC 监听地址 |
| `OLLAMA_URL` | `http://localhost:11434` | Ollama API 地址 |
| `EMBED_MODEL` | `qwen3-embedding:0.6b` | embedding 模型名 |
| `EMBED_DIM` | `1024` | embedding 维度 |
| `LOGOS_SOCKET` | `../../data/state/sandbox/logos.sock` | MCP 适配器 socket 路径 |
| `LOGOS_TOKEN` | （空） | MCP 适配器 handshake token |
