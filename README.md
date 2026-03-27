# logos-fs

URI-addressed virtual filesystem data kernel. Exposes 5 agent primitives over gRPC, adapted to AI agents via MCP (JSON-RPC 2.0).

```
AI Agent → MCP (JSON-RPC) → gRPC Kernel → VFS Routing Table → Namespaces
```

> [中文文档](./README.zh.md)

## 1. gRPC Primitives

| RPC | Input | Output | Semantics |
|-----|-------|--------|-----------|
| **Read** | uri | content | URI-routed → namespace.read |
| **Write** | uri, content | — | URI-routed → namespace.write |
| **Patch** | uri, partial | — | URI-routed → namespace.patch (typically JSON deep merge) |
| **Exec** | command | stdout, stderr, exit_code | Shell execution in agent's sandbox container; `logos://` in commands auto-translated to container paths. Requires valid session. |
| **Call** | tool, params_json | result_json | Proc tool dispatch (built-in or external git projects). Requires valid session. |

> `logos_complete` is NOT a kernel primitive — it is a runtime-side protocol for runtime implementors. See RFC 002 §9.1.

Management interface (called by runtime, not agents):

| RPC | Purpose |
|-----|---------|
| **Handshake** | Consume one-time token → bind session |
| **RegisterToken** | Runtime pre-registers token (token, task_id, agent_config_id, role) |
| **RevokeToken** | Revoke unused token |

## 2. Namespace Overview

| # | Namespace | Backend | Persistent | Purpose |
|---|-----------|---------|:----------:|---------|
| 1 | **memory** | SQLite (per-gid) + FTS5 | ✓ | Chat messages, 3-layer summaries, view queries |
| 2 | **system** | SQLite | ✓ | Task state machine, anchors, search |
| 3 | **sandbox** | Host FS → Docker bind-mount | ✓ | Per-agent working directory + execution environment |
| 4 | **users** | Filesystem | ✓ | User profiles, preferences, progressive persona |
| 5 | **proc** | In-memory call slots | ✗ | Tool registry + call sessions |
| 6 | **proc-store** | Filesystem + git clone | ✓ | External tool declarations & code distribution |
| 7 | **services** | In-memory (restored from svc-store at boot) | ✗ | Active service registry |
| 8 | **svc-store** | Filesystem | ✓ | Service artifacts (compose.yaml, etc.) |
| 9 | **devices** | In-memory driver registry | ✗ | Hardware/virtual device abstraction |
| 10 | **tmp** | In-memory HashMap | ✗ | Ephemeral KV, lost on restart |

## 3. Path × Operation Matrix

### 3.1 memory

| Path | Read | Write | Patch |
|------|------|-------|-------|
| `groups/{gid}/messages` | "null" | **Insert** message (ts/speaker/text/mentions) | — |
| `groups/{gid}/messages/{id}` | Single message JSON | — | — |
| `groups/{gid}/summary/{layer}/latest` | Latest summary | — | — |
| `groups/{gid}/summary/{layer}/{period}` | Summary for period | Write/overwrite | **deep merge** |
| `groups/{gid}/views/{name}` | Query view | — | — |

> layer = short (hourly) / mid (daily) / long (monthly); period = ISO8601 (`2026-03-20T10` / `2026-03-20` / `2026-03`)

### 3.2 system

| Path | Read | Write | Patch |
|------|------|-------|-------|
| `tasks` | List non-pending tasks | Create task | — |
| `tasks/{id}` | Task details | — | — |
| `tasks/{id}/description` | — | Update description | — |
| `anchors/{task_id}` | All anchors for task | — | — |
| `anchors/{task_id}/{anchor_id}` | Single anchor | — | — |

> State machine: `pending → active ⇄ sleep → finished`

### 3.3 sandbox

| Path | Read | Write | Patch |
|------|------|-------|-------|
| `{task_id}/...` | File content / directory listing | Write file (auto-creates parents) | Overwrite |
| `{task_id}/log` | Read log | — | **Append** (newline-delimited) |
| `__system__/proc/{name}/` | Tool code directory | — | — |
| `__system__/svc/{name}/` | Service artifact directory | — | — |

> Containers are keyed by agent_config_id (not task_id). Multiple tasks from the same agent share one container.
> URI translation: `logos://sandbox/{task_id}/...` → `/workspace/...`

### 3.4 users

| Path | Read | Write | Patch |
|------|------|-------|-------|
| `{uid}/...` | File / directory listing | Write file | **deep merge** JSON |
| `{uid}/persona/short/{period}` | Read | **Append** JSON array entry | — |
| `{uid}/persona/mid/` | Read | Overwrite | — |
| `{uid}/persona/long.md` | Read | Overwrite | — |

### 3.5 proc

| Path | Read | Write |
|------|------|-------|
| `/` | List all tool names | — |
| `{tool}` or `{tool}/.schema` | Tool schema JSON | — |
| `{tool}/{call_id}/input` | Read back params | **Submit params → trigger synchronous execution** |
| `{tool}/{call_id}/output` | Get result (**slot cleared after read**) | — |
| `{tool}/{call_id}/error` | Error / "null" | — |

### 3.6 proc-store

| Path | Read | Write |
|------|------|-------|
| `/` | List tool names | — |
| `{tool}/schema.json` | Tool declaration | **Register tool** (with `run` cmd, optional `git`) |
| `{tool}/...` | Other files | Write file |

> If `git` field present → `git clone` to `sandbox/__system__/proc/{tool}/` at boot, `git pull --ff-only` if already cloned

### 3.7 services

| Path | Read | Write | Patch |
|------|------|-------|-------|
| `/` | List service names | — | — |
| `{name}` | ServiceEntry JSON | Register/update | **deep merge** |

> ServiceEntry: name, source (builtin/agent), svc_type (oneshot/daemon), endpoint, status

### 3.8 svc-store

| Path | Read | Write |
|------|------|-------|
| `/` | List service names | — |
| `{name}/compose.yaml` | Service manifest | Write |
| `{name}/artifacts/...` | Build artifacts | Write |

### 3.9 devices

| Path | Read | Write |
|------|------|-------|
| `/` | List device names | — |
| `{name}` or `{name}/capabilities` | Capabilities JSON | — |
| `{name}/state` | `{volume, muted, dark_mode, battery_percent, battery_charging}` | — |
| `{name}/control` | — | Send command: `set_volume`/`mute`/`unmute`/`screenshot`/`dark_mode` |

### 3.10 tmp

| Path | Read | Write | Patch |
|------|------|-------|-------|
| `/` | List all keys | — | — |
| `{key/path}` | Get value | Set value | Overwrite |

## 4. Built-in Proc Tools

| Tool | Params | Function |
|------|--------|----------|
| `memory.search` | chat_id, query, limit | FTS5 full-text search |
| `memory.range_fetch` | chat_id, ranges, limit, offset | Batch message range fetch |
| `memory.view.by_speaker` | chat_id, speaker, limit | Filter by speaker |
| `memory.view.recent` | chat_id, limit | Most recent N messages |
| `system.search_tasks` | query, limit | Multi-level task + anchor search |
| `system.get_context` | chat_id, sender_uid, msg_id? | Auto-inject session + summary + persona |

## 5. Kernel Subsystems

| Subsystem | Storage | Purpose |
|-----------|---------|---------|
| **Token Registry** | In-memory (24h TTL) | One-time token → session binding; role = User / Admin; carries agent_config_id |
| **Session Clustering** | In-memory L0/L1 + LanceDB L2 | Reply-chain-based session clustering, 3-layer LRU eviction |
| **Cron Scheduler** | In-memory job table | 60s tick, cron expression matching → create pending task |
| **Consolidator** | 4 registered cron jobs | Progressive memory compression: messages → hourly → daily summaries; persona likewise |
| **Context Injector** | Stateless | Assemble session + summary + persona before agent turn |
| **VFS Middleware** | — | JsonValidator: validate JSON before writes to system/ and memory/ |

### Consolidator Cron Jobs

| Job | Frequency | Action |
|-----|-----------|--------|
| `consolidate-short-summary` | Hourly :00 | Raw messages → short summary |
| `consolidate-short-persona` | Hourly :05 | Raw messages → persona observations |
| `consolidate-mid-summary` | Daily 03:00 | Short summaries → mid summary |
| `consolidate-mid-persona` | Daily 03:30 | Short persona → rewrite mid persona |

### Session Clustering: 3-Layer LRU

| Layer | Storage | Purpose |
|-------|---------|---------|
| L0 | In-memory HashMap | Active sessions (recently replied to) |
| L1 | In-memory HashMap | Inactive (LRU-evicted from L0) |
| L2 | LanceDB | Archived (persisted on L1 eviction, page-faulted back to L0 on reply) |

> Core principle: reply chains are hard bindings; semantic similarity is only a fallback.

## 6. Access Control

| Rule | Description |
|------|-------------|
| Sandbox isolation | Owner task can read/write own sandbox; non-owner can only read finished tasks' sandboxes |
| Namespace permissions | User role cannot write to `services/`, `system/`, `devices/` |
| Exec/Call auth | Exec and Call require a valid session (no anonymous execution) |
| No session = Admin | Management interface calls (no header) on Read/Write/Patch treated as admin |
| JSON validation | Writes/patches to `system/` and `memory/` must be valid JSON |

## 7. MCP Adapter

| MCP Tool | Maps to |
|----------|---------|
| `logos_read` | gRPC Read |
| `logos_write` | gRPC Write |
| `logos_patch` | gRPC Patch |
| `logos_exec` | gRPC Exec |
| `logos_call` | gRPC Call |

## 8. Boot Sequence

```
users → memory → system → tmp → sandbox (create)
→ proc (built-in tools) → proc-store (restore external tools + git clone)
→ services (restore from svc-store) → svc-store
→ devices (macOS driver) → sandbox (mount) → table.open()
→ cron.start() + consolidator registers 4 jobs
```

## Project Structure

```
logos-fs/
├── proto/logos.proto          # gRPC service definition (5 primitives + management)
├── crates/
│   ├── logos-vfs/             # VFS core: Namespace trait, RoutingTable, URI parsing, Middleware
│   ├── logos-kernel/          # Kernel: gRPC impl, namespace mounting, Token, Cron, Consolidator, Context
│   ├── logos-mm/              # Memory module: message storage, summaries, views, FTS5
│   ├── logos-system/          # System module: task state machine, anchors, search
│   ├── logos-session/         # Session clustering: reply-chain topology, 3-layer LRU, LanceDB L2
│   └── logos-mcp/             # MCP adapter: gRPC → JSON-RPC 2.0 (5 tools)
```
