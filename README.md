# logos-fs

Logos 给 AI agent 提供了一套通过 URI 访问一切资源的内核：消息、记忆、任务、沙箱、工具、服务、设备，全部统一在 `logos://` 地址空间下，agent 只需要 5 个操作就能和整个系统交互。

```
Agent --logos_call/read/write/exec--> MCP adapter (JSON-RPC stdio)
  --> gRPC unix socket --> VFS routing table --> namespace handlers
```

> [中文文档](./README.zh.md)

## gRPC Service

Proto: `proto/logos.proto`, package `logos.kernel.v1`.

### Agent Primitives

```protobuf
rpc Read(ReadReq)   returns (ReadRes);    // uri -> content
rpc Write(WriteReq) returns (WriteRes);   // uri, content -> ()
rpc Patch(PatchReq) returns (PatchRes);   // uri, partial -> () (JSON deep merge)
rpc Exec(ExecReq)   returns (ExecRes);    // command -> stdout, stderr, exit_code
rpc Call(CallReq)   returns (CallRes);    // tool, params_json -> result_json
```

- `Read/Write/Patch` are pure data operations, no side effects.
- `Exec` runs shell in the agent's sandbox container. `logos://sandbox/` and `logos://services/` URIs in commands are auto-translated to real paths. Other namespaces are not accessible via Exec.
- `Call` dispatches to proc tools (built-in or external). This is how agents invoke `system.complete`, `memory.search`, etc.
- All five require a valid session (established via Handshake), except management RPCs.

### Management Interface (called by runtime, not agents)

```protobuf
rpc Handshake(HandshakeReq)         returns (HandshakeRes);
rpc RegisterToken(RegisterTokenReq) returns (RegisterTokenRes);
rpc RevokeToken(RevokeTokenReq)     returns (RevokeTokenRes);
```

Flow: runtime calls `RegisterToken(token, task_id, agent_config_id, role)` → launches agent with token in env → agent connects to UDS, sends `Handshake(token)` → kernel binds connection to task, returns session key in `x-logos-session` header → all subsequent calls are scoped to that task.

Roles: `user` (read-only on system/services/devices) or `admin` (full access). Connections without a session header are treated as admin (for management use).

## Namespaces

| Namespace | Backend | Persistent | What it stores |
|-----------|---------|:----------:|----------------|
| `memory` | SQLite per group + FTS5 | yes | Messages, summaries (short/mid/long), knowledge graph |
| `system` | SQLite | yes | Tasks, anchors, search index |
| `sandbox` | Host FS, containerd + overlayfs | yes | Per-task working directories, execution logs |
| `users` | Filesystem | yes | User profiles, persona (short/mid/long.md) |
| `proc` | In-memory | no | Tool registry + per-call session slots |
| `proc-store` | Filesystem + git | yes | External tool declarations and code |
| `services` | In-memory (restored from svc-store) | no | Running service registry |
| `svc-store` | Filesystem | yes | Service artifacts (compose.yaml, binaries) |
| `devices` | In-memory | no | Hardware/virtual device drivers |
| `tmp` | In-memory HashMap | no | Ephemeral KV |

## Path Reference

### memory

| Path | Read | Write | Patch |
|------|------|-------|-------|
| `groups/{gid}/messages` | — | Insert message JSON (`ts`, `speaker`, `text`, `mentions`, `reply_to`) | — |
| `groups/{gid}/messages/{id}` | Single message | — | — |
| `groups/{gid}/summary/{layer}/latest` | Latest summary | — | — |
| `groups/{gid}/summary/{layer}/{period}` | Summary for period | Write/overwrite | Deep merge |
| `groups/{gid}/graph/` | List all subjects | — | — |
| `groups/{gid}/graph/{subject}` | All triples for subject | Write/upsert triples | Delegates to write |
| `groups/{gid}/views/{name}` | Query view (params as JSON) | — | — |
| `plugins/` | List mounted plugins with docs | — | — |

`layer` = `short` (hourly) / `mid` (daily) / `long` (monthly). `period` = ISO 8601 prefix.

Plugins: `summary` and `graph` are mounted by default. Each exposes its own URI structure. Discover available plugins via `logos://memory/plugins/`.

### system

| Path | Read | Write | Patch |
|------|------|-------|-------|
| `tasks` | List all non-pending tasks | Create task (JSON with `task_id`, `description`, `chat_id`, etc.) | — |
| `tasks/{id}` | Task JSON (includes `plan_todo`, `plan_parent`) | — | Deep merge |
| `tasks/{id}/description` | — | Update description | — |
| `anchors/{task_id}` | All anchors for task | — | — |
| `anchors/{task_id}/{anchor_id}` | Single anchor | — | — |

Task states: `pending → active ⇄ sleep → finished`. Pending tasks are never visible to agents. All state transitions go through `system.complete`, not direct writes.

Task table columns: `task_id`, `description`, `workspace`, `resource`, `status`, `chat_id`, `trigger`, `plan_todo` (JSON string array), `plan_parent` (planner task ID), `created_at`, `updated_at`.

### sandbox

| Path | Read | Write |
|------|------|-------|
| `{task_id}/...` | File content or directory listing | Write file (creates parents) |
| `{task_id}/log` | Read log | Append (via `system.complete`) |
| `__system__/proc/{name}/` | Tool code | — |
| `__system__/svc/{name}/` | Service artifacts | — |

Containers are keyed by `agent_config_id`, not `task_id` — multiple tasks from the same agent share one container. URI `logos://sandbox/{task_id}/...` maps to `/workspace/...` inside the container.

### users

| Path | Read | Write | Patch |
|------|------|-------|-------|
| `{uid}/...` | File or directory listing | Write file | Deep merge JSON |
| `{uid}/persona/short/{period}` | Read | **Append** to JSON array | — |
| `{uid}/persona/mid/` | Read | Overwrite | — |
| `{uid}/persona/long.md` | Read | Overwrite | — |

`persona/short` is append-only — each write adds one entry to the array. Corrupted files are rejected (not silently cleared).

### proc

| Path | Read | Write |
|------|------|-------|
| `/` | List all tool names | — |
| `{tool}` or `{tool}/.schema` | Tool schema JSON | — |
| `{tool}/{call_id}/input` | Read back params | Submit params → trigger execution |
| `{tool}/{call_id}/output` | Get result (slot cleared after read) | — |
| `{tool}/{call_id}/error` | Error or "null" | — |

In practice, agents use `logos_call(tool, params)` which handles the call_id lifecycle internally.

### proc-store, services, svc-store, devices, tmp

See the [Path Reference tables in README.zh.md](./README.zh.md) for full details. Key points:

- **proc-store**: external tool declarations. If `git` field present, kernel clones the repo to sandbox at boot.
- **services**: runtime service registry. `ServiceEntry` = name, source, svc_type (oneshot/daemon), endpoint, status.
- **svc-store**: persistent service artifacts (compose.yaml). Restored to services registry on boot.
- **devices**: driver-based. Currently ships with a macOS system driver (volume, screenshot, dark mode).
- **tmp**: ephemeral key-value. Lost on restart.

## Built-in Proc Tools

These are registered at boot and invoked via `logos_call`.

### system.complete

The turn terminator. Every agent turn must end with this call.

```json
{
  "task_id": "task-001",
  "summary": "what happened this turn",
  "reply": "message to deliver to user (optional)",
  "anchor": true,
  "anchor_facts": "[{\"type\":\"decision\",\"topic\":\"font\",\"value\":\"宋体\"}]",
  "task_log": "execution details to append to sandbox/log",
  "sleep_reason": "awaiting_user (optional — task sleeps instead of finishing)",
  "sleep_retry": false,
  "resume": "task-042 (optional — abandon current task, activate this one)",
  "plan": {
    "todo": ["step 1 description", "step 2 description", "step 3 description"]
  }
}
```

Three mutually exclusive paths:

1. **Finish** (default): task → `finished`. If `anchor: true`, creates an anchor checkpoint.
2. **Sleep**: if `sleep_reason` is set, task → `sleep`.
3. **Resume**: if `resume` is set, deletes current task, activates target task.

**Plan mode**: if `plan.todo` is non-empty, task → `sleep` with the todo list stored. The scheduler picks up the first item, creates an executor task (`trigger: "plan"`, `plan_parent: "task-001"`), and waits for it to complete. When it does, the scheduler creates the next one. When all items are done (or the planner is woken up), the planner agent can submit a new todo, revise the plan, or finish.

After execution, `task_log` is written to `logos://sandbox/{task_id}/log`.

### memory.search

```json
{ "chat_id": "group-1", "query": "search terms", "limit": 10 }
```

FTS5 full-text search over messages. Max 50 results.

### memory.range_fetch

```json
{ "chat_id": "group-1", "ranges": [[100, 200], [300, 350]], "limit": 50, "offset": 0 }
```

Batch fetch messages by ID ranges. Max 200 per call.

### system.search_tasks

```json
{ "query": "error snippet or keyword", "limit": 10 }
```

Two-level search: L1 = BM25 over anchor facts, L2 = keyword match on task descriptions. Returns `{ "l1_hits": [...], "l2_hits": [...] }`.

### system.get_context

```json
{ "chat_id": "group-1", "sender_uid": "user-123", "msg_id": 456 }
```

Assembles turn context: current session, latest short summary, sender persona paths. Intended to be called by runtime before the agent's first token.

## Session Clustering

Three-layer LRU with topology-first design:

| Layer | Storage | Trigger |
|-------|---------|---------|
| L0 | In-memory | Active sessions |
| L1 | In-memory | LRU-evicted from L0 |
| L2 | LanceDB (vector DB) | Evicted from L1, persisted with embeddings |

Message routing:
1. Has `reply_to`? → follow reply chain (hard binding). If target session is in L2, page-fault it back to L0.
2. No reply chain? → semantic search in L2 via Ollama embeddings (soft binding, L2 distance < 0.5).
3. No match? → create new session.

Embeddings: Ollama `qwen3-embedding:0.6b`, 1024-dim. Configurable via `OLLAMA_URL`, `EMBED_MODEL`, `EMBED_DIM` env vars.

If LanceDB init fails, kernel falls back to in-memory only (L0+L1) and prints a WARNING. Sessions will not be persisted across restarts in this mode.

## Cron & Consolidator

Scheduler ticks every 60s. Five consolidator jobs registered at boot:

| Job | Schedule | Output |
|-----|----------|--------|
| `consolidate-short-summary` | Hourly :00 | Messages → short summary |
| `consolidate-short-persona` | Hourly :05 | Messages → persona observations |
| `consolidate-graph` | Hourly :10 | Messages → knowledge graph triples |
| `consolidate-mid-summary` | Daily 03:00 | Short summaries → mid summary |
| `consolidate-mid-persona` | Daily 03:30 | Short persona → rewrite mid persona |

The scheduler also handles **plan dispatch**: scans for sleeping tasks with non-empty `plan_todo`, pops the head item, creates an executor task. One executor per planner at a time.

## Access Control

- **Sandbox isolation**: task owner can read/write own sandbox. Others can only read finished tasks' sandboxes.
- **Namespace permissions**: `user` role is read-only on `services/`, `system/`, `devices/`.
- **Exec/Call**: require a valid session.
- **No session = admin**: management calls without `x-logos-session` header are treated as admin.
- **JSON validation**: middleware validates JSON on writes/patches to `system/` and `memory/`.

## Boot Sequence

```
1. users/
2. memory/ (+ session store with optional LanceDB L2)
3. system/ (SQLite)
4. tmp/
5. sandbox/ (containerd + overlayfs)
6. proc/ (register built-in tools: memory.search, memory.range_fetch,
          system.search_tasks, system.get_context, system.complete)
7. proc-store/ (restore external tools, git clone/pull)
8. services/ (restore from svc-store)
9. svc-store/
10. devices/
11. sandbox/ (final mount)
→ table.open() — kernel accepts connections
→ cron.start() + 5 consolidator jobs registered
```

## Project Structure

```
logos-fs/
├── proto/logos.proto            # gRPC service (5 primitives + management)
├── crates/
│   ├── logos-vfs/               # Namespace trait, RoutingTable, URI parsing, middleware
│   ├── logos-kernel/            # gRPC server, namespace wiring, token, cron, consolidator,
│   │                            # builtin tools, sandbox (containerd), context injection
│   ├── logos-mm/                # Memory: messages, summaries, graph, FTS5, view plugins
│   ├── logos-system/            # Tasks, anchors, search, logos_complete logic, plan storage
│   ├── logos-session/           # Session clustering: reply-chain topology, 3-layer LRU,
│   │                            # LanceDB L2 with Ollama embeddings
│   └── logos-mcp/               # MCP adapter: stdio JSON-RPC ↔ gRPC UDS
├── test.Dockerfile              # Debian slim build + test environment
└── data/state/                  # Runtime data (SQLite DBs, sandbox root)
```

## Environment Variables

| Var | Default | Purpose |
|-----|---------|---------|
| `VFS_USERS_ROOT` | `../../data/state/entities` | Users namespace root |
| `VFS_MEMORY_ROOT` | `../../data/state/memory` | Memory DB root |
| `VFS_SYSTEM_DB` | `../../data/state/system.db` | System SQLite path |
| `VFS_SANDBOX_ROOT` | `../../data/state/sandbox` | Sandbox root |
| `VFS_PROC_STORE_ROOT` | `../../data/state/proc-store` | Proc-store root |
| `VFS_SVC_STORE_ROOT` | `../../data/state/svc-store` | Svc-store root |
| `VFS_LISTEN` | `unix://{sandbox_root}/logos.sock` | gRPC listen address |
| `OLLAMA_URL` | `http://localhost:11434` | Ollama API for embeddings |
| `EMBED_MODEL` | `qwen3-embedding:0.6b` | Embedding model name |
| `EMBED_DIM` | `1024` | Embedding dimension |
| `LOGOS_SOCKET` | `../../data/state/sandbox/logos.sock` | MCP adapter socket path |
| `LOGOS_TOKEN` | (empty) | MCP adapter handshake token |
