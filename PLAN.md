The engineering answer is: **build a dedicated orchestration run/event bus with a blocking MCP wait primitive.** Do **not** make `assign` smarter, do **not** use the inbox as the primary callback path, and do **not** start with a monolithic “batch handoff owns everything” tool.

The best framing is:

**Approach 2 as the architecture. Approach 3 as the rollout/package.**

Meaning: orchestration gets its own durable run/job/event system, while existing inbox, `assign`, `handoff`, and `send_message` semantics stay intact. The attached memos are strongly aligned on that point, even when they label the preferred approach slightly differently. One memo calls this “event-yielding orchestration session,” another calls it “hybrid event-channel architecture,” and the third says the hybrid framing is mostly a backwards-compatibility label for the same run/event-bus architecture.   

## Final recommendation

Build a new CAO orchestration layer:

```text
tmux workers
  → structured worker callback / terminal marker parser
  → durable orchestration_events table
  → blocking wait_for_event MCP tool
  → supervisor model
  → spawn follow-up jobs
  → repeat until supervisor finalizes run
```

The core unlock is not “async handoff.” It is **tracked orchestration runs with blocking event joins**.

The supervisor should be able to do this inside one active Codex turn:

```text
start run
spawn 4 developer jobs
wait_for_event blocks through MCP
receive first developer completion
spawn reviewer immediately
wait_for_event again
receive reviewer rejection
spawn developer fix
wait_for_event again
...
finalize when all branches meet supervisor-owned completion criteria
```

That gives CAO the missing primitive: **handoff-style blocking continuity plus multi-worker fan-out plus incremental event handling.**

## What the research agrees on

The memos have a clear consensus:

| Topic                  | Consensus                                                                                                                                     |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `assign`               | Not enough. It is fire-and-forget and tied to inbox delivery / supervisor idle semantics.                                                     |
| Inbox extension        | Wrong abstraction for primary orchestration. You would end up building an event bus inside the inbox anyway.                                  |
| Event bus              | Correct foundation. Needs run/job/event correlation, durable replay, blocking wait, and observability.                                        |
| Blocking MCP wait      | Required for the Codex continuity constraint. Waiting must happen through a tool call, not by ending the turn and waiting for inbox delivery. |
| Terminal polling       | Acceptable for a spike or callback detection, not acceptable as the durable state model.                                                      |
| Monolithic batch tool  | Useful later or as fallback, but too opaque and too rigid as the first-class primitive.                                                       |
| Worker callbacks       | Need structured completion signals with `run_id`, `job_id`, and preferably `attempt_id`.                                                      |
| Persistence            | SQLite is enough for local CAO Phase 1. Postgres is unnecessary unless CAO becomes multi-host.                                                |
| Backward compatibility | Existing `handoff`, `assign`, and `send_message` should not change during the initial rollout.                                                |

That is the branch direction.

## Where the research splits, and the decision

| Split                                                                                   | Decision                                                                                                                                                                                                                                                                                                                                                        |
| --------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Approach 2 vs Approach 3**                                                            | Treat Approach 2 as the architecture and Approach 3 as the migration story. The inbox remains for human/direct agent messaging; orchestration callbacks go to a separate event bus.                                                                                                                                                                             |
| **`start_orchestration` / `spawn_job` names vs `handoff_async` / `handoff_wait` names** | Use explicit orchestration names internally and expose simple MCP names. I’d avoid `handoff_async` because it sounds like old `assign`. Recommended names: `orchestration_start`, `orchestration_spawn`, `orchestration_wait`, `orchestration_cancel`, `orchestration_status`, `orchestration_finalize`.                                                        |
| **One event per wait vs event batch**                                                   | Return a small event batch. Default `min_events=1`, `max_events=10`, with short debounce. This preserves immediate reaction while avoiding one tool call per simultaneous completion burst.                                                                                                                                                                     |
| **Global consumed events vs cursor**                                                    | Use append-only events plus cursors. Do not mutate event rows with global `consumed_at` as the source of truth. Multiple subscribers, replay, inspection, and resume all get cleaner with cursors.                                                                                                                                                              |
| **Engine-owned `RUN_COMPLETED` when open jobs hit zero**                                | Don’t do that in model-driven mode. Zero active jobs means `run.idle` / `drained`, not necessarily complete. The supervisor may still need to spawn reviewers or fixes after seeing the last completion. Final completion should be supervisor-owned via `orchestration_finalize`, unless using a server-driven batch workflow with explicit completion policy. |
| **Completion parser vs worker MCP callback**                                            | Start with a structured terminal marker parser because it reuses the tmux substrate. Add worker-side MCP callback tools later for reliability.                                                                                                                                                                                                                  |
| **Retries inside engine vs supervisor-driven retries**                                  | Supervisor-driven in MVP. Engine tracks attempts and lineage; supervisor decides whether to retry, abandon, escalate, or spawn a specialist.                                                                                                                                                                                                                    |
| **Monolithic `handoff_batch`**                                                          | Build later as a wrapper over the same engine. Also keep it as the fallback if Phase 0 proves Codex cannot maintain loop continuity across repeated blocking MCP calls inside one turn.                                                                                                                                                                         |

The most important design correction I’d make to the research: **do not let the engine auto-finalize dynamic runs just because no jobs are active.** In a dynamic DAG, “no jobs active right now” is a transient state between “developer completed” and “supervisor spawned reviewer.” Auto-finalizing there creates a race.

## Non-negotiable Codex implications

Codex’s MCP docs confirm that MCP server configuration supports `tool_timeout_sec`, and the default per-tool timeout is documented as 60 seconds. CAO should therefore not rely on indefinite blocking calls. Ship a recommended Codex config snippet with the branch and keep `orchestration_wait` as bounded long-polling. ([OpenAI Developers][1])

MCP progress notifications are optional and notification-based, so they should be treated as UI/diagnostic signals only, not as model-visible orchestration events. The supervisor should reason from normal tool results returned by `orchestration_wait`. ([Model Context Protocol][2])

MCP cancellation is request-scoped: it cancels an in-progress MCP request. CAO still needs domain-level cancellation that kills tmux workers, marks jobs, emits events, and cleans up terminals. ([Model Context Protocol][3])

Recommended Codex config to ship in docs/tests:

```toml
[mcp_servers.cao]
command = "node"
args = ["dist/cao-mcp-server.js"]
startup_timeout_sec = 20
tool_timeout_sec = 3600
```

But even with that config, `orchestration_wait` should not block for an hour by default. Use bounded waits and heartbeats.

Recommended default:

```text
wait_timeout_sec = 45
max_events = 10
min_events = 1
```

Then allow higher wait windows only when the operator has explicitly configured Codex’s MCP timeout.

## The MCP tool surface

Start with six tools.

### 1. `orchestration_start`

Creates a run and optionally initial jobs.

```ts
orchestration_start({
  name?: string,
  jobs?: JobSpec[],
  policy?: {
    max_active_workers_per_run?: number,
    max_active_reviewers_per_run?: number,
    max_total_terminals?: number,
    default_job_timeout_sec?: number,
    run_timeout_sec?: number,
    fail_fast?: boolean,
    debug_retention?: boolean
  },
  metadata?: object
}) -> {
  run_id: string,
  job_ids: string[],
  status: "running"
}
```

This can be sugar over creating a run plus calling `orchestration_spawn` for each job. Keep it because it makes the first fan-out clean.

### 2. `orchestration_spawn`

Creates one tracked worker job.

```ts
orchestration_spawn({
  run_id: string,
  agent_profile: string,
  message: string,

  parent_job_id?: string,
  chain_id?: string,
  role?: "developer" | "reviewer" | "specialist" | string,
  kind?: "implementation" | "review" | "fix" | "analysis" | string,

  timeout_sec?: number,
  idempotency_key?: string,
  metadata?: object
}) -> {
  run_id: string,
  job_id: string,
  attempt_id: string,
  status: "queued" | "running",
  terminal_id?: string
}
```

This is non-blocking. It writes job state, emits `job.created` / `job.queued`, and wakes the scheduler.

### 3. `orchestration_wait`

The critical Codex-compatible primitive.

```ts
orchestration_wait({
  run_id: string,
  cursor?: number,
  event_types?: string[],
  job_ids?: string[],

  min_events?: number,        // default 1
  max_events?: number,        // default 10
  wait_timeout_sec?: number,  // default 45
  include_snapshot?: boolean  // default true
}) -> {
  run_id: string,
  cursor: number,
  events: OrchestrationEvent[],
  timeout: boolean,
  run_status: "running" | "idle" | "finalized" | "cancelled" | "timed_out" | "failed",
  snapshot?: RunSnapshot
}
```

Behavior:

```text
1. Read events after cursor.
2. If matching events exist, return immediately.
3. Otherwise block until:
   - event arrives,
   - min_events is satisfied,
   - wait_timeout_sec elapses,
   - run enters terminal status,
   - MCP request is cancelled.
4. If no event arrives before timeout, return timeout=true with current snapshot.
5. Do not mutate event rows as consumed.
```

The supervisor immediately calls `orchestration_wait` again after heartbeat/timeouts unless it has work to dispatch.

### 4. `orchestration_status`

Non-blocking replay and debugging.

```ts
orchestration_status({
  run_id: string,
  include_jobs?: boolean,
  include_events_since?: number,
  include_terminal_refs?: boolean
}) -> {
  run: RunRecord,
  jobs?: JobRecord[],
  events?: OrchestrationEvent[],
  snapshot: RunSnapshot
}
```

This is essential for recovery and debugging. Do not punt it too far.

### 5. `orchestration_cancel`

Domain-level cancellation.

```ts
orchestration_cancel({
  run_id: string,
  scope: "run" | "job" | "attempt" | "subtree",
  job_id?: string,
  attempt_id?: string,
  reason?: string
}) -> {
  cancelled_jobs: string[],
  cancelled_attempts: string[],
  events: OrchestrationEvent[]
}
```

Phase 0 needs `run` and `job`. Phase 1 adds `attempt` and `subtree`.

### 6. `orchestration_finalize`

Explicitly closes a run.

```ts
orchestration_finalize({
  run_id: string,
  outcome: "succeeded" | "failed" | "cancelled",
  summary?: string,
  cleanup_policy?: {
    successful_terminals: "kill" | "preserve_until_ttl",
    failed_terminals: "preserve" | "kill",
    ttl_sec?: number
  }
}) -> {
  run_id: string,
  status: "finalized",
  summary: RunSummary,
  cleanup_status: "ok" | "partial"
}
```

Do not make this implicit in MVP. Explicit finalization keeps dynamic DAGs sane.

## Event model

Use one common envelope.

```ts
type OrchestrationEvent = {
  event_id: number,       // global monotonic
  seq: number,            // per-run monotonic
  run_id: string,

  job_id?: string,
  attempt_id?: string,
  parent_job_id?: string,
  chain_id?: string,

  type:
    | "run.created"
    | "run.started"
    | "run.idle"
    | "run.finalized"
    | "run.cancelled"
    | "run.timed_out"

    | "job.created"
    | "job.queued"
    | "job.started"
    | "job.progress"
    | "job.completed"
    | "job.failed"
    | "job.timed_out"
    | "job.cancelled"

    | "review.approved"
    | "review.rejected"

    | "terminal.created"
    | "terminal.exited"
    | "terminal.cleaned"
    | "terminal.orphan_detected"
    | "terminal.cleanup_failed";

  status?: string;

  actor?: {
    kind: "supervisor" | "worker" | "engine" | "terminal_monitor";
    id?: string;
  };

  correlation?: {
    idempotency_key?: string;
    role?: string;
    kind?: string;
    module?: string;
  };

  payload: object;
  visible_to_model: boolean;
  created_at: string;
}
```

Key rule: **separate job lifecycle from semantic outcome.**

A reviewer worker can complete successfully while semantically rejecting the work. That should not be represented as `job.failed`.

Recommended reviewer event pattern:

```text
job.completed
  payload.result.decision = "rejected"

review.rejected
  payload.findings = [...]
  payload.fix_prompt = "..."
```

The engine can emit both from one callback if the reviewer output is structured. The supervisor can filter on `review.rejected` if it wants semantic events, or inspect `job.completed` directly.

## Worker callback protocol

Phase 0 should use a terminal marker parser. Do not use raw JSON lines; terminal wrapping and accidental text collisions will bite you.

Use a sentinel plus base64url JSON:

```text
⟦CAO-EVENT-v1:<base64url-json>⟧
```

Decoded payload:

```json
{
  "version": 1,
  "run_id": "run_01J...",
  "job_id": "job_01J...",
  "attempt_id": "att_01J...",
  "type": "job.completed",
  "status": "succeeded",
  "result": {
    "summary": "Implemented auth changes",
    "artifacts": ["src/auth/session.ts"],
    "notes": "No migrations required"
  },
  "nonce": "evt_..."
}
```

Reviewer rejection:

```json
{
  "version": 1,
  "run_id": "run_01J...",
  "job_id": "job_01K...",
  "attempt_id": "att_01K...",
  "type": "review.rejected",
  "status": "succeeded",
  "result": {
    "decision": "rejected",
    "findings": [
      {
        "severity": "blocker",
        "message": "Session expiry not tested"
      }
    ],
    "fix_prompt": "Add tests for expired sessions and refresh-token invalidation."
  },
  "nonce": "evt_..."
}
```

Every spawned worker prompt must include:

```text
You are part of CAO orchestration run {{run_id}}.
Your job_id is {{job_id}}.
Your attempt_id is {{attempt_id}}.
Your chain_id is {{chain_id}}.

When finished, emit exactly one CAO completion marker using the required format.
Do not mark complete until the work is actually done.
If blocked or unable to complete, emit a failure marker with failure_type and explanation.
```

Phase 1 or 2 should add worker-side MCP tools:

```text
job_emit_event(...)
job_complete(...)
job_fail(...)
```

But don’t require those for the first branch. The marker parser lets the team reuse existing tmux worker infrastructure.

## State model

Use SQLite with WAL for the local CAO implementation. Add Postgres only if CAO needs multi-host orchestration.

Minimum tables for a real MVP:

```text
orchestration_runs
orchestration_jobs
orchestration_attempts
orchestration_events
worker_terminals
orchestration_subscriptions
```

### `orchestration_runs`

```sql
CREATE TABLE orchestration_runs (
  run_id TEXT PRIMARY KEY,
  name TEXT,
  supervisor_id TEXT,
  supervisor_session_id TEXT,

  status TEXT NOT NULL CHECK (
    status IN ('created','running','idle','finalized','failed','cancelled','timed_out')
  ),

  policy_json TEXT NOT NULL,
  metadata_json TEXT,

  created_at TEXT NOT NULL,
  started_at TEXT,
  updated_at TEXT NOT NULL,
  finalized_at TEXT,

  deadline_at TEXT,
  fail_fast INTEGER NOT NULL DEFAULT 0,

  lease_owner TEXT,
  lease_expires_at TEXT
);
```

### `orchestration_jobs`

Logical unit of work. Same `job_id` across retries.

```sql
CREATE TABLE orchestration_jobs (
  job_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES orchestration_runs(run_id),
  parent_job_id TEXT REFERENCES orchestration_jobs(job_id),

  chain_id TEXT,
  agent_profile TEXT NOT NULL,
  role TEXT,
  kind TEXT,

  status TEXT NOT NULL CHECK (
    status IN ('created','queued','spawning','running','succeeded','failed','timed_out','cancelled')
  ),

  message TEXT NOT NULL,
  message_hash TEXT,
  idempotency_key TEXT,

  timeout_sec INTEGER,
  deadline_at TEXT,

  attempt_count INTEGER NOT NULL DEFAULT 0,
  current_attempt_id TEXT,

  result_ref TEXT,
  error_json TEXT,

  created_at TEXT NOT NULL,
  started_at TEXT,
  completed_at TEXT,
  metadata_json TEXT,

  UNIQUE(run_id, idempotency_key)
);
```

### `orchestration_attempts`

Actual terminal execution. New `attempt_id` per retry.

```sql
CREATE TABLE orchestration_attempts (
  attempt_id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES orchestration_jobs(job_id),
  run_id TEXT NOT NULL REFERENCES orchestration_runs(run_id),

  attempt_no INTEGER NOT NULL,
  terminal_id TEXT,

  status TEXT NOT NULL CHECK (
    status IN ('starting','running','succeeded','failed','timed_out','cancelled')
  ),

  started_at TEXT,
  completed_at TEXT,

  failure_type TEXT,
  failure_message TEXT,
  output_ref TEXT,
  metadata_json TEXT
);
```

### `orchestration_events`

Append-only event log. This is the source of truth for replay.

```sql
CREATE TABLE orchestration_events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL REFERENCES orchestration_runs(run_id),
  seq INTEGER NOT NULL,

  job_id TEXT REFERENCES orchestration_jobs(job_id),
  attempt_id TEXT REFERENCES orchestration_attempts(attempt_id),
  parent_job_id TEXT,
  chain_id TEXT,

  type TEXT NOT NULL,
  status TEXT,

  actor_kind TEXT,
  actor_id TEXT,

  payload_json TEXT NOT NULL,
  visible_to_model INTEGER NOT NULL DEFAULT 1,

  dedupe_key TEXT,
  created_at TEXT NOT NULL,

  UNIQUE(run_id, seq),
  UNIQUE(run_id, dedupe_key)
);

CREATE INDEX idx_events_run_seq
  ON orchestration_events(run_id, seq);

CREATE INDEX idx_events_run_type
  ON orchestration_events(run_id, type);
```

### `worker_terminals`

```sql
CREATE TABLE worker_terminals (
  terminal_id TEXT PRIMARY KEY,

  run_id TEXT,
  job_id TEXT,
  attempt_id TEXT,

  tmux_session TEXT,
  tmux_window TEXT,
  tmux_pane TEXT,

  provider TEXT,

  status TEXT NOT NULL CHECK (
    status IN ('created','running','exited','killed','orphaned','cleaned')
  ),

  pid INTEGER,

  created_at TEXT NOT NULL,
  last_observed_at TEXT,
  exited_at TEXT,

  cleanup_status TEXT,
  retention_policy TEXT,
  metadata_json TEXT
);
```

### `orchestration_subscriptions`

This gives easy server-side cursor handling while still allowing explicit cursors for recovery.

```sql
CREATE TABLE orchestration_subscriptions (
  subscription_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES orchestration_runs(run_id),
  caller_id TEXT NOT NULL,

  cursor_event_id INTEGER NOT NULL DEFAULT 0,

  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,

  UNIQUE(run_id, caller_id)
);
```

Do not store giant outputs inline in events. Inline up to a cap, say 64KB, then spill to file/object reference:

```json
{
  "output_ref": "cao://runs/run_123/jobs/job_456/output.txt",
  "output_truncated": true,
  "inline_preview": "First 4000 chars..."
}
```

## Execution model

The MCP server hosts:

```text
1. SQLite repository
2. in-process pub/sub keyed by run_id
3. worker scheduler
4. terminal lifecycle monitor
5. timeout/reaper loop
6. MCP tool handlers
```

Worker lifecycle:

```python
def run_worker_attempt(job):
    attempt = create_attempt(job)
    terminal = spawn_tmux(job, attempt)

    emit("terminal.created", job, attempt, terminal)
    emit("job.started", job, attempt)

    try:
        event = wait_for_completion_marker_or_terminal_exit(
            terminal=terminal,
            timeout=job.timeout_sec
        )

        if event.marker_received:
            ingest_worker_event(event.marker)
            mark_attempt_succeeded(attempt)
            mark_job_succeeded(job)

        elif event.timeout:
            interrupt_terminal(terminal)
            wait_grace_period()
            kill_if_still_alive(terminal)
            preserve_output(terminal)
            mark_attempt_timed_out(attempt)
            mark_job_timed_out(job)
            emit("job.timed_out", job, attempt)

        elif event.terminal_exited_without_marker:
            preserve_output(terminal)
            mark_attempt_failed(attempt, "terminal_exited_without_marker")
            mark_job_failed(job)
            emit("job.failed", job, attempt)

    finally:
        cleanup_successful_terminal_or_apply_retention(terminal)
        maybe_emit_run_idle(job.run_id)
```

Wait implementation:

```python
def orchestration_wait(run_id, cursor=None, event_types=None,
                       min_events=1, max_events=10,
                       wait_timeout_sec=45,
                       caller_id=None):

    cursor = cursor or subscription_cursor(run_id, caller_id)
    deadline = now() + wait_timeout_sec

    while now() < deadline:
        reconcile_timeouts(run_id)

        events = read_events_after(
            run_id=run_id,
            cursor=cursor,
            event_types=event_types,
            limit=max_events
        )

        if len(events) >= min_events:
            new_cursor = events[-1].event_id
            update_subscription_cursor(run_id, caller_id, new_cursor)
            return response(events, new_cursor, timeout=False)

        if run_is_terminal(run_id):
            return response([], latest_event_id(run_id), timeout=False)

        wait_on_pubsub_or_poll(run_id, max_sleep_ms=500)

    return response([], cursor, timeout=True)
```

Supervisor loop:

```python
run = orchestration_start({
  "jobs": [
    {"agent_profile": "developer", "message": auth_msg, "chain_id": "auth", "role": "developer"},
    {"agent_profile": "developer", "message": billing_msg, "chain_id": "billing", "role": "developer"},
    {"agent_profile": "developer", "message": dashboard_msg, "chain_id": "dashboard", "role": "developer"},
    {"agent_profile": "developer", "message": notifications_msg, "chain_id": "notifications", "role": "developer"}
  ]
})

cursor = None
approved = set()
failed = set()

while len(approved) + len(failed) < 4:
    result = orchestration_wait(run_id=run.run_id, cursor=cursor)
    cursor = result.cursor

    if result.timeout:
        continue

    for event in result.events:
        if event.type == "job.completed" and event.correlation.role == "developer":
            orchestration_spawn({
                "run_id": run.run_id,
                "agent_profile": "reviewer",
                "message": build_review_prompt(event),
                "parent_job_id": event.job_id,
                "chain_id": event.chain_id,
                "role": "reviewer",
                "kind": "review"
            })

        elif event.type == "review.approved":
            approved.add(event.chain_id)

        elif event.type == "review.rejected":
            orchestration_spawn({
                "run_id": run.run_id,
                "agent_profile": "developer",
                "message": event.payload.result.fix_prompt,
                "parent_job_id": event.job_id,
                "chain_id": event.chain_id,
                "role": "developer",
                "kind": "fix"
            })

        elif event.type in ["job.failed", "job.timed_out"]:
            decide_retry_or_fail_chain(event)

orchestration_finalize({
  "run_id": run.run_id,
  "outcome": "succeeded" if not failed else "failed",
  "summary": build_summary()
})
```

## Scheduling and concurrency

Start conservative. Local tmux + Codex + multiple model workers can degrade fast.

Recommended defaults:

```json
{
  "max_active_workers_per_run": 4,
  "max_active_reviewers_per_run": 2,
  "max_total_terminals": 8,
  "spawn_parallelism": 1,
  "spawn_rate_limit_per_sec": 0.5,
  "event_poll_interval_ms": 500,
  "job_default_timeout_sec": 900,
  "run_default_timeout_sec": 3600
}
```

Make these configurable by provider/profile:

```yaml
providers:
  codex_cli:
    max_active_workers: 4
    max_active_reviewers: 2
    spawn_parallelism: 1

  claude_code:
    max_active_workers: 6
    max_active_reviewers: 3
    spawn_parallelism: 2
```

Scheduler rules:

```text
1. Prioritize callback ingestion over new spawns.
2. Never exceed global terminal cap.
3. Never exceed per-run cap.
4. Apply role caps, especially reviewers.
5. Queue jobs rather than rejecting them when caps are full.
6. Emit job.started only when the terminal actually starts.
7. Use idempotency keys to prevent duplicate spawns when supervisor retries a tool call.
```

## Timeout semantics

Keep these separate:

| Timeout                      | Meaning                                                         | Action                                                                                                       |
| ---------------------------- | --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `orchestration_wait` timeout | No events became available during the blocking MCP wait window. | Return `timeout=true`; supervisor usually calls wait again. Does not affect jobs.                            |
| Job timeout                  | One job attempt exceeded its wall-clock limit.                  | Interrupt, grace wait, kill if needed, preserve output, mark attempt/job timed out, emit `job.timed_out`.    |
| Run timeout                  | Entire orchestration exceeded run deadline.                     | Cancel active jobs, emit `run.timed_out`, preserve logs.                                                     |
| MCP tool timeout             | Codex/client kills the tool call.                               | Run continues server-side; supervisor can resume with `orchestration_status` + `orchestration_wait(cursor)`. |

MCP’s own lifecycle guidance says implementations should use request timeouts to avoid hung connections, and may reset clocks on progress but should still enforce maximum timeouts. That supports bounded long-polling over indefinite blocking. ([Model Context Protocol][4])

## Cancellation semantics

Implement domain cancellation independently of MCP request cancellation.

Scopes:

```text
run       cancel all active/queued jobs in a run
job       cancel one logical job and current attempt
attempt   cancel only current terminal attempt
subtree   cancel a job and descendants
```

Flow:

```text
1. Mark cancellation requested.
2. Emit cancellation_requested event.
3. Send Ctrl-C/provider interrupt to terminal.
4. Wait grace period.
5. Kill tmux pane/session if still alive.
6. Mark job/attempt cancelled.
7. Emit job.cancelled / run.cancelled.
8. Apply retention policy.
```

Phase 0 only needs run and job cancellation. Add subtree in Phase 1.

## Crash recovery and orphan prevention

Every tmux session created by orchestration must be named/tagged:

```text
cao:{run_id}:{job_id}:{attempt_id}
```

On CAO MCP server startup:

```python
def reconcile_on_startup():
    db_running = load_nonterminal_runs_and_jobs()
    tmux_sessions = discover_cao_tmux_sessions()

    for tmux in tmux_sessions:
        if not known_terminal(tmux):
            mark_orphan_terminal(tmux)
            preserve_or_cleanup(tmux)

    for attempt in running_attempts():
        if attempt.terminal_id not in tmux_sessions:
            mark_attempt_failed("terminal_missing")
            emit("job.failed")
        elif attempt.deadline_at < now():
            timeout_attempt(attempt)
        else:
            reattach_terminal_monitor(attempt)
```

A reaper loop should run every 60 seconds:

```text
- kill terminals for finalized runs after retention expires
- preserve failed/timed-out terminals according to policy
- mark unknown CAO-tagged terminals as orphaned
- emit cleanup events
```

This is non-optional. Without it, the first crashed supervisor leaves tmux garbage everywhere.

## Compatibility plan

Do **not** refactor existing `handoff` in Phase 0.

Phase 0 should add the new tools side-by-side. Existing flows keep using old `handoff`, `assign`, and `send_message`.

After the new engine passes the starter scenario and regression tests, optionally reimplement `handoff` internally as:

```text
orchestration_start
→ orchestration_spawn(one job)
→ orchestration_wait(until that job terminal event)
→ orchestration_finalize
→ return old handoff-compatible shape
```

But only after exact return-shape and cleanup parity tests pass. There is no upside to destabilizing `handoff` while proving the new branch.

`assign` should remain untouched. It is still useful for fire-and-forget delegation where in-call continuity is not required.

## What to keep, cut, and postpone

### Keep

```text
Dedicated orchestration event bus
Blocking MCP wait primitive
Durable SQLite event log
run_id / job_id / attempt_id correlation
Explicit parent_job_id lineage
Worker marker parser for Phase 0
MCP worker callback tools later
Append-only events + cursors
Structured reviewer decisions
Job-vs-attempt separation
Run/job cancellation
Run-ID-tagged tmux sessions
Startup reconciler and reaper
Supervisor prompt template
Codex MCP timeout config snippet
```

### Cut

```text
Making assign smarter
Using inbox as the primary event transport
Global consumed_at as event delivery source of truth
Auto-finalizing dynamic runs when open_jobs == 0
Raw JSON terminal completion lines
Relying on MCP progress notifications for model reasoning
Engine-hidden retries in MVP
Starting with handoff_batch as the only primitive
Refactoring existing handoff before the new engine is proven
```

### Postpone

```text
handoff_batch / declarative workflow DSL
Subtree cancellation
Multi-supervisor handoff
Postgres
Nested CAO runs
Rich progress streaming
UI timeline rendering
Worker-side MCP completion tools
Automatic semantic validators beyond reviewer approve/reject
```

## Phase plan

### Phase 0 — spike / branch proof

Goal: prove the core loop works under Codex.

Build:

```text
orchestration_start
orchestration_spawn
orchestration_wait
orchestration_status
orchestration_cancel(run/job)
orchestration_finalize

SQLite tables
in-process pub/sub
structured terminal marker parser
basic scheduler
basic job timeout
basic tmux liveness watchdog
run/job/attempt IDs injected into worker prompts
canonical supervisor prompt
Codex config snippet
```

Do **not** build:

```text
handoff_batch
workflow DSL
subtree cancellation
full run resume UX
payload spillover unless immediately needed
existing handoff refactor
Postgres
```

Phase 0 gate:

```text
Codex supervisor completes the 4-developer → 4-reviewer → rejection/fix loop scenario.
At least 3 workers run concurrently.
Supervisor waits via blocking MCP calls, not inbox delivery.
Reviewer jobs dispatch before unrelated chains finish.
One reviewer rejection triggers a developer fix.
Run finalizes deterministically.
No orphan terminals remain after normal completion.
```

Also test the key Codex uncertainty:

```text
Can Codex stay in one active turn across this sequence?

orchestration_wait
→ event returned
→ supervisor reasons
→ orchestration_spawn
→ orchestration_wait
→ event returned
→ orchestration_spawn
→ orchestration_wait
...
```

If yes, continue with composable model-driven orchestration.

If no, build `handoff_batch` earlier as the fallback, accepting that server-side workflow policy will make fewer LLM judgment calls mid-run.

### Phase 1 — usable prototype

Add:

```text
attempt retries as data model, still supervisor-decided
subtree cancellation
run wall-clock timeout
startup reconciliation
orphan terminal cleanup
payload spillover above 64KB
dedupe keys for worker callbacks
structured output schema validation
provider/profile concurrency config
cao runs list
cao runs inspect <run_id>
cao runs tail <run_id>
cao jobs inspect <job_id>
metrics: spawn latency, event latency, wait latency, job duration
```

Acceptance gate:

```text
Starter scenario passes 10 consecutive times.
handoff / assign regression suite passes unchanged.
No orphan terminals after chaos tests.
CAO restart can reattach monitors or fail jobs deterministically.
Duplicate completion markers produce one event.
Malformed marker fails attempt and preserves output.
```

### Phase 2 — ergonomic wrappers and advanced orchestration

Add:

```text
handoff_batch
declarative workflow rules
server-driven reviewer/fix loop controller
MCP progress notifications for UI only
worker-side MCP callback tools
resume_run helper
semantic event validators
multi-supervisor handoff if actually needed
```

`handoff_batch` should be a wrapper over the same engine, not a separate implementation.

Example:

```ts
handoff_batch({
  name: "feature-123 module implementation",
  initial_jobs: [...],
  workflow: {
    on_developer_success: "spawn_reviewer",
    on_review_rejected: "spawn_developer_fix",
    on_review_approved: "mark_chain_approved",
    max_cycles_per_chain: 3
  },
  completion: {
    mode: "all_chains_approved",
    global_timeout_sec: 3600
  }
})
```

Use it for workflows where decisions are pre-specifiable. Do not make it the default path for open-ended LLM-supervised work.

## Validation suite

Minimum tests before merging the branch:

```text
1. Single-job degenerate case
   - spawn one worker
   - wait
   - finalize
   - verify cleanup

2. Happy-path fan-out
   - spawn N developers
   - all complete
   - verify all events ordered and correlated

3. Starter scenario
   - 4 developers
   - 4 reviewers
   - one or more rejections
   - developer fix loop
   - all branches approved

4. Wait timeout
   - no event before wait_timeout_sec
   - wait returns timeout=true
   - job continues running
   - next wait receives event

5. Job timeout
   - one worker exceeds timeout
   - attempt marked timed_out
   - terminal interrupted/killed
   - output preserved
   - other jobs continue

6. Duplicate marker
   - worker emits same completion twice
   - one event accepted
   - duplicate ignored via dedupe_key

7. Malformed marker
   - attempt fails with malformed_callback
   - terminal output preserved

8. Terminal killed externally
   - watchdog detects missing tmux pane
   - emits job.failed

9. CAO server restart
   - workers continue or terminal state reconciles
   - event log survives
   - status/wait can resume

10. Run cancellation
   - active terminals killed
   - queued jobs cancelled
   - run.cancelled emitted
   - no orphans
```

Performance envelope:

```text
N = 3, 4, 8, 16 workers

Measure:
- spawn latency
- event ingestion latency
- wait wake-up latency
- terminal CPU/memory
- SQLite write contention
- provider rate-limit behavior
- cleanup reliability
```

Initial targets:

```text
event ingestion → wait return p95 < 250ms
zero orphan terminals after normal runs
zero duplicate visible completion events
starter scenario deterministic across 10 runs
```

## Main risks

| Risk                                                    | Severity | Mitigation                                                                          |
| ------------------------------------------------------- | -------: | ----------------------------------------------------------------------------------- |
| Codex breaks loop between sequential blocking MCP calls |     High | Phase 0 gate. If it fails, promote `handoff_batch` fallback.                        |
| MCP tool timeout kills long waits                       |     High | Bounded long-polling, default 45s, recommended `tool_timeout_sec = 3600`.           |
| Inbox/inbox-like semantics creep back in                |     High | Hard architectural rule: orchestration events never depend on recipient idle state. |
| Auto-finalization race                                  |     High | Use `run.idle`; supervisor explicitly finalizes.                                    |
| Worker never emits completion                           |   Medium | Job timeout + tmux liveness watchdog + preserved output.                            |
| Malformed or duplicate markers                          |   Medium | Sentinel/base64 format, schema validation, dedupe key.                              |
| Orphan terminals                                        |     High | tmux naming, startup reconciler, reaper loop.                                       |
| Context bloat in supervisor                             |   Medium | Return compact event batches and output refs/previews, not full logs.               |
| Reviewer/fix loops never converge                       |   Medium | Supervisor prompt and optional `max_cycles_per_chain`; fail deterministically.      |
| Existing behavior regression                            |   Medium | Do not refactor `handoff` in Phase 0; regression suite before wrapper refactor.     |

## Branch scope summary

I’d scope the new branch as:

```text
branch: feature/orchestration-event-bus
```

Definition of done for the first reviewable branch:

```text
- New orchestration tables created/migrated
- New MCP tools exposed:
  - orchestration_start
  - orchestration_spawn
  - orchestration_wait
  - orchestration_status
  - orchestration_cancel
  - orchestration_finalize

- Worker prompts include run/job/attempt identifiers
- Worker terminal marker parser writes orchestration_events
- Scheduler supports queued/running jobs with basic concurrency caps
- wait blocks through MCP and returns event batches
- Existing assign/handoff/send_message untouched
- Starter scenario test passes
- Codex MCP timeout config documented
- Basic cleanup/reaper implemented
```

The thing to be ruthless about: **don’t turn this into a general workflow engine in Phase 0.** The MVP is not a DAG DSL. The MVP is the lower-level substrate that lets a supervisor model run a dynamic DAG while staying in the MCP wait loop.

That substrate is: **durable run/job/event state + tracked worker callbacks + blocking wait + explicit spawn/finalize.** Once that works, `handoff_batch`, workflow policies, prettier observability, and richer progress events are straightforward wrappers instead of architectural bets.
