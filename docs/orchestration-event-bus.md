# Orchestration Event Bus (Phase 0)

This document describes the Phase 0 orchestration run/event bus shipped on branch
`feature/orchestration-event-bus`.

## What Shipped

- Durable orchestration entities in SQLite: runs, jobs, attempts, events, worker terminal links.
- Server-owned orchestration engine in `cao-server` (scheduler, timeout handling, cancellation, explicit finalization).
- MCP + HTTP orchestration lifecycle tools/endpoints:
  - `orchestration_start`
  - `orchestration_spawn`
  - `orchestration_wait`
  - `orchestration_status`
  - `orchestration_cancel`
  - `orchestration_finalize`
- Worker callback marker protocol:
  - `⟦CAO-EVENT-v1:<base64url-json>⟧`
- Startup reconciliation + periodic reaper behavior for terminal/run cleanup.

## Supervisor Loop Pattern

`orchestration_wait` is the blocking primitive for in-call continuity. Use cursors and repeat:

```text
1. orchestration_start
2. orchestration_spawn (fan-out)
3. orchestration_wait(cursor=..., min_events=1, wait_timeout_sec=45)
4. react to events (spawn review/fix/cancel as needed)
5. repeat step 3 with returned cursor
6. orchestration_finalize when complete
```

Key rule: do not auto-finalize when active jobs drop to zero. `run.idle` is intermediate; finalization is explicit.

## Recovery and Reaper Behavior

### Startup reconciliation

On server startup, orchestration reconciliation now:

- scans non-terminal runs (`running`, `idle`),
- checks `running` attempts for missing worker terminals,
- marks missing-running attempts/jobs as failed (`terminal_missing`),
- ingests any pending callback markers from existing worker log files,
- runs one maintenance/scheduling pass per run.

### Reaper

Background reaper runs every 60s and reconciles terminal runs (`finalized`, `cancelled`, `timed_out`, `failed`):

- `finalized` runs:
  - apply persisted finalization cleanup policy,
  - support `preserve_until_ttl` retention for successful terminals,
  - kill when TTL expires,
  - release links for preserved terminals.
- `cancelled`/`timed_out`/`failed` runs:
  - deterministically terminate linked worker terminals to avoid CAO-tagged orphans.

Run cancellation now also triggers deterministic worker terminal cleanup via maintenance/reaper path.

## Codex Timeout Guidance

For orchestration, use bounded waits and ensure Codex MCP server tool timeout is long enough:

```toml
[mcp_servers.cao-mcp-server]
tool_timeout_sec = 3600.0
```

Guidance:

- Keep `orchestration_wait.wait_timeout_sec` bounded (recommended `45`).
- Call `orchestration_wait` repeatedly with cursor advancement.
- Do not rely on indefinite single-call blocking.

## Validation Commands

- Orchestration unit/integration:
  - `uv run pytest test/services/test_orchestration_service.py test/services/test_orchestration_runtime.py test/clients/test_orchestration_store.py test/api/test_orchestration_api.py test/mcp_server/test_orchestration_tools.py -v`
- Codex starter scenario e2e:
  - `uv run pytest -m e2e test/e2e/test_orchestration_run.py -v -o "addopts=" -k codex`

## Known Limits / Deferred Work

- No worker-side MCP callback tool yet; Phase 0 uses terminal log marker ingestion.
- No `attempt` or `subtree` cancellation scope yet (only `run` and `job`).
- No Postgres/multi-host runtime support in this branch.
- No declarative workflow DSL or `handoff_batch`.
- Legacy `handoff`/`assign` semantics are intentionally preserved (not reimplemented on orchestration engine yet).
