# WORK-UNITS: Orchestration Event Bus Branch

## Branch

- Branch name: `feature/orchestration-event-bus`
- Base branch: current fork head from `sync/plugin-d472f64`
- Goal: deliver the first reviewable implementation of a dedicated orchestration run/event bus with blocking wait semantics for Codex-compatible multi-worker fan-out

## Scope guardrails

- In scope:
  - New orchestration storage, runtime, API, MCP tools, worker callback protocol, cleanup/recovery, and tests
  - A Codex-focused proof that repeated blocking waits can drive a dynamic developer/reviewer loop
  - Documentation for the new branch and operator configuration
- Out of scope for this branch:
  - `handoff_batch`
  - Declarative workflow DSL
  - Worker-side MCP callback tools
  - Postgres or multi-host support
  - Subtree cancellation
  - Reimplementing legacy `handoff` on top of the new engine
  - Changing existing `assign`, `handoff`, or `send_message` semantics

## Confirmed current-state constraints

- Current `handoff` is implemented in [`src/cli_agent_orchestrator/mcp_server/server.py`](/home/erik/Development/cli-agent-orchestrator/src/cli_agent_orchestrator/mcp_server/server.py) as a blocking flow that creates a worker, injects a callback protocol, and polls the supervisor inbox for a completion marker.
- Current `assign` is non-blocking and depends on inbox delivery through [`src/cli_agent_orchestrator/services/inbox_service.py`](/home/erik/Development/cli-agent-orchestrator/src/cli_agent_orchestrator/services/inbox_service.py).
- Current persistence is concentrated in [`src/cli_agent_orchestrator/clients/database.py`](/home/erik/Development/cli-agent-orchestrator/src/cli_agent_orchestrator/clients/database.py) with only `terminals`, `inbox`, and `flows`.
- Current server lifecycle ownership sits in [`src/cli_agent_orchestrator/api/main.py`](/home/erik/Development/cli-agent-orchestrator/src/cli_agent_orchestrator/api/main.py); inbox watching, plugin loading, DB init, and flow daemon already start there.
- Narrow baseline verified live on 2026-04-18:
  - `uv run pytest test/mcp_server/test_handoff.py test/api/test_api_endpoints.py -k 'handoff or skill or inbox' -v`
  - Result: 12 passed

## Definition of done for the branch

- New orchestration entities exist and are durable in SQLite.
- New MCP tools exist:
  - `orchestration_start`
  - `orchestration_spawn`
  - `orchestration_wait`
  - `orchestration_status`
  - `orchestration_cancel`
  - `orchestration_finalize`
- `orchestration_wait` provides bounded blocking long-poll behavior and does not rely on inbox delivery.
- Orchestration workers emit structured completion markers that are parsed into durable events.
- A Codex supervisor can drive a 4-developer -> 4-reviewer -> rejection/fix -> finalize loop using repeated blocking waits.
- Legacy `handoff`, `assign`, and `send_message` regressions remain green.
- No CAO-tagged orphan terminals remain after normal completion or cancellation.

## Implementation rules

- Keep the orchestration engine server-owned inside `cao-server`; the MCP server remains a thin adapter.
- Do not add orchestration SQL directly to MCP or API handlers; use a dedicated repository/service layer.
- Use append-only events plus cursors. Do not introduce a global consumed flag as the source of truth.
- Do not auto-finalize runs when active jobs drop to zero. Emit `run.idle`; finalization remains explicit.
- Keep inbox transport for human/direct agent messages only. Orchestration callbacks must use the new event bus.
- Keep provider-specific behavior isolated. Do not leak provider parsing logic into generic orchestration models.

## Suggested file seams

- Storage and migrations:
  - [`src/cli_agent_orchestrator/clients/database.py`](/home/erik/Development/cli-agent-orchestrator/src/cli_agent_orchestrator/clients/database.py)
  - new module: `src/cli_agent_orchestrator/clients/orchestration_store.py`
- Types and request/response models:
  - new module: `src/cli_agent_orchestrator/models/orchestration.py`
- Runtime services:
  - new module: `src/cli_agent_orchestrator/services/orchestration_service.py`
  - new module: `src/cli_agent_orchestrator/services/orchestration_runtime.py`
  - possible helper: `src/cli_agent_orchestrator/services/orchestration_callbacks.py`
- HTTP surface:
  - [`src/cli_agent_orchestrator/api/main.py`](/home/erik/Development/cli-agent-orchestrator/src/cli_agent_orchestrator/api/main.py)
- MCP surface:
  - [`src/cli_agent_orchestrator/mcp_server/server.py`](/home/erik/Development/cli-agent-orchestrator/src/cli_agent_orchestrator/mcp_server/server.py)
- Terminal/session integration:
  - [`src/cli_agent_orchestrator/services/terminal_service.py`](/home/erik/Development/cli-agent-orchestrator/src/cli_agent_orchestrator/services/terminal_service.py)
  - [`src/cli_agent_orchestrator/services/session_service.py`](/home/erik/Development/cli-agent-orchestrator/src/cli_agent_orchestrator/services/session_service.py)
- Built-in protocols:
  - [`src/cli_agent_orchestrator/agent_store/code_supervisor.md`](/home/erik/Development/cli-agent-orchestrator/src/cli_agent_orchestrator/agent_store/code_supervisor.md)
  - [`src/cli_agent_orchestrator/agent_store/developer.md`](/home/erik/Development/cli-agent-orchestrator/src/cli_agent_orchestrator/agent_store/developer.md)
  - [`src/cli_agent_orchestrator/agent_store/reviewer.md`](/home/erik/Development/cli-agent-orchestrator/src/cli_agent_orchestrator/agent_store/reviewer.md)
- Tests:
  - `test/clients/`
  - `test/services/`
  - `test/api/`
  - `test/mcp_server/`
  - `test/e2e/`

## Work units

### W0. Branch bootstrap and branch ADR

- Owner: lead engineer
- Depends on: none
- Outcome:
  - Create the feature branch
  - Add a short ADR or workpad that freezes scope to Phase 0
  - Record branch success criteria and explicit non-goals
- Deliverables:
  - Repo-local branch note or ADR
  - Checklist for merge gate
- Acceptance:
  - Team can point to a single branch-scoping doc that says this branch does not include `handoff_batch`, worker-side MCP callbacks, Postgres, subtree cancel, or a legacy `handoff` refactor

### W1. Orchestration types and contracts

- Owner: backend engineer
- Depends on: W0
- Outcome:
  - Introduce typed models for runs, jobs, attempts, events, snapshots, and tool request/response payloads
  - Centralize enums for run status, job status, attempt status, event types, cancel scopes, and cleanup policy
- Deliverables:
  - `models/orchestration.py`
  - Shared request/response types consumed by API, service, and MCP layers
- Acceptance:
  - No stringly-typed orchestration payloads remain outside model definitions
  - The six MCP tools and matching HTTP endpoints can reuse the same typed schemas

### W2. SQLite schema and migration support

- Owner: persistence engineer
- Depends on: W1
- Outcome:
  - Extend the existing SQLite schema with:
    - `orchestration_runs`
    - `orchestration_jobs`
    - `orchestration_attempts`
    - `orchestration_events`
    - `worker_terminals`
    - `orchestration_subscriptions`
  - Add idempotent schema migration helpers to `init_db()`
- Deliverables:
  - Updated DB initialization path
  - Migration helpers for existing local databases
- Acceptance:
  - New tables are created on an empty DB
  - Existing DBs upgrade without breaking current `terminals`, `inbox`, or `flows`
  - WAL-friendly assumptions remain intact

### W3. Orchestration repository layer

- Owner: persistence engineer
- Depends on: W2
- Outcome:
  - Build a dedicated store/repository abstraction for orchestration state
  - Support create/read/update operations for runs, jobs, attempts, events, subscriptions, and snapshots
  - Support event cursor reads and dedupe checks
- Deliverables:
  - `clients/orchestration_store.py`
  - Repository tests
- Acceptance:
  - API and runtime layers do not directly write raw orchestration SQL
  - Store supports append-only event writes and cursor-based reads

### W4. Runtime bootstrap and in-process signaling

- Owner: backend engineer
- Depends on: W3
- Outcome:
  - Add orchestration runtime ownership to `cao-server` lifespan
  - Introduce per-`run_id` in-process signaling/pub-sub for wait wakeups
  - Keep this fully separate from inbox delivery
- Deliverables:
  - Runtime start/stop wiring in `api/main.py`
  - Orchestration runtime component
- Acceptance:
  - Server startup initializes orchestration services cleanly
  - Inbox watcher and flow daemon continue to work unchanged

### W5. Worker callback marker protocol

- Owner: protocol engineer
- Depends on: W1
- Outcome:
  - Define the exact orchestration worker marker:
    - `⟦CAO-EVENT-v1:<base64url-json>⟧`
  - Define the JSON schema fields:
    - `version`
    - `run_id`
    - `job_id`
    - `attempt_id`
    - `type`
    - `status`
    - `result`
    - `nonce`
  - Define orchestration-only prompt injection rules
- Deliverables:
  - Worker callback contract document in code comments/tests
  - Prompt templating helper for orchestration workers
- Acceptance:
  - Orchestration-spawned workers receive stable IDs and one explicit completion protocol
  - Legacy handoff/assign prompts remain untouched

### W6. Marker parser and callback ingestion

- Owner: backend engineer
- Depends on: W3, W5
- Outcome:
  - Parse worker terminal output for orchestration markers
  - Validate payload shape
  - Deduplicate duplicate events
  - Persist lifecycle and semantic events
- Deliverables:
  - Parser/ingest module
  - Event normalization logic
- Acceptance:
  - Valid markers become durable events
  - Malformed markers produce deterministic failure handling
  - Duplicate markers do not generate duplicate visible events

### W7. Scheduler and attempt lifecycle

- Owner: backend engineer
- Depends on: W4, W5, W6
- Outcome:
  - Implement orchestration job queueing and attempt creation
  - Spawn tmux workers for orchestration jobs with prompt injection
  - Track `job_id` vs `attempt_id` cleanly
  - Enforce conservative concurrency defaults
- Defaults:
  - `max_active_workers_per_run = 4`
  - `max_active_reviewers_per_run = 2`
  - `max_total_terminals = 8`
  - `job_default_timeout_sec = 900`
  - `run_default_timeout_sec = 3600`
- Deliverables:
  - Scheduler loop
  - Attempt state transitions
  - Worker terminal tagging/naming for orchestration attempts
- Acceptance:
  - Jobs move through `created -> queued -> running -> terminal outcome`
  - Scheduler never exceeds per-run or global caps

### W8. `orchestration_wait` engine

- Owner: backend engineer
- Depends on: W3, W4, W7
- Outcome:
  - Implement bounded blocking wait behavior over append-only events
  - Support:
    - `cursor`
    - `event_types`
    - `job_ids`
    - `min_events`
    - `max_events`
    - `wait_timeout_sec`
    - `include_snapshot`
- Deliverables:
  - Wait service
  - Snapshot assembler
- Acceptance:
  - Returns immediately when matching events exist
  - Blocks when none exist
  - Returns `timeout=true` on wait timeout without affecting jobs
  - Never depends on inbox delivery

### W9. Timeout handling and cancellation

- Owner: backend engineer
- Depends on: W7, W8
- Outcome:
  - Add job timeout handling
  - Add run timeout handling
  - Add Phase-0 cancel scopes:
    - `run`
    - `job`
  - Ensure timed-out or cancelled jobs interrupt and then kill terminals if needed
- Deliverables:
  - Timeout reconciler
  - Cancel handlers
- Acceptance:
  - Job timeout marks the attempt/job correctly and preserves output
  - Run timeout cancels active/queued jobs deterministically
  - Cancel events are durable and observable

### W10. Status and finalization surface

- Owner: backend engineer
- Depends on: W3, W8, W9
- Outcome:
  - Implement non-blocking run inspection and snapshots
  - Implement explicit finalization with cleanup policy
  - Emit `run.idle` separately from `run.finalized`
- Deliverables:
  - `orchestration_status`
  - `orchestration_finalize`
- Acceptance:
  - Runs never auto-finalize just because active jobs hit zero
  - Supervisors can inspect and explicitly close runs

### W11. HTTP API endpoints

- Owner: API engineer
- Depends on: W8, W9, W10
- Outcome:
  - Add HTTP endpoints in `cao-server` for:
    - start
    - spawn
    - wait
    - status
    - cancel
    - finalize
- Deliverables:
  - Typed request/response endpoints in `api/main.py`
  - API tests
- Acceptance:
  - HTTP endpoints expose the full lifecycle with stable validation and error handling
  - Existing endpoints remain backward compatible

### W12. MCP tool surface

- Owner: MCP engineer
- Depends on: W11
- Outcome:
  - Add six MCP tools to `mcp_server/server.py`
  - Keep the MCP server thin: forward requests to API, parse responses, and avoid server-local orchestration state
- Deliverables:
  - MCP tool registration
  - MCP wrapper tests
- Acceptance:
  - MCP tool names exactly match the approved contract
  - The MCP server does not reimplement orchestration logic already owned by `cao-server`

### W13. Recovery, reconciliation, and reaper

- Owner: runtime engineer
- Depends on: W7, W9
- Outcome:
  - Tag orchestration terminals predictably
  - On server startup:
    - reconcile running attempts
    - detect missing terminals
    - reattach monitors where possible
  - Add a reaper for finalized runs and orphan cleanup
- Deliverables:
  - Startup reconciliation path
  - Reaper loop
- Acceptance:
  - Restart does not strand orphan attempts silently
  - Normal completion and cancellation leave no CAO-tagged orphan terminals

### W14. Supervisor and worker protocol updates

- Owner: prompt/protocol engineer
- Depends on: W5, W12
- Outcome:
  - Update built-in protocol content only where needed for orchestration-run behavior
  - Add a canonical supervisor orchestration loop for repeated `orchestration_wait`
  - Tell orchestration workers how to emit the new completion marker
- Deliverables:
  - Updates to built-in agent markdown or bundled skills
- Acceptance:
  - Supervisor knows how to spawn, wait, react, and finalize
  - Developer/reviewer know the orchestration-specific callback contract
  - Legacy handoff/assign guidance stays intact

### W15. Unit and integration test suite

- Owner: test engineer
- Depends on: W6, W8, W9, W10, W11, W12, W13
- Outcome:
  - Add deterministic tests for:
    - store CRUD and migrations
    - marker parsing
    - dedupe
    - wait timeout
    - job timeout
    - run timeout
    - cancel
    - finalize
    - restart reconciliation
    - MCP wrapper validation
- Deliverables:
  - New test modules in `test/clients`, `test/services`, `test/api`, and `test/mcp_server`
- Acceptance:
  - Orchestration units are covered without depending on e2e-only validation

### W16. Codex starter-scenario e2e proof

- Owner: e2e engineer
- Depends on: W12, W14, W15
- Outcome:
  - Add a Codex-first end-to-end scenario:
    - 4 developer jobs fan out
    - each completion triggers a reviewer
    - one reviewer rejects
    - supervisor spawns a developer fix
    - all chains eventually approve
    - supervisor explicitly finalizes
- Deliverables:
  - New e2e test file for orchestration runs
  - Helper utilities as needed
- Acceptance:
  - Scenario completes via repeated blocking `orchestration_wait` calls
  - At least 3 workers run concurrently
  - No orphan terminals remain after completion

### W17. Documentation and merge preparation

- Owner: docs engineer
- Depends on: W16
- Outcome:
  - Document the new orchestration tool surface
  - Document Codex MCP timeout guidance and recommended config
  - Update rolling README per repo rule when the code lands
  - Summarize known limitations and deferred work
- Deliverables:
  - README rolling update entry
  - docs updates for orchestration usage and configuration
- Acceptance:
  - Operators know how to configure the branch
  - Reviewers can see exactly what shipped, what was deferred, and how to validate it

## Dependency order

1. `W0 -> W1 -> W2 -> W3 -> W4`
2. `W5 -> W6 -> W7`
3. `W8 -> W9 -> W10`
4. `W11 -> W12`
5. `W13`
6. `W14`
7. `W15`
8. `W16`
9. `W17`

## Parallelization opportunities

- `W1` and branch prep details in `W0` can overlap after scope is frozen.
- `W5` can run in parallel with `W2-W4` once the type contracts are stable.
- `W11` and `W14` can start once the service contracts stop moving.
- `W15` can be split across store/runtime/API/MCP owners once interfaces stabilize.

## Validation gates

### Gate A: pre-branch regression baseline

- `uv run pytest test/mcp_server/test_handoff.py test/mcp_server/test_send_message.py -v`
- `uv run pytest test/api/test_terminals.py test/api/test_api_endpoints.py -k 'handoff or inbox or send_input or skill' -v`

### Gate B: orchestration unit/integration baseline

- `uv run pytest test/clients test/services test/api test/mcp_server -k 'orchestration or handoff or inbox' -v`

### Gate C: formatting and typing

- `uv run black src/ test/`
- `uv run isort src/ test/`
- `uv run mypy src/`

### Gate D: Codex proof

- `uv run pytest -m e2e test/e2e/test_orchestration_run.py -v -o "addopts=" -k codex`

### Gate E: branch acceptance

- Starter scenario passes
- Existing `handoff` and `assign` regressions remain green
- Wait is bounded and resumable
- One worker timeout does not collapse unrelated jobs
- No CAO-tagged orphan terminals remain after normal completion or cancellation

## Risks to watch while implementing

- Codex may fail to sustain the repeated blocking wait loop across sequential MCP calls.
- Long waits may exceed MCP/client timeout configuration if defaults are not documented or respected.
- Engineers may accidentally route orchestration callbacks through inbox semantics because that path already exists.
- Engine-owned auto-finalization is a likely race condition in dynamic DAG flows.
- Terminal cleanup will drift unless startup reconciliation and a reaper are treated as required branch work, not follow-up polish.

## Explicit defaults for engineers

- Store orchestration state in SQLite for this branch.
- Use append-only events and cursor replay.
- Return event batches, not single events only.
- Default `orchestration_wait.wait_timeout_sec` to `45`.
- Default `orchestration_wait.min_events` to `1`.
- Default `orchestration_wait.max_events` to `10`.
- Keep retries supervisor-driven in Phase 0; the engine tracks attempts but does not make semantic retry decisions.
- Preserve output for timed-out, malformed, or externally-killed attempts so failures are debuggable.

## Defer list for next branch

- `handoff_batch`
- Declarative workflow rules / DSL
- Worker-side callback MCP tools
- Subtree cancellation
- `cao runs` CLI inspection commands
- Advanced metrics and UI timeline rendering
- Postgres
- Wrapping legacy `handoff` on the new substrate
