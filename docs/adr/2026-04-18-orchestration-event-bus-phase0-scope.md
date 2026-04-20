# ADR: Orchestration Event Bus Branch Scope (Phase 0)

- Date: 2026-04-18
- Branch: `feature/orchestration-event-bus`
- Status: Accepted

## Branch Goal

Deliver a first reviewable implementation of a dedicated orchestration run/event bus with blocking wait semantics, while preserving current `assign`, `handoff`, and `send_message` behavior.

## In Scope (Phase 0)

- Typed orchestration contracts for runs, jobs, attempts, events, snapshots, and request/response payloads.
- SQLite-backed orchestration entities and append-only events with cursor-based reads.
- Runtime ownership in `cao-server` with bounded `orchestration_wait` long-poll behavior.
- Six orchestration surfaces:
  - `orchestration_start`
  - `orchestration_spawn`
  - `orchestration_wait`
  - `orchestration_status`
  - `orchestration_cancel`
  - `orchestration_finalize`
- Structured worker callback protocol via terminal marker parsing.

## Explicit Non-Goals for This Branch

- `handoff_batch`
- worker-side MCP callbacks
- Postgres / multi-host support
- subtree cancellation
- refactoring legacy `handoff` onto the new engine

## Merge / Acceptance Gates

- A single source-of-truth orchestration model contract exists and is shared by storage/runtime/API/MCP layers.
- Orchestration events are append-only and consumed via cursor semantics (no global consumed flag).
- `orchestration_wait` remains bounded and compatible with Codex MCP timeout behavior.
- Runs are not auto-finalized when active jobs reach zero; explicit finalization is required.
- Existing `assign`, `handoff`, and `send_message` semantics and tests remain green.
- Focused tests cover new contract validation and enum stability.

## Notes

This ADR freezes Phase 0 scope to keep the branch reviewable and to prevent coupling early contracts to future multi-host or DSL work.
