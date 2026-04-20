# Research Brief: Async Fan-Out Handoffs in CAO

## Purpose

This document gives a research planning agent enough context to investigate how to support **multi-worker fan-out orchestration with in-call continuity** in CLI Agent Orchestrator (CAO), especially under Codex CLI behavior constraints.

The researcher should treat this as a systems-design + feasibility + implementation-path investigation.

## Context: What CAO Is

CLI Agent Orchestrator (CAO) runs multiple AI agents in tmux-backed terminals and exposes orchestration tools through an MCP server (`cao-mcp-server`).

Core orchestration tools exposed to supervisor agents:

- `handoff(agent_profile, message, timeout?)`
- `assign(agent_profile, message)`
- `send_message(receiver_id, message)`

High-level behavior today:

- `handoff` creates a worker terminal, sends work, then **blocks** until completion callback is observed, then returns output and cleans up worker.
- `assign` creates a worker terminal, sends work, and **returns immediately** (async callback expected later via inbox + `send_message`).
- `send_message` persists message to inbox for receiver; delivery happens when receiver is idle.

## Key Terms

- **Supervisor**: coordinating agent (e.g., `code_supervisor`) that delegates work.
- **Worker**: delegated agent (developer/reviewer/etc.).
- **Handoff**: synchronous/blocking delegation with in-call wait loop.
- **Assign**: asynchronous delegation, no built-in join/wait in the same tool call.
- **In-call work loop**: the calling model/session remains in one tool call while continuously waiting/processing callback events.

## Current Problem

We need supervisor workflows that can:

1. fan out many jobs quickly,
2. keep an active continuous loop (no turn-boundary interruption),
3. receive worker outputs as they complete,
4. dispatch follow-up work (e.g., reviewers) immediately,
5. continue until a dynamic DAG of work is done.

Today, CAO offers a tradeoff:

- `handoff` gives loop continuity but only one blocking worker per call.
- `assign` gives fan-out but loses in-call continuity because results return via inbox delivery tied to supervisor idle turns.

So the gap is: **there is no first-class primitive that combines handoff-style blocking loop semantics with multi-job fan-out and incremental result handling.**

## Critical Constraint From User

This is non-negotiable for research:

> Codex CLI will only sustain the desired loop behavior if we block/wait through MCP in the style `handoff` does.

Implication:

- Solutions that rely only on “end turn, wait for inbox, resume later” are not sufficient for target behavior.
- We likely need a **blocking MCP tool surface** that internally multiplexes many child jobs and streams/returns completion events.

## Why Existing `assign` Is Not Enough

`assign` is fire-and-forget with callback via inbox. It lacks:

- correlation-aware join semantics,
- blocking event loop in-call,
- built-in aggregation and completion criteria,
- supervisor-side immediate “on callback, dispatch next job” orchestration without loop interruption.

## What “Good” Looks Like (Solution Outcomes)

A successful design should support:

1. **Fan-out**: start N workers/jobs in one supervisor action.
2. **Blocking continuity**: single MCP call can stay active and manage many jobs.
3. **Incremental returns**: receive each completion as event(s), not only final summary.
4. **Dynamic dispatch**: on each completion, launch follow-on jobs (e.g., reviewer) in same loop.
5. **Deterministic completion**: clear stop conditions (all done, quorum, timeout, cancellation).
6. **Correlation and state**: job IDs, parent-child linkage, status transitions.
7. **Resilience**: retries/timeouts/crash recovery without orphaning terminals.
8. **Operational safety**: bounded worker count, cleanup policy, observability.
9. **Backward compatibility**: existing `handoff`/`assign` flows keep working.

## Candidate Direction: First-Class Async Handoff Family

Potential API family to investigate:

- `handoff_async(...)` (spawn + register tracked job, returns `job_id`)
- `await_handoffs(...)` or `handoff_wait(...)` (blocking wait loop over job set)
- optional `handoff_stream(...)` (blocking tool call yielding event batches)

Alternative single-tool design:

- `handoff_batch(...)` that both spawns and blocks, internally running an event loop until completion criteria.

Important: despite “async” naming, the supervisor-facing control loop can still be **blocking in MCP** to satisfy Codex behavior; async here means multi-job orchestration, not “end turn and come back later”.

## Research Questions

### A. Protocol and API Design

1. What is the minimal tool API that enables fan-out + in-call wait + dynamic follow-up dispatch?
2. Should we use one monolithic blocking tool (`handoff_batch`) or composable tools (`spawn` + `wait`)?
3. How should event payloads be represented (job lifecycle, partial output, completion marker, errors)?
4. How are parent/child workflows modeled (developer -> reviewer chains)?

### B. Runtime/Event Loop Feasibility

1. Can one MCP tool invocation safely host a long-lived multiplexed wait loop?
2. How do we poll/subscribe to callbacks without relying on idle inbox delivery semantics?
3. Should callbacks bypass inbox and go to a dedicated orchestration event store/queue?
4. What are tool timeout constraints at provider layer (Codex/others), and how do we extend safely?

### C. State Management

1. What schema is needed for jobs/runs/events (IDs, statuses, timestamps, worker terminal IDs)?
2. How do we support idempotency and reattachment after supervisor interruption?
3. How are retries represented (same job ID vs attempt IDs)?

### D. Scheduling/Concurrency

1. How many workers can be active before provider/tmux/resource degradation?
2. Do we need per-provider concurrency limits and backpressure?
3. What fairness model is needed when many callbacks arrive?

### E. Failure and Cleanup

1. Timeout strategy per job vs global loop timeout?
2. Cancellation semantics (cancel one job, subtree, whole run)?
3. How to prevent orphan terminals on crash/timeout?
4. What to preserve for debugging vs auto-cleanup?

### F. UX/Agent Prompting

1. How should supervisor prompts/protocols change to exploit new primitives?
2. How do we expose intermediate progress to user without breaking loop?
3. How do we keep developer/reviewer roles explicit in a dynamic DAG?

## Approaches the Researcher Should Compare

### Approach 1: Extend Inbox Model

Keep inbox as transport but add:

- correlated job metadata,
- active blocking wait API over inbox events,
- richer callback protocol.

Pros: minimal architectural change.  
Cons: inbox currently tied to idle delivery assumptions; may fight desired always-active loop.

### Approach 2: New Orchestration Run/Event Bus (Recommended to evaluate deeply)

Add first-class orchestration entities:

- `orchestration_runs`
- `orchestration_jobs`
- `orchestration_events`

Workers post to run/job event channel directly (not idle-gated inbox). Blocking MCP call waits on this channel.

Pros: clean semantics for fan-out loops, observability, replay/resume.  
Cons: larger implementation surface.

### Approach 3: Hybrid

Use existing inbox for human/direct messaging, introduce dedicated job event channel only for orchestration callbacks.

Pros: bounded change with cleaner behavior.  
Cons: dual-path complexity.

### Approach 4: Supervisor-internal polling over terminal output only

Avoid new stores, poll worker terminals directly.

Pros: fast prototype.  
Cons: brittle, poor correlation semantics, weak recovery; likely not production-grade.

## Non-Goals (For This Research Phase)

- Rewriting provider status parsers wholesale.
- Changing tmux substrate.
- Removing existing `handoff` or `assign`.
- Perfect real-time streaming UI.

## Deliverables Expected From Researcher

1. **Feasibility memo**: which approach can satisfy Codex-style blocking-loop constraint.
2. **Proposed protocol spec**:
   - tool APIs,
   - callback markers/messages,
   - job/event schema,
   - timeout/cancellation semantics.
3. **State model + data schema** draft.
4. **Execution model**:
   - event loop pseudocode,
   - concurrency limits,
   - retry/cleanup strategy.
5. **Risk register**:
   - technical risks,
   - failure modes,
   - mitigations.
6. **Incremental rollout plan**:
   - phase 0 prototype,
   - phase 1 production hardening,
   - backward compatibility strategy.
7. **Validation plan**:
   - e2e scenarios for fan-out + dynamic reviewer dispatch,
   - chaos/failure tests (timeouts/crashes/orphans),
   - performance envelope tests.

## Minimum Acceptance Criteria for Final Design

The researched design must prove it can:

1. Dispatch at least 3+ workers in one orchestration run.
2. Keep supervisor in one blocking MCP loop while workers complete.
3. Process each worker completion and trigger follow-up jobs in-loop.
4. Finish with deterministic final completion and cleanup.
5. Survive at least one worker timeout/failure without collapsing whole run (unless configured fail-fast).

## Starter Scenario to Validate Against

Use this scenario as a concrete benchmark:

1. Supervisor receives feature request.
2. Fan out 4 developer workers (different modules).
3. On each developer completion, dispatch reviewer worker for that module.
4. If reviewer rejects, re-dispatch developer fix for that module.
5. Continue loop until all module reviewer chains are approved.
6. Return final aggregate summary in same orchestration run.

Any proposed architecture that cannot support this cleanly is insufficient.

