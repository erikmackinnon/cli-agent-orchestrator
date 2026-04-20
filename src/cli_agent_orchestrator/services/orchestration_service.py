"""Core orchestration engine service for run scheduling and lifecycle control."""

from __future__ import annotations

import asyncio
import logging
import math
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from cli_agent_orchestrator.clients.database import get_terminal_metadata
from cli_agent_orchestrator.clients.tmux import tmux_client
from cli_agent_orchestrator.constants import DEFAULT_PROVIDER, TERMINAL_LOG_DIR
from cli_agent_orchestrator.models.orchestration import (
    AttemptRecord,
    AttemptStatePayload,
    AttemptStatus,
    AttemptTerminalRef,
    CancellationPayload,
    CancelScope,
    CleanupPolicy,
    EventType,
    FinalizationPayload,
    JobRecord,
    JobStatePayload,
    JobStatus,
    OrchestrationCancelRequest,
    OrchestrationCancelResponse,
    OrchestrationEvent,
    OrchestrationEventPayload,
    OrchestrationFinalizeRequest,
    OrchestrationFinalizeResponse,
    OrchestrationJobSpec,
    OrchestrationSpawnRequest,
    OrchestrationSpawnResponse,
    OrchestrationStartPolicy,
    OrchestrationStartRequest,
    OrchestrationStartResponse,
    OrchestrationStatusRequest,
    OrchestrationStatusResponse,
    OrchestrationWaitRequest,
    OrchestrationWaitResponse,
    RunOutcome,
    RunRecord,
    RunStatePayload,
    RunStatus,
    SuccessfulTerminalCleanupPolicy,
)
from cli_agent_orchestrator.models.terminal import Terminal
from cli_agent_orchestrator.services import session_service, terminal_service
from cli_agent_orchestrator.services.orchestration_runtime import OrchestrationRuntime
from cli_agent_orchestrator.utils.agent_profiles import resolve_provider

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _EffectivePolicy:
    """Effective scheduler/time budget policy for a run."""

    max_active_workers_per_run: int
    max_active_reviewers_per_run: int
    max_total_terminals: int
    default_job_timeout_sec: int
    run_timeout_sec: int


class OrchestrationService:
    """Server-owned orchestration lifecycle engine.

    This service intentionally stays independent from public HTTP/MCP surfaces.
    It owns run/job/attempt scheduling, bounded event waits, cancellation,
    timeouts, and explicit finalization over the append-only event store.
    """

    DEFAULT_MAX_ACTIVE_WORKERS_PER_RUN = 4
    DEFAULT_MAX_ACTIVE_REVIEWERS_PER_RUN = 2
    DEFAULT_MAX_TOTAL_TERMINALS = 8
    DEFAULT_JOB_TIMEOUT_SEC = 900
    DEFAULT_RUN_TIMEOUT_SEC = 3600

    _JOB_TERMINAL_STATUSES = {
        JobStatus.SUCCEEDED.value,
        JobStatus.FAILED.value,
        JobStatus.CANCELLED.value,
        JobStatus.TIMED_OUT.value,
    }
    _ATTEMPT_TERMINAL_STATUSES = {
        AttemptStatus.SUCCEEDED.value,
        AttemptStatus.FAILED.value,
        AttemptStatus.CANCELLED.value,
        AttemptStatus.TIMED_OUT.value,
    }
    _RUN_TERMINAL_STATUSES = {
        RunStatus.FINALIZED.value,
        RunStatus.CANCELLED.value,
        RunStatus.TIMED_OUT.value,
        RunStatus.FAILED.value,
    }

    def __init__(
        self,
        *,
        runtime: OrchestrationRuntime,
        create_session_fn: Callable[..., Terminal] = session_service.create_session,
        send_input_fn: Callable[[str, str], bool] = terminal_service.send_input,
        send_special_key_fn: Callable[[str, str], bool] = terminal_service.send_special_key,
        delete_terminal_fn: Callable[[str], bool] = terminal_service.delete_terminal,
        resolve_provider_fn: Callable[[str, str], str] = resolve_provider,
        terminal_exists_fn: Optional[Callable[[str], bool]] = None,
        terminal_log_dir: Path = TERMINAL_LOG_DIR,
        now_fn: Optional[Callable[[], datetime]] = None,
    ):
        self._runtime = runtime
        self._store = runtime.store
        self._create_session = create_session_fn
        self._send_input = send_input_fn
        self._send_special_key = send_special_key_fn
        self._delete_terminal = delete_terminal_fn
        self._resolve_provider = resolve_provider_fn
        self._terminal_exists = terminal_exists_fn or self._default_terminal_exists
        self._terminal_log_dir = terminal_log_dir
        self._now_fn = now_fn or (lambda: datetime.now(timezone.utc))

    def reconcile_startup(self) -> None:
        """Reconcile non-terminal runs after process startup/restart.

        Startup reconciliation is responsible for:
        - detecting running attempts whose worker terminal disappeared,
        - ingesting pending callback markers from surviving worker logs, and
        - running one maintenance pass so scheduling/timeouts are re-evaluated.
        """

        for run in self._store.list_runs(statuses=[RunStatus.RUNNING, RunStatus.IDLE]):
            self._reconcile_run_attempts(run_id=run.run_id, ingest_logs=True)
            self._run_maintenance(run_id=run.run_id)

    def reap(self) -> None:
        """Run one reaper pass over terminal runs/orphaned worker links."""

        for run in self._store.list_runs(
            statuses=[
                RunStatus.FINALIZED,
                RunStatus.CANCELLED,
                RunStatus.TIMED_OUT,
                RunStatus.FAILED,
            ]
        ):
            self._reap_terminal_run(run=run)

    def start(self, request: OrchestrationStartRequest) -> OrchestrationStartResponse:
        """Create a run and optional initial jobs, then schedule immediately."""

        now = self._now()
        policy = self._policy_from_start_request(request.policy)
        run_id = self._new_id(prefix="run")

        metadata = dict(request.metadata or {})
        metadata["orchestration"] = {
            "policy": self._policy_to_dict(policy),
            "run_started_at": now.isoformat(),
        }

        self._store.create_run(
            run_id=run_id,
            name=request.name,
            status=RunStatus.RUNNING,
            metadata=metadata,
            created_at=now,
        )

        self._append_event(
            run_id=run_id,
            event_type=EventType.RUN_CREATED,
            payload=OrchestrationEventPayload(
                run=RunStatePayload(status=RunStatus.RUNNING),
                metadata={"name": request.name} if request.name else None,
            ),
            dedupe_key=f"run-created:{run_id}",
        )

        job_ids: List[str] = []
        for job_spec in request.jobs:
            job, _ = self._create_queued_job_with_attempt(
                run_id=run_id,
                job_spec=job_spec,
                default_timeout_sec=policy.default_job_timeout_sec,
            )
            job_ids.append(job.job_id)

        self._run_maintenance(run_id=run_id)
        run = self._require_run(run_id)
        return OrchestrationStartResponse(run_id=run_id, job_ids=job_ids, status=run.status)

    def spawn(self, request: OrchestrationSpawnRequest) -> OrchestrationSpawnResponse:
        """Queue one new job under an existing run and attempt to schedule it."""

        run = self._require_run(request.run_id)
        if self._is_run_terminal(run):
            raise ValueError(f"run {request.run_id} is not accepting new jobs")

        policy = self._effective_policy(run)

        if self._status_value(run.status) == RunStatus.IDLE.value:
            self._set_run_status(
                run=run,
                new_status=RunStatus.RUNNING,
                reason="job_queued",
            )
            run = self._require_run(request.run_id)

        job_spec = OrchestrationJobSpec(
            agent_profile=request.agent_profile,
            message=request.message,
            parent_job_id=request.parent_job_id,
            chain_id=request.chain_id,
            role=request.role,
            kind=request.kind,
            timeout_sec=request.timeout_sec,
            idempotency_key=request.idempotency_key,
            metadata=request.metadata,
        )

        job, attempt = self._create_queued_job_with_attempt(
            run_id=request.run_id,
            job_spec=job_spec,
            default_timeout_sec=policy.default_job_timeout_sec,
        )

        self._run_maintenance(run_id=request.run_id)

        job = self._require_job(job.job_id)
        attempt = self._require_attempt(attempt.attempt_id)
        return OrchestrationSpawnResponse(
            run_id=request.run_id,
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            status=job.status,
            terminal_id=attempt.terminal_id,
        )

    async def wait(self, request: OrchestrationWaitRequest) -> OrchestrationWaitResponse:
        """Bounded long-poll wait over append-only orchestration events."""

        run = self._require_run(request.run_id)
        cursor = request.cursor or 0
        deadline = time.monotonic() + float(request.wait_timeout_sec)

        self._run_maintenance(run_id=request.run_id)

        while True:
            run = self._require_run(request.run_id)
            events = self._store.read_events(
                run_id=request.run_id,
                cursor=cursor,
                max_events=request.max_events,
                event_types=request.event_types,
                job_ids=request.job_ids,
            )

            if len(events) >= request.min_events:
                return self._build_wait_response(
                    run=run,
                    cursor=cursor,
                    events=events,
                    timeout=False,
                    include_snapshot=request.include_snapshot,
                )

            if self._is_run_terminal(run):
                return self._build_wait_response(
                    run=run,
                    cursor=cursor,
                    events=events,
                    timeout=False,
                    include_snapshot=request.include_snapshot,
                )

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return self._build_wait_response(
                    run=run,
                    cursor=cursor,
                    events=events,
                    timeout=True,
                    include_snapshot=request.include_snapshot,
                )

            signal_cursor = self._runtime.current_signal_cursor(run_id=request.run_id)
            timeout_sec = max(1, int(math.ceil(remaining)))
            await self._runtime.wait_for_run_update(
                run_id=request.run_id,
                cursor=signal_cursor,
                timeout_sec=timeout_sec,
            )
            self._run_maintenance(run_id=request.run_id)

    def status(self, request: OrchestrationStatusRequest) -> OrchestrationStatusResponse:
        """Return non-blocking run/job/attempt/event snapshot state."""

        self._run_maintenance(run_id=request.run_id)

        run = self._require_run(request.run_id)
        snapshot = self._store.get_run_snapshot(run_id=request.run_id)
        if snapshot is None:
            raise ValueError(f"run {request.run_id} not found")

        include_attempts = request.include_jobs or request.include_terminal_refs
        jobs = self._store.list_jobs(run_id=request.run_id) if request.include_jobs else None
        attempts = self._store.list_attempts(run_id=request.run_id) if include_attempts else None
        terminal_refs: Optional[List[AttemptTerminalRef]] = None
        if request.include_terminal_refs:
            terminal_refs = self._build_terminal_refs(run_id=request.run_id)

        events: Optional[List[OrchestrationEvent]] = None
        if request.include_events_since is not None:
            events = self._store.read_events(
                run_id=request.run_id,
                cursor=request.include_events_since,
                max_events=1000,
            )

        return OrchestrationStatusResponse(
            run=run,
            snapshot=snapshot,
            jobs=jobs,
            attempts=attempts,
            terminal_refs=terminal_refs,
            events=events,
        )

    def cancel(self, request: OrchestrationCancelRequest) -> OrchestrationCancelResponse:
        """Cancel active/queued jobs for run/job scopes."""

        run = self._require_run(request.run_id)
        if self._is_run_terminal(run) and request.scope == CancelScope.RUN:
            return OrchestrationCancelResponse()

        cancelled_jobs: List[str] = []
        cancelled_attempts: List[str] = []
        emitted_events: List[OrchestrationEvent] = []

        if request.scope == CancelScope.RUN:
            jobs = self._store.list_jobs(run_id=request.run_id)
            for job in jobs:
                if self._status_value(job.status) in self._JOB_TERMINAL_STATUSES:
                    continue
                job_cancelled, attempt_cancelled, events = self._cancel_job(
                    run_id=request.run_id,
                    job=job,
                    reason=request.reason or "run_cancelled",
                )
                if job_cancelled:
                    cancelled_jobs.append(job.job_id)
                cancelled_attempts.extend(attempt_cancelled)
                emitted_events.extend(events)

            run = self._require_run(request.run_id)
            if not self._is_run_terminal(run):
                payload = OrchestrationEventPayload(
                    cancellation=CancellationPayload(
                        scope=CancelScope.RUN,
                        reason=request.reason,
                        cancelled_jobs=cancelled_jobs,
                        cancelled_attempts=cancelled_attempts,
                    ),
                    run=RunStatePayload(
                        status=RunStatus.CANCELLED,
                        previous_status=run.status,
                        reason=request.reason,
                    ),
                )
                emitted_events.append(
                    self._append_event(
                        run_id=request.run_id,
                        event_type=EventType.RUN_CANCELLED,
                        payload=payload,
                        dedupe_key=f"run-cancelled:{request.run_id}:{','.join(cancelled_jobs)}",
                    )
                )
                emitted_events.append(
                    self._append_event(
                        run_id=request.run_id,
                        event_type=EventType.RUN_STATUS_CHANGED,
                        payload=OrchestrationEventPayload(
                            run=RunStatePayload(
                                status=RunStatus.CANCELLED,
                                previous_status=run.status,
                                reason=request.reason,
                            )
                        ),
                    )
                )
                self._store.update_run(run_id=request.run_id, status=RunStatus.CANCELLED)

        elif request.scope == CancelScope.JOB:
            if not request.job_id:
                raise ValueError("job_id is required when scope=job")
            job = self._require_job(request.job_id)
            if job.run_id != request.run_id:
                raise ValueError(f"job {request.job_id} does not belong to run {request.run_id}")

            if self._status_value(job.status) not in self._JOB_TERMINAL_STATUSES:
                job_cancelled, attempt_cancelled, events = self._cancel_job(
                    run_id=request.run_id,
                    job=job,
                    reason=request.reason or "job_cancelled",
                )
                if job_cancelled:
                    cancelled_jobs.append(job.job_id)
                cancelled_attempts.extend(attempt_cancelled)
                emitted_events.extend(events)

        else:
            raise ValueError("scope=attempt is not supported in this phase")

        self._run_maintenance(run_id=request.run_id)

        return OrchestrationCancelResponse(
            cancelled_jobs=cancelled_jobs,
            cancelled_attempts=cancelled_attempts,
            events=emitted_events,
        )

    def finalize(self, request: OrchestrationFinalizeRequest) -> OrchestrationFinalizeResponse:
        """Explicitly finalize a run and apply cleanup policy."""

        # Finalization should not schedule new work. Run only timeout/link-idle reconciliation.
        self._reconcile_timeouts(run_id=request.run_id)
        self._release_terminal_links_for_completed_attempts(run_id=request.run_id)
        self._emit_run_idle_if_drained(run_id=request.run_id)
        run = self._require_run(request.run_id)
        now = self._now()
        cleanup_policy = request.cleanup_policy or CleanupPolicy()

        if self._status_value(run.status) == RunStatus.FINALIZED.value:
            finalization = {}
            metadata = run.metadata or {}
            orchestration = metadata.get("orchestration")
            if isinstance(orchestration, dict):
                maybe_finalization = orchestration.get("finalization")
                if isinstance(maybe_finalization, dict):
                    finalization = maybe_finalization

            outcome = finalization.get("outcome") or request.outcome
            summary = finalization["summary"] if "summary" in finalization else request.summary
            cleanup_policy_data = finalization.get("cleanup_policy")
            if isinstance(cleanup_policy_data, dict):
                try:
                    cleanup_policy = CleanupPolicy.model_validate(cleanup_policy_data)
                except Exception:
                    logger.debug(
                        "Ignoring invalid stored finalization cleanup policy for run %s",
                        request.run_id,
                        exc_info=True,
                    )

            return OrchestrationFinalizeResponse(
                run_id=request.run_id,
                status=RunStatus.FINALIZED,
                outcome=outcome,
                finalized_at=run.finalized_at or now,
                summary=summary,
                cleanup_policy=cleanup_policy,
            )

        snapshot = self._store.get_run_snapshot(run_id=request.run_id)
        if snapshot is None:
            raise ValueError(f"run {request.run_id} not found")
        if snapshot.active_job_ids:
            raise ValueError(
                f"run {request.run_id} has active jobs and cannot be finalized: "
                f"{', '.join(snapshot.active_job_ids)}"
            )

        self._apply_finalization_cleanup(run_id=request.run_id, cleanup_policy=cleanup_policy)

        updated_run = self._store.update_run(
            run_id=request.run_id,
            status=RunStatus.FINALIZED,
            finalized_at=now,
            metadata=self._merged_run_metadata(
                run=run,
                merge={
                    "orchestration": {
                        "finalization": {
                            "outcome": request.outcome,
                            "summary": request.summary,
                            "cleanup_policy": cleanup_policy.model_dump(mode="json"),
                            "finalized_at": now.isoformat(),
                        }
                    }
                },
            ),
        )
        if updated_run is None:
            raise ValueError(f"run {request.run_id} not found")

        finalization_payload = OrchestrationEventPayload(
            finalization=FinalizationPayload(
                outcome=request.outcome,
                summary=request.summary,
                cleanup_policy=cleanup_policy,
            ),
            run=RunStatePayload(
                status=RunStatus.FINALIZED,
                previous_status=run.status,
                reason=request.summary,
            ),
        )

        self._append_event(
            run_id=request.run_id,
            event_type=EventType.RUN_FINALIZED,
            payload=finalization_payload,
            dedupe_key=f"run-finalized:{request.run_id}",
        )
        self._append_event(
            run_id=request.run_id,
            event_type=EventType.RUN_STATUS_CHANGED,
            payload=OrchestrationEventPayload(
                run=RunStatePayload(
                    status=RunStatus.FINALIZED,
                    previous_status=run.status,
                    reason=request.summary,
                )
            ),
            dedupe_key=f"run-status-finalized:{request.run_id}",
        )

        return OrchestrationFinalizeResponse(
            run_id=request.run_id,
            status=RunStatus.FINALIZED,
            outcome=request.outcome,
            finalized_at=now,
            summary=request.summary,
            cleanup_policy=cleanup_policy,
        )

    def _run_maintenance(self, *, run_id: str) -> None:
        """Run scheduler, timeout checks, and run-idle reconciliation."""

        run = self._store.get_run(run_id=run_id)
        if run is None:
            raise ValueError(f"run {run_id} not found")

        if self._is_run_terminal(run):
            self._reap_terminal_run(run=run)
            return

        # Reconcile active attempts continuously so exited worker terminals do not strand runs.
        self._reconcile_run_attempts(run_id=run_id, ingest_logs=True)
        self._reconcile_timeouts(run_id=run_id)
        self._release_terminal_links_for_completed_attempts(run_id=run_id)
        self._schedule_run(run_id=run_id)
        self._emit_run_idle_if_drained(run_id=run_id)

    def _schedule_run(self, *, run_id: str) -> None:
        run = self._store.get_run(run_id=run_id)
        if run is None:
            raise ValueError(f"run {run_id} not found")
        if self._is_run_terminal(run):
            return

        policy = self._effective_policy(run)
        jobs = self._store.list_jobs(run_id=run_id)
        attempts = self._store.list_attempts(run_id=run_id)
        latest_attempt_by_job = self._latest_attempt_by_job(attempts)

        running_jobs = [
            job for job in jobs if self._status_value(job.status) == JobStatus.RUNNING.value
        ]
        running_total = len(running_jobs)
        running_reviewers = sum(1 for job in running_jobs if self._is_reviewer(job))
        running_workers = running_total - running_reviewers

        queued_jobs = [
            job for job in jobs if self._status_value(job.status) == JobStatus.QUEUED.value
        ]

        for job in queued_jobs:
            if running_total >= policy.max_total_terminals:
                break

            is_reviewer = self._is_reviewer(job)
            if is_reviewer and running_reviewers >= policy.max_active_reviewers_per_run:
                continue
            if (not is_reviewer) and running_workers >= policy.max_active_workers_per_run:
                continue

            attempt = latest_attempt_by_job.get(job.job_id)
            if (
                attempt is None
                or self._status_value(attempt.status) in self._ATTEMPT_TERMINAL_STATUSES
            ):
                attempt = self._store.create_attempt(
                    attempt_id=self._new_id(prefix="attempt"),
                    run_id=run_id,
                    job_id=job.job_id,
                    status=AttemptStatus.CREATED,
                )
                latest_attempt_by_job[job.job_id] = attempt

            if self._status_value(attempt.status) != AttemptStatus.CREATED.value:
                continue

            started = self._start_attempt(run=run, job=job, attempt=attempt)
            if not started:
                continue

            running_total += 1
            if is_reviewer:
                running_reviewers += 1
            else:
                running_workers += 1

    def _start_attempt(self, *, run: RunRecord, job: JobRecord, attempt: AttemptRecord) -> bool:
        now = self._now()
        created_terminal_id: Optional[str] = None

        self._store.update_attempt(
            attempt_id=attempt.attempt_id,
            status=AttemptStatus.RUNNING,
            started_at=now,
        )
        self._store.update_job(job_id=job.job_id, status=JobStatus.RUNNING)

        try:
            provider = self._resolve_provider(job.agent_profile, DEFAULT_PROVIDER)
            terminal = self._create_session(
                provider=provider,
                agent_profile=job.agent_profile,
                session_name=self._orchestration_session_name(
                    run_id=run.run_id,
                    job_id=job.job_id,
                    attempt_id=attempt.attempt_id,
                ),
                orchestration_run_id=run.run_id,
                orchestration_job_id=job.job_id,
                orchestration_attempt_id=attempt.attempt_id,
                orchestration_chain_id=job.chain_id,
            )
            created_terminal_id = terminal.id

            self._store.update_attempt(attempt_id=attempt.attempt_id, terminal_id=terminal.id)
            self._store.link_worker_terminal(
                terminal_id=terminal.id,
                run_id=run.run_id,
                job_id=job.job_id,
                attempt_id=attempt.attempt_id,
                provider=provider,
                tmux_session=terminal.session_name,
                tmux_window=terminal.name,
            )
            sent = self._send_input(terminal.id, job.message)
            if not sent:
                raise RuntimeError("terminal input delivery returned False")

            self._append_event(
                run_id=run.run_id,
                job_id=job.job_id,
                attempt_id=attempt.attempt_id,
                event_type=EventType.ATTEMPT_STARTED,
                payload=OrchestrationEventPayload(
                    attempt=AttemptStatePayload(
                        status=AttemptStatus.RUNNING,
                        terminal_id=terminal.id,
                    )
                ),
            )
            self._append_event(
                run_id=run.run_id,
                job_id=job.job_id,
                attempt_id=attempt.attempt_id,
                event_type=EventType.JOB_STARTED,
                payload=OrchestrationEventPayload(
                    job=JobStatePayload(
                        status=JobStatus.RUNNING,
                        role=job.role,
                        kind=job.kind,
                        parent_job_id=job.parent_job_id,
                        chain_id=job.chain_id,
                        terminal_id=terminal.id,
                    )
                ),
            )
            return True
        except Exception as exc:
            if created_terminal_id:
                self._terminate_worker_terminal(terminal_id=created_terminal_id)

            reason = f"terminal_spawn_failed:{exc.__class__.__name__}"
            self._store.update_attempt(
                attempt_id=attempt.attempt_id,
                status=AttemptStatus.FAILED,
                completed_at=now,
                result_summary=str(exc)[:500],
                result_data={"reason": reason},
            )
            self._store.update_job(job_id=job.job_id, status=JobStatus.FAILED)
            self._append_event(
                run_id=run.run_id,
                job_id=job.job_id,
                attempt_id=attempt.attempt_id,
                event_type=EventType.ATTEMPT_FAILED,
                payload=OrchestrationEventPayload(
                    attempt=AttemptStatePayload(
                        status=AttemptStatus.FAILED,
                        result_summary=str(exc)[:500],
                        result_data={"reason": reason},
                    )
                ),
            )
            self._append_event(
                run_id=run.run_id,
                job_id=job.job_id,
                attempt_id=attempt.attempt_id,
                event_type=EventType.JOB_FAILED,
                payload=OrchestrationEventPayload(
                    job=JobStatePayload(
                        status=JobStatus.FAILED,
                        role=job.role,
                        kind=job.kind,
                        parent_job_id=job.parent_job_id,
                        chain_id=job.chain_id,
                        reason=reason,
                    )
                ),
            )
            logger.warning(
                "Failed to spawn orchestration attempt %s for job %s: %s",
                attempt.attempt_id,
                job.job_id,
                exc,
            )
            return False

    def _create_queued_job_with_attempt(
        self,
        *,
        run_id: str,
        job_spec: OrchestrationJobSpec,
        default_timeout_sec: int,
    ) -> Tuple[JobRecord, AttemptRecord]:
        existing_job: Optional[JobRecord] = None
        if job_spec.idempotency_key:
            existing_job = self._store.get_job_by_idempotency_key(
                run_id=run_id,
                idempotency_key=job_spec.idempotency_key,
            )

        if existing_job is not None:
            attempts = self._store.list_attempts(job_id=existing_job.job_id)
            if attempts:
                return existing_job, attempts[-1]
            new_attempt = self._store.create_attempt(
                attempt_id=self._new_id(prefix="attempt"),
                run_id=run_id,
                job_id=existing_job.job_id,
                status=AttemptStatus.CREATED,
            )
            return existing_job, new_attempt

        job_id = self._new_id(prefix="job")
        timeout_sec = job_spec.timeout_sec or default_timeout_sec

        job = self._store.create_job(
            job_id=job_id,
            run_id=run_id,
            agent_profile=job_spec.agent_profile,
            message=job_spec.message,
            status=JobStatus.CREATED,
            role=job_spec.role,
            kind=job_spec.kind,
            parent_job_id=job_spec.parent_job_id,
            chain_id=job_spec.chain_id,
            timeout_sec=timeout_sec,
            idempotency_key=job_spec.idempotency_key,
            metadata=job_spec.metadata,
        )

        self._append_event(
            run_id=run_id,
            job_id=job.job_id,
            event_type=EventType.JOB_CREATED,
            payload=OrchestrationEventPayload(
                job=JobStatePayload(
                    status=JobStatus.CREATED,
                    role=job.role,
                    kind=job.kind,
                    parent_job_id=job.parent_job_id,
                    chain_id=job.chain_id,
                )
            ),
            dedupe_key=f"job-created:{run_id}:{job.job_id}",
        )

        self._store.update_job(job_id=job.job_id, status=JobStatus.QUEUED)
        self._append_event(
            run_id=run_id,
            job_id=job.job_id,
            event_type=EventType.JOB_QUEUED,
            payload=OrchestrationEventPayload(
                job=JobStatePayload(
                    status=JobStatus.QUEUED,
                    role=job.role,
                    kind=job.kind,
                    parent_job_id=job.parent_job_id,
                    chain_id=job.chain_id,
                )
            ),
            dedupe_key=f"job-queued:{run_id}:{job.job_id}",
        )

        attempt = self._store.create_attempt(
            attempt_id=self._new_id(prefix="attempt"),
            run_id=run_id,
            job_id=job.job_id,
            status=AttemptStatus.CREATED,
        )

        return self._require_job(job.job_id), attempt

    def _reconcile_run_attempts(self, *, run_id: str, ingest_logs: bool) -> None:
        jobs = self._store.list_jobs(run_id=run_id)
        latest_attempts = self._latest_attempt_by_job(self._store.list_attempts(run_id=run_id))

        for job in jobs:
            if self._status_value(job.status) != JobStatus.RUNNING.value:
                continue

            attempt = latest_attempts.get(job.job_id)
            if attempt is None:
                self._store.update_job(job_id=job.job_id, status=JobStatus.FAILED)
                self._append_event(
                    run_id=run_id,
                    job_id=job.job_id,
                    event_type=EventType.JOB_FAILED,
                    payload=OrchestrationEventPayload(
                        job=JobStatePayload(
                            status=JobStatus.FAILED,
                            role=job.role,
                            kind=job.kind,
                            parent_job_id=job.parent_job_id,
                            chain_id=job.chain_id,
                            reason="attempt_missing",
                        )
                    ),
                    dedupe_key=f"reconcile-job-failed:{run_id}:{job.job_id}:attempt-missing",
                )
                continue

            if self._status_value(attempt.status) != AttemptStatus.RUNNING.value:
                continue

            terminal_id = attempt.terminal_id
            if ingest_logs and terminal_id:
                self._ingest_terminal_log(terminal_id=terminal_id)
                refreshed_attempt = self._store.get_attempt(attempt_id=attempt.attempt_id)
                refreshed_job = self._store.get_job(job_id=job.job_id)
                if refreshed_attempt is not None:
                    attempt = refreshed_attempt
                if refreshed_job is not None:
                    job = refreshed_job

            if self._status_value(job.status) != JobStatus.RUNNING.value:
                continue
            if self._status_value(attempt.status) != AttemptStatus.RUNNING.value:
                continue

            terminal_id = attempt.terminal_id
            if terminal_id and self._terminal_exists(terminal_id):
                continue

            # If the worker terminal is gone, perform one full-log recovery pass
            # before failing the attempt. This recovers markers that were missed by
            # prior incremental parsing/offset state.
            if ingest_logs and terminal_id:
                self._ingest_terminal_log(terminal_id=terminal_id, full_scan=True)
                refreshed_attempt = self._store.get_attempt(attempt_id=attempt.attempt_id)
                refreshed_job = self._store.get_job(job_id=job.job_id)
                if refreshed_attempt is not None:
                    attempt = refreshed_attempt
                if refreshed_job is not None:
                    job = refreshed_job

            if self._status_value(job.status) != JobStatus.RUNNING.value:
                continue
            if self._status_value(attempt.status) != AttemptStatus.RUNNING.value:
                continue

            self._mark_attempt_terminal_missing(run_id=run_id, job=job, attempt=attempt)

    def _mark_attempt_terminal_missing(
        self, *, run_id: str, job: JobRecord, attempt: AttemptRecord
    ) -> None:
        reason = "terminal_missing"
        self._store.update_attempt(
            attempt_id=attempt.attempt_id,
            status=AttemptStatus.FAILED,
            completed_at=self._now(),
            result_summary=reason,
            result_data={"reason": reason},
        )
        self._store.update_job(job_id=job.job_id, status=JobStatus.FAILED)
        if attempt.terminal_id:
            self._store.release_worker_terminal(terminal_id=attempt.terminal_id)

        self._append_event(
            run_id=run_id,
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            event_type=EventType.ATTEMPT_FAILED,
            payload=OrchestrationEventPayload(
                attempt=AttemptStatePayload(
                    status=AttemptStatus.FAILED,
                    terminal_id=attempt.terminal_id,
                    result_summary=reason,
                    result_data={"reason": reason},
                )
            ),
            dedupe_key=f"reconcile-attempt-failed:{run_id}:{attempt.attempt_id}:{reason}",
        )
        self._append_event(
            run_id=run_id,
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            event_type=EventType.JOB_FAILED,
            payload=OrchestrationEventPayload(
                job=JobStatePayload(
                    status=JobStatus.FAILED,
                    role=job.role,
                    kind=job.kind,
                    parent_job_id=job.parent_job_id,
                    chain_id=job.chain_id,
                    terminal_id=attempt.terminal_id,
                    reason=reason,
                )
            ),
            dedupe_key=f"reconcile-job-failed:{run_id}:{job.job_id}:{attempt.attempt_id}",
        )

    def _ingest_terminal_log(self, *, terminal_id: str, full_scan: bool = False) -> None:
        log_path = self._terminal_log_dir / f"{terminal_id}.log"
        if not log_path.exists():
            return
        try:
            if full_scan:
                log_text = log_path.read_text(encoding="utf-8", errors="ignore")
                if not log_text:
                    return
                result = self._runtime.callback_ingestor.ingest_terminal_output(
                    terminal_id=terminal_id,
                    output=log_text,
                )
                for run_id in result.affected_run_ids:
                    self._runtime.notify_run_update(run_id=run_id)
            else:
                result = self._runtime.ingest_log_update(terminal_id=terminal_id, log_path=log_path)
            if result.ingestion_failures:
                logger.warning(
                    "Orchestration log ingestion saw marker ingestion errors for %s",
                    terminal_id,
                )
        except Exception:
            logger.warning(
                "Failed to ingest orchestration marker log during reconciliation for terminal %s",
                terminal_id,
                exc_info=True,
            )

    def _reconcile_timeouts(self, *, run_id: str) -> None:
        run = self._store.get_run(run_id=run_id)
        if run is None:
            raise ValueError(f"run {run_id} not found")
        if self._is_run_terminal(run):
            return

        policy = self._effective_policy(run)
        now = self._now()

        run_started_at = self._run_started_at(run)
        if (now - run_started_at).total_seconds() > float(policy.run_timeout_sec):
            self._handle_run_timeout(run=run)
            return

        jobs = self._store.list_jobs(run_id=run_id)
        attempts_by_job = self._latest_attempt_by_job(self._store.list_attempts(run_id=run_id))
        for job in jobs:
            if self._status_value(job.status) != JobStatus.RUNNING.value:
                continue

            attempt = attempts_by_job.get(job.job_id)
            if attempt is None:
                continue

            job_timeout = job.timeout_sec or policy.default_job_timeout_sec
            started_at = attempt.started_at or attempt.created_at
            if (now - started_at).total_seconds() <= float(job_timeout):
                continue

            self._timeout_running_job(run_id=run_id, job=job, attempt=attempt)

    def _handle_run_timeout(self, *, run: RunRecord) -> None:
        run_id = run.run_id
        jobs = self._store.list_jobs(run_id=run_id)

        timed_out_jobs: List[str] = []
        cancelled_jobs: List[str] = []
        cancelled_attempts: List[str] = []

        attempts_by_job = self._latest_attempt_by_job(self._store.list_attempts(run_id=run_id))

        for job in jobs:
            status = self._status_value(job.status)
            if status in self._JOB_TERMINAL_STATUSES:
                continue

            if status == JobStatus.RUNNING.value:
                attempt = attempts_by_job.get(job.job_id)
                if attempt is not None:
                    self._timeout_running_job(run_id=run_id, job=job, attempt=attempt)
                    timed_out_jobs.append(job.job_id)
            else:
                job_cancelled, attempt_cancelled, _ = self._cancel_job(
                    run_id=run_id,
                    job=job,
                    reason="run_timeout",
                )
                if job_cancelled:
                    cancelled_jobs.append(job.job_id)
                cancelled_attempts.extend(attempt_cancelled)

        latest_run = self._require_run(run_id)
        if self._is_run_terminal(latest_run):
            return

        self._store.update_run(run_id=run_id, status=RunStatus.TIMED_OUT)
        payload = OrchestrationEventPayload(
            run=RunStatePayload(
                status=RunStatus.TIMED_OUT,
                previous_status=latest_run.status,
                reason="run_timeout",
            ),
            cancellation=CancellationPayload(
                scope=CancelScope.RUN,
                reason="run_timeout",
                cancelled_jobs=[*timed_out_jobs, *cancelled_jobs],
                cancelled_attempts=cancelled_attempts,
            ),
        )
        self._append_event(
            run_id=run_id,
            event_type=EventType.RUN_TIMED_OUT,
            payload=payload,
            dedupe_key=f"run-timeout:{run_id}",
        )
        self._append_event(
            run_id=run_id,
            event_type=EventType.RUN_STATUS_CHANGED,
            payload=OrchestrationEventPayload(
                run=RunStatePayload(
                    status=RunStatus.TIMED_OUT,
                    previous_status=latest_run.status,
                    reason="run_timeout",
                )
            ),
        )

    def _timeout_running_job(self, *, run_id: str, job: JobRecord, attempt: AttemptRecord) -> None:
        terminal_id = attempt.terminal_id
        if terminal_id:
            self._terminate_worker_terminal(terminal_id=terminal_id)

        now = self._now()
        self._store.update_attempt(
            attempt_id=attempt.attempt_id,
            status=AttemptStatus.TIMED_OUT,
            completed_at=now,
            result_summary="job_timeout",
            result_data={"reason": "job_timeout"},
        )
        self._store.update_job(job_id=job.job_id, status=JobStatus.TIMED_OUT)

        self._append_event(
            run_id=run_id,
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            event_type=EventType.ATTEMPT_TIMED_OUT,
            payload=OrchestrationEventPayload(
                attempt=AttemptStatePayload(
                    status=AttemptStatus.TIMED_OUT,
                    terminal_id=terminal_id,
                    result_summary="job_timeout",
                    result_data={"reason": "job_timeout"},
                )
            ),
            dedupe_key=f"attempt-timeout:{run_id}:{attempt.attempt_id}",
        )
        self._append_event(
            run_id=run_id,
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            event_type=EventType.JOB_TIMED_OUT,
            payload=OrchestrationEventPayload(
                job=JobStatePayload(
                    status=JobStatus.TIMED_OUT,
                    role=job.role,
                    kind=job.kind,
                    parent_job_id=job.parent_job_id,
                    chain_id=job.chain_id,
                    terminal_id=terminal_id,
                    reason="job_timeout",
                )
            ),
            dedupe_key=f"job-timeout:{run_id}:{job.job_id}:{attempt.attempt_id}",
        )

    def _cancel_job(
        self,
        *,
        run_id: str,
        job: JobRecord,
        reason: str,
    ) -> Tuple[bool, List[str], List[OrchestrationEvent]]:
        attempts = self._store.list_attempts(job_id=job.job_id)
        latest_attempt = attempts[-1] if attempts else None

        cancelled_attempt_ids: List[str] = []
        emitted_events: List[OrchestrationEvent] = []

        if (
            latest_attempt
            and self._status_value(latest_attempt.status) not in self._ATTEMPT_TERMINAL_STATUSES
        ):
            terminal_id = latest_attempt.terminal_id
            if terminal_id:
                self._terminate_worker_terminal(terminal_id=terminal_id)

            self._store.update_attempt(
                attempt_id=latest_attempt.attempt_id,
                status=AttemptStatus.CANCELLED,
                completed_at=self._now(),
                result_summary=reason[:500],
                result_data={"reason": reason},
            )
            cancelled_attempt_ids.append(latest_attempt.attempt_id)

            emitted_events.append(
                self._append_event(
                    run_id=run_id,
                    job_id=job.job_id,
                    attempt_id=latest_attempt.attempt_id,
                    event_type=EventType.ATTEMPT_CANCELLED,
                    payload=OrchestrationEventPayload(
                        attempt=AttemptStatePayload(
                            status=AttemptStatus.CANCELLED,
                            terminal_id=terminal_id,
                            result_summary=reason[:500],
                            result_data={"reason": reason},
                        )
                    ),
                    dedupe_key=f"attempt-cancelled:{run_id}:{latest_attempt.attempt_id}",
                )
            )

        self._store.update_job(job_id=job.job_id, status=JobStatus.CANCELLED)
        emitted_events.append(
            self._append_event(
                run_id=run_id,
                job_id=job.job_id,
                attempt_id=latest_attempt.attempt_id if latest_attempt else None,
                event_type=EventType.JOB_CANCELLED,
                payload=OrchestrationEventPayload(
                    job=JobStatePayload(
                        status=JobStatus.CANCELLED,
                        role=job.role,
                        kind=job.kind,
                        parent_job_id=job.parent_job_id,
                        chain_id=job.chain_id,
                        terminal_id=latest_attempt.terminal_id if latest_attempt else None,
                        reason=reason,
                    )
                ),
                dedupe_key=f"job-cancelled:{run_id}:{job.job_id}",
            )
        )

        return True, cancelled_attempt_ids, emitted_events

    def _emit_run_idle_if_drained(self, *, run_id: str) -> None:
        run = self._store.get_run(run_id=run_id)
        if run is None:
            raise ValueError(f"run {run_id} not found")
        if self._status_value(run.status) != RunStatus.RUNNING.value:
            return

        snapshot = self._store.get_run_snapshot(run_id=run_id)
        if snapshot is None:
            raise ValueError(f"run {run_id} not found")
        if snapshot.active_job_ids:
            return

        self._store.update_run(run_id=run_id, status=RunStatus.IDLE)
        self._append_event(
            run_id=run_id,
            event_type=EventType.RUN_IDLE,
            payload=OrchestrationEventPayload(
                run=RunStatePayload(
                    status=RunStatus.IDLE,
                    previous_status=RunStatus.RUNNING,
                )
            ),
            dedupe_key=f"run-idle:{run_id}:{snapshot.latest_event_cursor or 0}",
        )
        self._append_event(
            run_id=run_id,
            event_type=EventType.RUN_STATUS_CHANGED,
            payload=OrchestrationEventPayload(
                run=RunStatePayload(
                    status=RunStatus.IDLE,
                    previous_status=RunStatus.RUNNING,
                )
            ),
        )

    def _release_terminal_links_for_completed_attempts(self, *, run_id: str) -> None:
        links = self._store.list_worker_terminals(run_id=run_id, active_only=True)
        if not links:
            return

        attempts_by_id = {
            attempt.attempt_id: attempt for attempt in self._store.list_attempts(run_id=run_id)
        }
        for link in links:
            attempt = attempts_by_id.get(link["attempt_id"])
            if attempt is None:
                continue
            if self._status_value(attempt.status) in self._ATTEMPT_TERMINAL_STATUSES:
                self._store.release_worker_terminal(terminal_id=link["terminal_id"])

    def _apply_finalization_cleanup(
        self,
        *,
        run_id: str,
        cleanup_policy: CleanupPolicy,
    ) -> None:
        links = self._store.list_worker_terminals(run_id=run_id, active_only=False)
        if not links:
            return

        attempts_by_id = {
            attempt.attempt_id: attempt for attempt in self._store.list_attempts(run_id=run_id)
        }

        for link in links:
            attempt = attempts_by_id.get(link["attempt_id"])
            if attempt is None:
                continue

            is_success = self._status_value(attempt.status) == AttemptStatus.SUCCEEDED.value
            should_kill = (
                is_success
                and cleanup_policy.successful_terminals == SuccessfulTerminalCleanupPolicy.KILL
            ) or ((not is_success) and str(cleanup_policy.failed_terminals) == "kill")

            if should_kill:
                self._terminate_worker_terminal(terminal_id=link["terminal_id"])
            elif link["released_at"] is None:
                self._store.release_worker_terminal(terminal_id=link["terminal_id"])

    def _reap_terminal_run(self, *, run: RunRecord) -> None:
        links = self._store.list_worker_terminals(run_id=run.run_id, active_only=False)
        if not links:
            return

        attempts_by_id = {
            attempt.attempt_id: attempt for attempt in self._store.list_attempts(run_id=run.run_id)
        }
        cleanup_policy = self._cleanup_policy_for_run(run=run)
        finalized_at = run.finalized_at or run.updated_at
        now = self._now()

        for link in links:
            attempt = attempts_by_id.get(link["attempt_id"])
            terminal_id = link["terminal_id"]
            if attempt is None:
                if link["released_at"] is None:
                    self._store.release_worker_terminal(terminal_id=terminal_id)
                continue

            if not self._terminal_exists(terminal_id):
                if self._status_value(attempt.status) == AttemptStatus.RUNNING.value:
                    job = self._store.get_job(job_id=attempt.job_id)
                    if job is not None:
                        self._mark_attempt_terminal_missing(
                            run_id=run.run_id, job=job, attempt=attempt
                        )
                elif link["released_at"] is None:
                    self._store.release_worker_terminal(terminal_id=terminal_id)
                continue

            if self._status_value(run.status) == RunStatus.FINALIZED.value:
                if self._should_kill_finalized_terminal(
                    attempt=attempt,
                    cleanup_policy=cleanup_policy,
                    finalized_at=finalized_at,
                    now=now,
                ):
                    self._terminate_worker_terminal(terminal_id=terminal_id)
                elif link["released_at"] is None:
                    self._store.release_worker_terminal(terminal_id=terminal_id)
                continue

            # For cancelled/timed_out/failed runs, deterministically clean all worker terminals.
            self._terminate_worker_terminal(terminal_id=terminal_id)

    def _should_kill_finalized_terminal(
        self,
        *,
        attempt: AttemptRecord,
        cleanup_policy: CleanupPolicy,
        finalized_at: datetime,
        now: datetime,
    ) -> bool:
        is_success = self._status_value(attempt.status) == AttemptStatus.SUCCEEDED.value

        if is_success:
            if cleanup_policy.successful_terminals == SuccessfulTerminalCleanupPolicy.KILL:
                return True
            if (
                cleanup_policy.successful_terminals
                == SuccessfulTerminalCleanupPolicy.PRESERVE_UNTIL_TTL
            ):
                ttl_sec = cleanup_policy.ttl_sec
                if ttl_sec is None:
                    return False
                return (now - finalized_at).total_seconds() >= float(ttl_sec)
            return False

        return str(cleanup_policy.failed_terminals) == "kill"

    def _cleanup_policy_for_run(self, *, run: RunRecord) -> CleanupPolicy:
        if self._status_value(run.status) != RunStatus.FINALIZED.value:
            return CleanupPolicy(
                successful_terminals=SuccessfulTerminalCleanupPolicy.KILL,
            )

        metadata = run.metadata or {}
        orchestration = metadata.get("orchestration")
        if not isinstance(orchestration, dict):
            return CleanupPolicy()
        finalization = orchestration.get("finalization")
        if not isinstance(finalization, dict):
            return CleanupPolicy()

        raw_cleanup = finalization.get("cleanup_policy")
        if not isinstance(raw_cleanup, dict):
            return CleanupPolicy()

        try:
            return CleanupPolicy.model_validate(raw_cleanup)
        except Exception:
            logger.debug(
                "Ignoring invalid persisted cleanup policy for run %s", run.run_id, exc_info=True
            )
            return CleanupPolicy()

    def _build_wait_response(
        self,
        *,
        run: RunRecord,
        cursor: int,
        events: Sequence[OrchestrationEvent],
        timeout: bool,
        include_snapshot: bool,
    ) -> OrchestrationWaitResponse:
        new_cursor = events[-1].event_id if events else cursor
        snapshot = self._store.get_run_snapshot(run_id=run.run_id) if include_snapshot else None
        return OrchestrationWaitResponse(
            run_id=run.run_id,
            cursor=new_cursor,
            events=list(events),
            timeout=timeout,
            run_status=run.status,
            snapshot=snapshot,
        )

    def _append_event(
        self,
        *,
        run_id: str,
        event_type: EventType,
        payload: Optional[OrchestrationEventPayload] = None,
        job_id: Optional[str] = None,
        attempt_id: Optional[str] = None,
        dedupe_key: Optional[str] = None,
    ) -> OrchestrationEvent:
        event = self._store.append_event(
            run_id=run_id,
            event_type=event_type,
            payload=payload,
            job_id=job_id,
            attempt_id=attempt_id,
            dedupe_key=dedupe_key,
        )
        self._runtime.notify_run_update(run_id=run_id)
        return event

    def _set_run_status(
        self, *, run: RunRecord, new_status: RunStatus, reason: Optional[str]
    ) -> None:
        if self._status_value(run.status) == new_status.value:
            return

        self._store.update_run(run_id=run.run_id, status=new_status)
        self._append_event(
            run_id=run.run_id,
            event_type=EventType.RUN_STATUS_CHANGED,
            payload=OrchestrationEventPayload(
                run=RunStatePayload(
                    status=new_status,
                    previous_status=run.status,
                    reason=reason,
                )
            ),
        )

    def _effective_policy(self, run: RunRecord) -> _EffectivePolicy:
        metadata = run.metadata or {}
        orchestration = metadata.get("orchestration")
        policy_data = orchestration.get("policy") if isinstance(orchestration, dict) else None
        if not isinstance(policy_data, dict):
            return self._policy_from_start_request(None)

        return _EffectivePolicy(
            max_active_workers_per_run=int(
                policy_data.get(
                    "max_active_workers_per_run",
                    self.DEFAULT_MAX_ACTIVE_WORKERS_PER_RUN,
                )
            ),
            max_active_reviewers_per_run=int(
                policy_data.get(
                    "max_active_reviewers_per_run",
                    self.DEFAULT_MAX_ACTIVE_REVIEWERS_PER_RUN,
                )
            ),
            max_total_terminals=int(
                policy_data.get("max_total_terminals", self.DEFAULT_MAX_TOTAL_TERMINALS)
            ),
            default_job_timeout_sec=int(
                policy_data.get("default_job_timeout_sec", self.DEFAULT_JOB_TIMEOUT_SEC)
            ),
            run_timeout_sec=int(policy_data.get("run_timeout_sec", self.DEFAULT_RUN_TIMEOUT_SEC)),
        )

    def _policy_from_start_request(
        self, policy: Optional[OrchestrationStartPolicy]
    ) -> _EffectivePolicy:
        return _EffectivePolicy(
            max_active_workers_per_run=(
                policy.max_active_workers_per_run
                if policy and policy.max_active_workers_per_run
                else self.DEFAULT_MAX_ACTIVE_WORKERS_PER_RUN
            ),
            max_active_reviewers_per_run=(
                policy.max_active_reviewers_per_run
                if policy and policy.max_active_reviewers_per_run
                else self.DEFAULT_MAX_ACTIVE_REVIEWERS_PER_RUN
            ),
            max_total_terminals=(
                policy.max_total_terminals
                if policy and policy.max_total_terminals
                else self.DEFAULT_MAX_TOTAL_TERMINALS
            ),
            default_job_timeout_sec=(
                policy.default_job_timeout_sec
                if policy and policy.default_job_timeout_sec
                else self.DEFAULT_JOB_TIMEOUT_SEC
            ),
            run_timeout_sec=(
                policy.run_timeout_sec
                if policy and policy.run_timeout_sec
                else self.DEFAULT_RUN_TIMEOUT_SEC
            ),
        )

    @staticmethod
    def _policy_to_dict(policy: _EffectivePolicy) -> Dict[str, int]:
        return {
            "max_active_workers_per_run": policy.max_active_workers_per_run,
            "max_active_reviewers_per_run": policy.max_active_reviewers_per_run,
            "max_total_terminals": policy.max_total_terminals,
            "default_job_timeout_sec": policy.default_job_timeout_sec,
            "run_timeout_sec": policy.run_timeout_sec,
        }

    def _run_started_at(self, run: RunRecord) -> datetime:
        metadata = run.metadata or {}
        orchestration = metadata.get("orchestration")
        if isinstance(orchestration, dict):
            raw_started_at = orchestration.get("run_started_at")
            if isinstance(raw_started_at, str):
                try:
                    return datetime.fromisoformat(raw_started_at)
                except ValueError:
                    pass
        return run.created_at

    def _merged_run_metadata(self, *, run: RunRecord, merge: Dict[str, Any]) -> Dict[str, Any]:
        metadata = dict(run.metadata or {})
        for key, value in merge.items():
            if key not in metadata:
                metadata[key] = value
                continue
            existing = metadata[key]
            if isinstance(existing, dict) and isinstance(value, dict):
                merged = dict(existing)
                merged.update(value)
                metadata[key] = merged
            else:
                metadata[key] = value
        return metadata

    @staticmethod
    def _default_terminal_exists(terminal_id: str) -> bool:
        metadata = get_terminal_metadata(terminal_id)
        if not metadata:
            return False

        session_name = metadata.get("tmux_session")
        window_name = metadata.get("tmux_window")
        if not session_name or not window_name:
            return False
        if not tmux_client.session_exists(session_name):
            return False

        windows = tmux_client.get_session_windows(session_name)
        return any(window.get("name") == window_name for window in windows)

    def _terminate_worker_terminal(self, *, terminal_id: str) -> None:
        try:
            self._send_special_key(terminal_id, "C-c")
        except Exception:
            logger.debug("Failed to send interrupt to terminal %s", terminal_id, exc_info=True)

        try:
            self._delete_terminal(terminal_id)
        except Exception:
            logger.warning("Failed to kill orchestration terminal %s", terminal_id, exc_info=True)

        self._store.release_worker_terminal(terminal_id=terminal_id)

    def _latest_attempt_by_job(self, attempts: Sequence[AttemptRecord]) -> Dict[str, AttemptRecord]:
        latest: Dict[str, AttemptRecord] = {}
        for attempt in attempts:
            current = latest.get(attempt.job_id)
            if current is None or attempt.created_at >= current.created_at:
                latest[attempt.job_id] = attempt
        return latest

    def _build_terminal_refs(self, *, run_id: str) -> List[AttemptTerminalRef]:
        now = self._normalize_datetime(self._now())
        if now is None:
            raise RuntimeError("clock returned invalid timestamp")
        refs: List[AttemptTerminalRef] = []
        for row in self._store.list_attempt_terminal_refs(run_id=run_id):
            terminal_last_active_at = self._normalize_datetime(row["terminal_last_active_at"])
            last_log_activity_at = self._normalize_datetime(row["last_log_activity_at"])
            worker_terminal_released_at = self._normalize_datetime(
                row["worker_terminal_released_at"]
            )

            activity_anchor = self._latest_activity_timestamp(
                terminal_last_active_at=terminal_last_active_at,
                last_log_activity_at=last_log_activity_at,
            )
            activity_age_sec: Optional[int] = None
            if activity_anchor is not None:
                activity_age_sec = max(0, int((now - activity_anchor).total_seconds()))

            refs.append(
                AttemptTerminalRef(
                    attempt_id=row["attempt_id"],
                    job_id=row["job_id"],
                    terminal_id=row["terminal_id"],
                    terminal_present=row["terminal_present"],
                    terminal_last_active_at=terminal_last_active_at,
                    log_offset=row["log_offset"],
                    last_log_activity_at=last_log_activity_at,
                    activity_age_sec=activity_age_sec,
                    tmux_session=row["tmux_session"],
                    tmux_window=row["tmux_window"],
                    worker_terminal_released_at=worker_terminal_released_at,
                )
            )
        return refs

    @staticmethod
    def _latest_activity_timestamp(
        *,
        terminal_last_active_at: Optional[datetime],
        last_log_activity_at: Optional[datetime],
    ) -> Optional[datetime]:
        candidates = [value for value in (terminal_last_active_at, last_log_activity_at) if value]
        if not candidates:
            return None
        return max(candidates)

    @staticmethod
    def _normalize_datetime(value: Optional[datetime]) -> Optional[datetime]:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.astimezone(timezone.utc)
        return value.astimezone(timezone.utc)

    def _is_reviewer(self, job: JobRecord) -> bool:
        return (job.role or "").strip().lower() == "reviewer"

    def _is_run_terminal(self, run: RunRecord) -> bool:
        return self._status_value(run.status) in self._RUN_TERMINAL_STATUSES

    def _require_run(self, run_id: str) -> RunRecord:
        run = self._store.get_run(run_id=run_id)
        if run is None:
            raise ValueError(f"run {run_id} not found")
        return run

    def _require_job(self, job_id: str) -> JobRecord:
        job = self._store.get_job(job_id=job_id)
        if job is None:
            raise ValueError(f"job {job_id} not found")
        return job

    def _require_attempt(self, attempt_id: str) -> AttemptRecord:
        attempt = self._store.get_attempt(attempt_id=attempt_id)
        if attempt is None:
            raise ValueError(f"attempt {attempt_id} not found")
        return attempt

    @staticmethod
    def _status_value(value: Any) -> str:
        return str(value)

    def _now(self) -> datetime:
        return self._now_fn()

    @staticmethod
    def _new_id(*, prefix: str) -> str:
        return f"{prefix}-{uuid.uuid4().hex[:12]}"

    @staticmethod
    def _orchestration_session_name(*, run_id: str, job_id: str, attempt_id: str) -> str:
        def _trim(value: str) -> str:
            parts = value.split("-", 1)
            suffix = parts[1] if len(parts) == 2 else value
            return suffix[:8]

        return f"cao-orch-{_trim(run_id)}-{_trim(job_id)}-{_trim(attempt_id)}"


async def orchestration_wait(
    service: OrchestrationService, request: OrchestrationWaitRequest
) -> OrchestrationWaitResponse:
    """Compatibility helper for future endpoint/tool adapter wiring."""

    return await service.wait(request)


def orchestration_wait_sync(
    service: OrchestrationService, request: OrchestrationWaitRequest
) -> OrchestrationWaitResponse:
    """Run bounded wait from sync contexts that do not already own an event loop."""

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(service.wait(request))

    if loop.is_running():
        raise RuntimeError("orchestration_wait_sync cannot be used from a running event loop")

    return loop.run_until_complete(service.wait(request))
