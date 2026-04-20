"""Focused tests for orchestration scheduler/wait/timeout/cancel/finalize behavior."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List
from unittest.mock import patch

import pytest

from cli_agent_orchestrator.clients.database import run_schema_migrations
from cli_agent_orchestrator.clients.orchestration_store import OrchestrationStore
from cli_agent_orchestrator.models.orchestration import (
    AttemptStatus,
    CancelScope,
    CleanupPolicy,
    EventType,
    JobStatus,
    OrchestrationCancelRequest,
    OrchestrationFinalizeRequest,
    OrchestrationJobSpec,
    OrchestrationSpawnRequest,
    OrchestrationStartPolicy,
    OrchestrationStartRequest,
    OrchestrationStatusRequest,
    OrchestrationWaitRequest,
    RunOutcome,
    RunStatus,
)
from cli_agent_orchestrator.models.provider import ProviderType
from cli_agent_orchestrator.models.terminal import Terminal, TerminalStatus
from cli_agent_orchestrator.services.orchestration_callbacks import encode_worker_callback_marker
from cli_agent_orchestrator.services.orchestration_runtime import OrchestrationRuntime
from cli_agent_orchestrator.services.orchestration_service import OrchestrationService


class _Clock:
    def __init__(self, start: datetime):
        self._now = start

    def now(self) -> datetime:
        return self._now

    def advance(self, *, seconds: int) -> None:
        self._now = self._now + timedelta(seconds=seconds)


class _TerminalFactory:
    def __init__(self):
        self.calls: List[dict] = []
        self._counter = 0

    def __call__(self, **kwargs) -> Terminal:
        self.calls.append(kwargs)
        terminal_id = f"term-{self._counter}"
        self._counter += 1
        return Terminal(
            id=terminal_id,
            name=f"w-{terminal_id}",
            provider=ProviderType.CODEX,
            session_name=f"cao-{terminal_id}",
            agent_profile=kwargs.get("agent_profile"),
            allowed_tools=None,
            status=TerminalStatus.IDLE,
            last_active=datetime.now(timezone.utc),
        )


@pytest.fixture
def store(tmp_path: Path) -> OrchestrationStore:
    db_file = tmp_path / "orchestration-service.db"
    run_schema_migrations(database_file=db_file)
    return OrchestrationStore(database_file=db_file)


@pytest.fixture
def runtime(store: OrchestrationStore) -> OrchestrationRuntime:
    return OrchestrationRuntime(store=store)


def test_scheduler_enforces_worker_reviewer_and_total_caps(runtime: OrchestrationRuntime) -> None:
    factory = _TerminalFactory()
    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        resolve_provider_fn=lambda _profile, _fallback: ProviderType.CODEX.value,
    )

    response = service.start(
        OrchestrationStartRequest(
            name="cap-check",
            policy=OrchestrationStartPolicy(
                max_active_workers_per_run=1,
                max_active_reviewers_per_run=1,
                max_total_terminals=2,
            ),
            jobs=[
                OrchestrationJobSpec(
                    agent_profile="developer",
                    message="impl-1",
                    role="developer",
                ),
                OrchestrationJobSpec(
                    agent_profile="developer",
                    message="impl-2",
                    role="developer",
                ),
                OrchestrationJobSpec(
                    agent_profile="reviewer",
                    message="review-1",
                    role="reviewer",
                ),
            ],
        )
    )

    snapshot = runtime.store.get_run_snapshot(run_id=response.run_id)
    assert snapshot is not None
    assert snapshot.jobs_by_status.running == 2
    assert snapshot.jobs_by_status.queued == 1
    assert len(factory.calls) == 2
    for call in factory.calls:
        session_name = call.get("session_name")
        assert isinstance(session_name, str)
        assert session_name.startswith("cao-orch-")


def test_job_and_run_timeouts_transition_states(runtime: OrchestrationRuntime) -> None:
    factory = _TerminalFactory()
    interrupted: List[str] = []
    deleted: List[str] = []
    clock = _Clock(datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc))

    def _interrupt(terminal_id: str, _key: str) -> bool:
        interrupted.append(terminal_id)
        return True

    def _delete(terminal_id: str) -> bool:
        deleted.append(terminal_id)
        return True

    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        send_special_key_fn=_interrupt,
        delete_terminal_fn=_delete,
        resolve_provider_fn=lambda _profile, _fallback: ProviderType.CODEX.value,
        now_fn=clock.now,
    )

    start_response = service.start(
        OrchestrationStartRequest(
            name="timeouts",
            policy=OrchestrationStartPolicy(
                max_active_workers_per_run=1,
                max_total_terminals=1,
                default_job_timeout_sec=2,
                run_timeout_sec=4,
            ),
            jobs=[
                OrchestrationJobSpec(
                    agent_profile="developer",
                    message="impl-1",
                    role="developer",
                ),
                OrchestrationJobSpec(
                    agent_profile="developer",
                    message="impl-2",
                    role="developer",
                ),
            ],
        )
    )

    # First pass: running job times out, queued job is then schedulable.
    clock.advance(seconds=3)
    status_after_job_timeout = service.status(
        OrchestrationStatusRequest(run_id=start_response.run_id)
    )
    assert status_after_job_timeout.snapshot.jobs_by_status.timed_out == 1
    assert interrupted
    assert deleted

    # Second pass: run timeout cancels/times out remaining non-terminal work and marks run.
    clock.advance(seconds=3)
    status_after_run_timeout = service.status(
        OrchestrationStatusRequest(run_id=start_response.run_id)
    )
    assert status_after_run_timeout.run.status == "timed_out"

    event_types = [
        event.event_type
        for event in runtime.store.read_events(
            run_id=start_response.run_id, cursor=0, max_events=200
        )
    ]
    assert EventType.JOB_TIMED_OUT.value in event_types
    assert EventType.RUN_TIMED_OUT.value in event_types


def test_cancel_run_cancels_active_and_queued_jobs(runtime: OrchestrationRuntime) -> None:
    factory = _TerminalFactory()
    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        resolve_provider_fn=lambda _profile, _fallback: ProviderType.CODEX.value,
    )

    start_response = service.start(
        OrchestrationStartRequest(
            name="cancel-run",
            policy=OrchestrationStartPolicy(max_active_workers_per_run=1, max_total_terminals=1),
            jobs=[
                OrchestrationJobSpec(
                    agent_profile="developer",
                    message="impl-1",
                    role="developer",
                ),
                OrchestrationJobSpec(
                    agent_profile="developer",
                    message="impl-2",
                    role="developer",
                ),
            ],
        )
    )

    cancel_response = service.cancel(
        OrchestrationCancelRequest(
            run_id=start_response.run_id,
            scope=CancelScope.RUN,
            reason="operator_stop",
        )
    )

    assert len(cancel_response.cancelled_jobs) == 2

    status_response = service.status(OrchestrationStatusRequest(run_id=start_response.run_id))
    assert status_response.run.status == "cancelled"

    event_types = [event.event_type for event in cancel_response.events]
    assert EventType.RUN_CANCELLED.value in event_types
    assert EventType.JOB_CANCELLED.value in event_types


def test_run_idle_is_emitted_separately_from_finalize(runtime: OrchestrationRuntime) -> None:
    factory = _TerminalFactory()
    deleted: List[str] = []

    def _delete(terminal_id: str) -> bool:
        deleted.append(terminal_id)
        return True

    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        delete_terminal_fn=_delete,
        resolve_provider_fn=lambda _profile, _fallback: ProviderType.CODEX.value,
    )

    start_response = service.start(
        OrchestrationStartRequest(
            name="idle-finalize",
            jobs=[
                OrchestrationJobSpec(
                    agent_profile="developer",
                    message="impl",
                    role="developer",
                )
            ],
        )
    )

    running_attempt = runtime.store.list_attempts(run_id=start_response.run_id)[0]
    runtime.store.update_attempt(
        attempt_id=running_attempt.attempt_id,
        status=AttemptStatus.SUCCEEDED,
        completed_at=datetime.now(timezone.utc),
    )
    running_job = runtime.store.list_jobs(run_id=start_response.run_id)[0]
    runtime.store.update_job(job_id=running_job.job_id, status=JobStatus.SUCCEEDED)

    status_response = service.status(OrchestrationStatusRequest(run_id=start_response.run_id))
    assert status_response.run.status == "idle"

    events_before_finalize = runtime.store.read_events(
        run_id=start_response.run_id, cursor=0, max_events=200
    )
    event_types_before_finalize = [event.event_type for event in events_before_finalize]
    assert EventType.RUN_IDLE.value in event_types_before_finalize
    assert EventType.RUN_FINALIZED.value not in event_types_before_finalize

    finalize_response = service.finalize(
        OrchestrationFinalizeRequest(
            run_id=start_response.run_id,
            outcome=RunOutcome.SUCCEEDED,
            summary="done",
            cleanup_policy=CleanupPolicy(),
        )
    )

    assert finalize_response.status == "finalized"
    assert deleted

    events_after_finalize = runtime.store.read_events(
        run_id=start_response.run_id, cursor=0, max_events=200
    )
    event_types_after_finalize = [event.event_type for event in events_after_finalize]
    assert EventType.RUN_FINALIZED.value in event_types_after_finalize


def test_finalize_rejects_when_run_has_active_jobs(runtime: OrchestrationRuntime) -> None:
    factory = _TerminalFactory()
    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        resolve_provider_fn=lambda _profile, _fallback: ProviderType.CODEX.value,
    )

    start_response = service.start(
        OrchestrationStartRequest(
            name="finalize-active-run",
            jobs=[
                OrchestrationJobSpec(
                    agent_profile="developer",
                    message="work-item",
                    role="developer",
                )
            ],
        )
    )

    with pytest.raises(ValueError, match=r"has active jobs and cannot be finalized"):
        service.finalize(
            OrchestrationFinalizeRequest(
                run_id=start_response.run_id,
                outcome=RunOutcome.SUCCEEDED,
                summary="should-fail",
                cleanup_policy=CleanupPolicy(),
            )
        )

    status = service.status(OrchestrationStatusRequest(run_id=start_response.run_id))
    assert status.run.status == "running"
    assert status.snapshot.active_job_ids


def test_finalize_is_idempotent_for_already_finalized_run(runtime: OrchestrationRuntime) -> None:
    service = OrchestrationService(runtime=runtime)
    start_response = service.start(OrchestrationStartRequest(name="finalize-idempotent"))

    first = service.finalize(
        OrchestrationFinalizeRequest(
            run_id=start_response.run_id,
            outcome=RunOutcome.SUCCEEDED,
            summary="first-summary",
            cleanup_policy=CleanupPolicy(),
        )
    )
    assert first.status == "finalized"

    events_after_first = runtime.store.read_events(
        run_id=start_response.run_id, cursor=0, max_events=200
    )
    first_finalized_status_changes = [
        event
        for event in events_after_first
        if event.event_type == EventType.RUN_STATUS_CHANGED.value
        and event.payload.run is not None
        and event.payload.run.status == "finalized"
    ]
    assert len(first_finalized_status_changes) == 1

    second = service.finalize(
        OrchestrationFinalizeRequest(
            run_id=start_response.run_id,
            outcome=RunOutcome.FAILED,
            summary="second-summary-ignored",
            cleanup_policy=CleanupPolicy(
                successful_terminals="preserve_until_ttl",
                failed_terminals="kill",
                ttl_sec=42,
            ),
        )
    )

    events_after_second = runtime.store.read_events(
        run_id=start_response.run_id, cursor=0, max_events=200
    )
    second_finalized_status_changes = [
        event
        for event in events_after_second
        if event.event_type == EventType.RUN_STATUS_CHANGED.value
        and event.payload.run is not None
        and event.payload.run.status == "finalized"
    ]

    assert len(second_finalized_status_changes) == 1
    assert len(events_after_second) == len(events_after_first)
    assert second.finalized_at == first.finalized_at
    assert second.outcome == first.outcome
    assert second.summary == first.summary


def test_start_attempt_cleans_up_terminal_when_linking_fails(runtime: OrchestrationRuntime) -> None:
    factory = _TerminalFactory()
    interrupted: List[str] = []
    deleted: List[str] = []

    def _interrupt(terminal_id: str, _key: str) -> bool:
        interrupted.append(terminal_id)
        return True

    def _delete(terminal_id: str) -> bool:
        deleted.append(terminal_id)
        return True

    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        send_special_key_fn=_interrupt,
        delete_terminal_fn=_delete,
        resolve_provider_fn=lambda _profile, _fallback: ProviderType.CODEX.value,
    )

    with patch.object(
        runtime.store, "link_worker_terminal", side_effect=RuntimeError("link failed")
    ):
        start_response = service.start(
            OrchestrationStartRequest(
                name="link-failure-cleanup",
                jobs=[
                    OrchestrationJobSpec(
                        agent_profile="developer",
                        message="work-item",
                        role="developer",
                    )
                ],
            )
        )

    jobs = runtime.store.list_jobs(run_id=start_response.run_id)
    attempts = runtime.store.list_attempts(run_id=start_response.run_id)
    assert len(jobs) == 1
    assert len(attempts) == 1
    assert jobs[0].status == JobStatus.FAILED.value
    assert attempts[0].status == AttemptStatus.FAILED.value
    assert attempts[0].terminal_id == "term-0"
    assert interrupted == ["term-0"]
    assert deleted == ["term-0"]
    assert runtime.store.get_worker_terminal(terminal_id="term-0") is None


@pytest.mark.asyncio
async def test_wait_returns_immediately_then_times_out(
    runtime: OrchestrationRuntime,
) -> None:
    await runtime.start()
    try:
        service = OrchestrationService(runtime=runtime)

        start_response = service.start(OrchestrationStartRequest(name="wait-run"))

        immediate = await service.wait(
            OrchestrationWaitRequest(
                run_id=start_response.run_id,
                cursor=0,
                min_events=1,
                max_events=10,
                wait_timeout_sec=1,
                include_snapshot=False,
            )
        )
        assert immediate.timeout is False
        assert immediate.events

        timed_out = await service.wait(
            OrchestrationWaitRequest(
                run_id=start_response.run_id,
                cursor=immediate.cursor,
                min_events=1,
                max_events=10,
                wait_timeout_sec=1,
                include_snapshot=False,
            )
        )
        assert timed_out.timeout is True
        assert timed_out.events == []
    finally:
        await runtime.stop()


def test_spawn_from_idle_moves_run_back_to_running(runtime: OrchestrationRuntime) -> None:
    factory = _TerminalFactory()
    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        resolve_provider_fn=lambda _profile, _fallback: ProviderType.CODEX.value,
    )

    start_response = service.start(OrchestrationStartRequest(name="idle-then-spawn"))
    assert start_response.status == "idle"
    # No jobs means run should become idle in maintenance.
    status_response = service.status(OrchestrationStatusRequest(run_id=start_response.run_id))
    assert status_response.run.status == "idle"

    spawn_response = service.spawn(
        OrchestrationSpawnRequest(
            run_id=start_response.run_id,
            agent_profile="developer",
            message="new work",
            role="developer",
        )
    )

    assert spawn_response.status in {JobStatus.RUNNING.value, JobStatus.QUEUED.value}

    status_after_spawn = service.status(OrchestrationStatusRequest(run_id=start_response.run_id))
    assert status_after_spawn.run.status == "running"


def test_reconcile_startup_marks_running_attempt_failed_when_terminal_missing(
    runtime: OrchestrationRuntime,
) -> None:
    factory = _TerminalFactory()
    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        terminal_exists_fn=lambda _terminal_id: False,
        resolve_provider_fn=lambda _profile, _fallback: ProviderType.CODEX.value,
    )

    started = service.start(
        OrchestrationStartRequest(
            name="startup-reconcile-missing-terminal",
            jobs=[
                OrchestrationJobSpec(agent_profile="developer", message="work", role="developer")
            ],
        )
    )

    service.reconcile_startup()

    status = service.status(OrchestrationStatusRequest(run_id=started.run_id))
    attempts = runtime.store.list_attempts(run_id=started.run_id)
    jobs = runtime.store.list_jobs(run_id=started.run_id)
    links = runtime.store.list_worker_terminals(run_id=started.run_id, active_only=False)

    assert attempts[0].status == AttemptStatus.FAILED.value
    assert jobs[0].status == JobStatus.FAILED.value
    assert status.run.status == "idle"
    assert links[0]["released_at"] is not None

    events = runtime.store.read_events(run_id=started.run_id, cursor=0, max_events=200)
    event_types = [event.event_type for event in events]
    assert EventType.ATTEMPT_FAILED.value in event_types
    assert EventType.JOB_FAILED.value in event_types


def test_reconcile_startup_ingests_pending_worker_marker_log(
    runtime: OrchestrationRuntime, tmp_path: Path
) -> None:
    factory = _TerminalFactory()
    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        terminal_exists_fn=lambda _terminal_id: True,
        terminal_log_dir=tmp_path,
        resolve_provider_fn=lambda _profile, _fallback: ProviderType.CODEX.value,
    )

    started = service.start(
        OrchestrationStartRequest(
            name="startup-reconcile-ingest-log",
            jobs=[
                OrchestrationJobSpec(agent_profile="developer", message="work", role="developer")
            ],
        )
    )
    attempt = runtime.store.list_attempts(run_id=started.run_id)[0]

    marker = encode_worker_callback_marker(
        {
            "version": 1,
            "run_id": started.run_id,
            "job_id": attempt.job_id,
            "attempt_id": attempt.attempt_id,
            "type": "job.completed",
            "status": "succeeded",
            "result": {"summary": "done"},
            "nonce": "startup-reconcile-marker",
        }
    )
    (tmp_path / f"{attempt.terminal_id}.log").write_text(f"{marker}\n", encoding="utf-8")

    service.reconcile_startup()

    refreshed_attempt = runtime.store.get_attempt(attempt_id=attempt.attempt_id)
    refreshed_job = runtime.store.get_job(job_id=attempt.job_id)
    run_status = service.status(OrchestrationStatusRequest(run_id=started.run_id)).run.status

    assert refreshed_attempt is not None
    assert refreshed_job is not None
    assert refreshed_attempt.status == AttemptStatus.SUCCEEDED.value
    assert refreshed_job.status == JobStatus.SUCCEEDED.value
    assert run_status == "idle"


def test_cancel_run_reaps_successful_worker_terminals(runtime: OrchestrationRuntime) -> None:
    factory = _TerminalFactory()
    deleted: List[str] = []

    def _delete(terminal_id: str) -> bool:
        deleted.append(terminal_id)
        return True

    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        delete_terminal_fn=_delete,
        terminal_exists_fn=lambda _terminal_id: True,
        resolve_provider_fn=lambda _profile, _fallback: ProviderType.CODEX.value,
    )

    started = service.start(
        OrchestrationStartRequest(
            name="cancel-reaps-terminals",
            jobs=[
                OrchestrationJobSpec(agent_profile="developer", message="work", role="developer")
            ],
        )
    )

    attempt = runtime.store.list_attempts(run_id=started.run_id)[0]
    runtime.store.update_attempt(
        attempt_id=attempt.attempt_id,
        status=AttemptStatus.SUCCEEDED,
        completed_at=datetime.now(timezone.utc),
    )
    runtime.store.update_job(job_id=attempt.job_id, status=JobStatus.SUCCEEDED)

    service.cancel(
        OrchestrationCancelRequest(
            run_id=started.run_id,
            scope=CancelScope.RUN,
            reason="operator-cancelled",
        )
    )

    run = runtime.store.get_run(run_id=started.run_id)
    assert run is not None
    assert run.status == RunStatus.CANCELLED.value
    assert deleted == [attempt.terminal_id]

    link = runtime.store.get_worker_terminal(terminal_id=attempt.terminal_id)
    assert link is not None
    assert link["released_at"] is not None


def test_reaper_kills_preserved_successful_terminals_when_ttl_expires(
    runtime: OrchestrationRuntime,
) -> None:
    factory = _TerminalFactory()
    deleted: List[str] = []
    clock = _Clock(datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc))

    def _delete(terminal_id: str) -> bool:
        deleted.append(terminal_id)
        return True

    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        delete_terminal_fn=_delete,
        terminal_exists_fn=lambda _terminal_id: True,
        resolve_provider_fn=lambda _profile, _fallback: ProviderType.CODEX.value,
        now_fn=clock.now,
    )

    started = service.start(
        OrchestrationStartRequest(
            name="ttl-reaper",
            jobs=[
                OrchestrationJobSpec(agent_profile="developer", message="work", role="developer")
            ],
        )
    )
    attempt = runtime.store.list_attempts(run_id=started.run_id)[0]
    runtime.store.update_attempt(
        attempt_id=attempt.attempt_id,
        status=AttemptStatus.SUCCEEDED,
        completed_at=clock.now(),
    )
    runtime.store.update_job(job_id=attempt.job_id, status=JobStatus.SUCCEEDED)
    service.status(OrchestrationStatusRequest(run_id=started.run_id))

    service.finalize(
        OrchestrationFinalizeRequest(
            run_id=started.run_id,
            outcome=RunOutcome.SUCCEEDED,
            summary="keep briefly",
            cleanup_policy=CleanupPolicy(
                successful_terminals="preserve_until_ttl",
                failed_terminals="preserve",
                ttl_sec=10,
            ),
        )
    )

    clock.advance(seconds=5)
    service.reap()
    assert deleted == []

    clock.advance(seconds=6)
    service.reap()
    assert deleted == [attempt.terminal_id]
