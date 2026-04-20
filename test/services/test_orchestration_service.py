"""Focused tests for orchestration scheduler/wait/timeout/cancel/finalize behavior."""

from __future__ import annotations

import os
import sqlite3
import time
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


def _noop_send_input(_terminal_id: str, _message: str) -> bool:
    return True


def _wrap_marker_like_ansi_terminal_output(marker: str) -> str:
    assert marker.startswith("⟦CAO-EVENT-v1:")
    assert marker.endswith("⟧")
    payload = marker[len("⟦CAO-EVENT-v1:") : -1]
    segments = [payload[i : i + 120] for i in range(0, len(payload), 120)]
    if len(segments) < 3:
        segments = [payload[:20], payload[20:40], payload[40:]]

    lines = [f"\x1b[39;49m\x1b[K  ⟦CAO-EVENT-\x1b[39m\x1b[49m\x1b[0m"]
    for index, segment in enumerate(segments):
        prefix = "v1:" if index == 0 else ""
        suffix = "⟧" if index == len(segments) - 1 else ""
        lines.append(f"\x1b[39;49m\x1b[K  {prefix}{segment}{suffix}\x1b[39m\x1b[49m\x1b[0m")
    return "\n".join(lines)


def _upsert_terminal_metadata(
    *,
    runtime: OrchestrationRuntime,
    terminal_id: str,
    last_active: datetime,
    tmux_session: str = "cao-test-session",
    tmux_window: str = "w-test",
    provider: str = "codex",
) -> None:
    with sqlite3.connect(str(runtime.store._database_file)) as conn:  # noqa: SLF001
        conn.execute("""
            CREATE TABLE IF NOT EXISTS terminals (
                id TEXT PRIMARY KEY,
                tmux_session TEXT NOT NULL,
                tmux_window TEXT NOT NULL,
                provider TEXT NOT NULL,
                agent_profile TEXT,
                allowed_tools TEXT,
                last_active TEXT
            )
            """)
        conn.execute(
            """
            INSERT INTO terminals (
                id, tmux_session, tmux_window, provider, agent_profile, allowed_tools, last_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                tmux_session = excluded.tmux_session,
                tmux_window = excluded.tmux_window,
                provider = excluded.provider,
                last_active = excluded.last_active
            """,
            (
                terminal_id,
                tmux_session,
                tmux_window,
                provider,
                "developer",
                None,
                last_active.isoformat(),
            ),
        )
        conn.commit()


def _delete_terminal_metadata(*, runtime: OrchestrationRuntime, terminal_id: str) -> None:
    with sqlite3.connect(str(runtime.store._database_file)) as conn:  # noqa: SLF001
        conn.execute("DELETE FROM terminals WHERE id = ?", (terminal_id,))
        conn.commit()


def test_scheduler_enforces_worker_reviewer_and_total_caps(runtime: OrchestrationRuntime) -> None:
    factory = _TerminalFactory()
    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        send_input_fn=_noop_send_input,
        terminal_exists_fn=lambda _terminal_id: True,
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
        send_input_fn=_noop_send_input,
        send_special_key_fn=_interrupt,
        delete_terminal_fn=_delete,
        terminal_exists_fn=lambda _terminal_id: True,
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
        send_input_fn=_noop_send_input,
        terminal_exists_fn=lambda _terminal_id: True,
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
        send_input_fn=_noop_send_input,
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
        send_input_fn=_noop_send_input,
        terminal_exists_fn=lambda _terminal_id: True,
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


def test_start_attempt_sends_job_message_to_worker_terminal(runtime: OrchestrationRuntime) -> None:
    factory = _TerminalFactory()
    sent_messages: List[tuple[str, str]] = []

    def _send_input(terminal_id: str, message: str) -> bool:
        sent_messages.append((terminal_id, message))
        return True

    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        send_input_fn=_send_input,
        resolve_provider_fn=lambda _profile, _fallback: ProviderType.CODEX.value,
    )

    start_response = service.start(
        OrchestrationStartRequest(
            name="send-message-success",
            jobs=[
                OrchestrationJobSpec(
                    agent_profile="developer",
                    message="deliver this prompt",
                    role="developer",
                )
            ],
        )
    )

    jobs = runtime.store.list_jobs(run_id=start_response.run_id)
    attempts = runtime.store.list_attempts(run_id=start_response.run_id)
    assert len(jobs) == 1
    assert len(attempts) == 1
    assert jobs[0].status == JobStatus.RUNNING.value
    assert attempts[0].status == AttemptStatus.RUNNING.value
    assert attempts[0].terminal_id == "term-0"
    assert sent_messages == [("term-0", "deliver this prompt")]


def test_start_attempt_cleans_up_terminal_when_input_send_fails(
    runtime: OrchestrationRuntime,
) -> None:
    factory = _TerminalFactory()
    interrupted: List[str] = []
    deleted: List[str] = []

    def _send_input(_terminal_id: str, _message: str) -> bool:
        raise RuntimeError("send failed")

    def _interrupt(terminal_id: str, _key: str) -> bool:
        interrupted.append(terminal_id)
        return True

    def _delete(terminal_id: str) -> bool:
        deleted.append(terminal_id)
        return True

    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        send_input_fn=_send_input,
        send_special_key_fn=_interrupt,
        delete_terminal_fn=_delete,
        resolve_provider_fn=lambda _profile, _fallback: ProviderType.CODEX.value,
    )

    start_response = service.start(
        OrchestrationStartRequest(
            name="send-message-failure-cleanup",
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
    link = runtime.store.get_worker_terminal(terminal_id="term-0")
    assert link is not None
    assert link["released_at"] is not None

    event_types = [
        event.event_type
        for event in runtime.store.read_events(
            run_id=start_response.run_id, cursor=0, max_events=100
        )
    ]
    assert EventType.ATTEMPT_FAILED.value in event_types
    assert EventType.JOB_FAILED.value in event_types
    assert EventType.ATTEMPT_STARTED.value not in event_types
    assert EventType.JOB_STARTED.value not in event_types


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
        send_input_fn=_noop_send_input,
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


@pytest.mark.asyncio
async def test_wait_maintenance_kills_succeeded_worker_terminal(
    runtime: OrchestrationRuntime,
) -> None:
    await runtime.start()
    deleted: List[str] = []
    interrupted: List[str] = []
    factory = _TerminalFactory()

    def _interrupt(terminal_id: str, _key: str) -> bool:
        interrupted.append(terminal_id)
        return True

    def _delete(terminal_id: str) -> bool:
        deleted.append(terminal_id)
        return True

    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        send_input_fn=_noop_send_input,
        send_special_key_fn=_interrupt,
        delete_terminal_fn=_delete,
        terminal_exists_fn=lambda _terminal_id: True,
        resolve_provider_fn=lambda _profile, _fallback: ProviderType.CODEX.value,
    )

    try:
        started = service.start(
            OrchestrationStartRequest(
                name="wait-success-terminal-cleanup",
                jobs=[
                    OrchestrationJobSpec(
                        agent_profile="developer",
                        message="work",
                        role="developer",
                    )
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

        wait_response = await service.wait(
            OrchestrationWaitRequest(
                run_id=started.run_id,
                cursor=0,
                min_events=1,
                wait_timeout_sec=1,
                include_snapshot=False,
            )
        )

        assert wait_response.timeout is False
        assert deleted == [attempt.terminal_id]
        assert interrupted == [attempt.terminal_id]
        link = runtime.store.get_worker_terminal(terminal_id=attempt.terminal_id)
        assert link is not None
        assert link["released_at"] is not None
    finally:
        await runtime.stop()


@pytest.mark.parametrize(
    ("attempt_status", "job_status"),
    [
        (AttemptStatus.FAILED, JobStatus.FAILED),
        (AttemptStatus.CANCELLED, JobStatus.CANCELLED),
        (AttemptStatus.TIMED_OUT, JobStatus.TIMED_OUT),
    ],
)
def test_status_maintenance_does_not_kill_non_success_terminal_attempts(
    runtime: OrchestrationRuntime,
    attempt_status: AttemptStatus,
    job_status: JobStatus,
) -> None:
    deleted: List[str] = []
    interrupted: List[str] = []
    factory = _TerminalFactory()

    def _interrupt(terminal_id: str, _key: str) -> bool:
        interrupted.append(terminal_id)
        return True

    def _delete(terminal_id: str) -> bool:
        deleted.append(terminal_id)
        return True

    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        send_input_fn=_noop_send_input,
        send_special_key_fn=_interrupt,
        delete_terminal_fn=_delete,
        terminal_exists_fn=lambda _terminal_id: True,
        resolve_provider_fn=lambda _profile, _fallback: ProviderType.CODEX.value,
    )

    started = service.start(
        OrchestrationStartRequest(
            name="status-non-success-terminal-preserve",
            jobs=[
                OrchestrationJobSpec(
                    agent_profile="developer",
                    message="work",
                    role="developer",
                )
            ],
        )
    )

    attempt = runtime.store.list_attempts(run_id=started.run_id)[0]
    runtime.store.update_attempt(
        attempt_id=attempt.attempt_id,
        status=attempt_status,
        completed_at=datetime.now(timezone.utc),
    )
    runtime.store.update_job(job_id=attempt.job_id, status=job_status)

    service.status(OrchestrationStatusRequest(run_id=started.run_id))

    assert deleted == []
    assert interrupted == []
    link = runtime.store.get_worker_terminal(terminal_id=attempt.terminal_id)
    assert link is not None
    assert link["released_at"] is not None


def test_status_maintenance_success_terminal_cleanup_is_idempotent(
    runtime: OrchestrationRuntime,
) -> None:
    deleted: List[str] = []
    interrupted: List[str] = []
    factory = _TerminalFactory()

    def _interrupt(terminal_id: str, _key: str) -> bool:
        interrupted.append(terminal_id)
        return True

    def _delete(terminal_id: str) -> bool:
        deleted.append(terminal_id)
        return True

    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        send_input_fn=_noop_send_input,
        send_special_key_fn=_interrupt,
        delete_terminal_fn=_delete,
        terminal_exists_fn=lambda _terminal_id: True,
        resolve_provider_fn=lambda _profile, _fallback: ProviderType.CODEX.value,
    )

    started = service.start(
        OrchestrationStartRequest(
            name="status-success-terminal-idempotent-cleanup",
            jobs=[
                OrchestrationJobSpec(
                    agent_profile="developer",
                    message="work",
                    role="developer",
                )
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

    service.status(OrchestrationStatusRequest(run_id=started.run_id))
    service.status(OrchestrationStatusRequest(run_id=started.run_id))

    assert deleted == [attempt.terminal_id]
    assert interrupted == [attempt.terminal_id]
    link = runtime.store.get_worker_terminal(terminal_id=attempt.terminal_id)
    assert link is not None
    assert link["released_at"] is not None


def test_spawn_from_idle_moves_run_back_to_running(runtime: OrchestrationRuntime) -> None:
    factory = _TerminalFactory()
    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        send_input_fn=_noop_send_input,
        terminal_exists_fn=lambda _terminal_id: True,
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


def test_status_includes_terminal_liveness_when_requested(runtime: OrchestrationRuntime) -> None:
    factory = _TerminalFactory()
    clock = _Clock(datetime(2026, 4, 20, 12, 0, tzinfo=timezone.utc))
    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        send_input_fn=_noop_send_input,
        terminal_exists_fn=lambda _terminal_id: True,
        resolve_provider_fn=lambda _profile, _fallback: ProviderType.CODEX.value,
        now_fn=clock.now,
    )

    started = service.start(
        OrchestrationStartRequest(
            name="status-liveness-present",
            jobs=[
                OrchestrationJobSpec(agent_profile="developer", message="work", role="developer")
            ],
        )
    )

    attempt = runtime.store.list_attempts(run_id=started.run_id)[0]
    assert attempt.terminal_id is not None

    terminal_last_active = datetime(2026, 4, 20, 11, 59, 20, tzinfo=timezone.utc)
    _upsert_terminal_metadata(
        runtime=runtime,
        terminal_id=attempt.terminal_id,
        last_active=terminal_last_active,
    )

    log_updated_at = datetime(2026, 4, 20, 11, 59, 40, tzinfo=timezone.utc)
    runtime.store.upsert_terminal_log_offset(
        terminal_id=attempt.terminal_id,
        byte_offset=456,
        updated_at=log_updated_at,
    )

    status = service.status(
        OrchestrationStatusRequest(run_id=started.run_id, include_terminal_refs=True)
    )
    assert status.terminal_refs is not None
    ref = status.terminal_refs[0]

    assert ref.attempt_id == attempt.attempt_id
    assert ref.job_id == attempt.job_id
    assert ref.terminal_id == attempt.terminal_id
    assert ref.terminal_present is True
    assert ref.terminal_last_active_at == terminal_last_active
    assert ref.log_offset == 456
    assert ref.last_log_activity_at == log_updated_at
    assert ref.activity_age_sec == 20
    assert ref.tmux_session is not None
    assert ref.tmux_window is not None
    assert ref.worker_terminal_released_at is None


def test_status_liveness_interprets_naive_terminal_timestamp_as_local_time(
    runtime: OrchestrationRuntime,
) -> None:
    if not hasattr(time, "tzset"):
        pytest.skip("time.tzset is unavailable on this platform")

    original_tz = os.environ.get("TZ")
    try:
        os.environ["TZ"] = "America/Vancouver"
        time.tzset()

        factory = _TerminalFactory()
        naive_local_last_active = datetime(2026, 4, 20, 11, 59, 20)
        expected_last_active_utc = naive_local_last_active.astimezone(timezone.utc)
        clock = _Clock(expected_last_active_utc + timedelta(seconds=40))
        service = OrchestrationService(
            runtime=runtime,
            create_session_fn=factory,
            send_input_fn=_noop_send_input,
            terminal_exists_fn=lambda _terminal_id: True,
            resolve_provider_fn=lambda _profile, _fallback: ProviderType.CODEX.value,
            now_fn=clock.now,
        )

        started = service.start(
            OrchestrationStartRequest(
                name="status-liveness-naive-local",
                jobs=[
                    OrchestrationJobSpec(
                        agent_profile="developer",
                        message="work",
                        role="developer",
                    )
                ],
            )
        )

        attempt = runtime.store.list_attempts(run_id=started.run_id)[0]
        assert attempt.terminal_id is not None
        _upsert_terminal_metadata(
            runtime=runtime,
            terminal_id=attempt.terminal_id,
            last_active=naive_local_last_active,
        )

        status = service.status(
            OrchestrationStatusRequest(run_id=started.run_id, include_terminal_refs=True)
        )
        assert status.terminal_refs is not None
        ref = status.terminal_refs[0]

        assert ref.terminal_last_active_at == expected_last_active_utc
        assert ref.activity_age_sec == 40
    finally:
        if original_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = original_tz
        time.tzset()


def test_status_liveness_handles_missing_terminal_row(runtime: OrchestrationRuntime) -> None:
    factory = _TerminalFactory()
    clock = _Clock(datetime(2026, 4, 20, 12, 0, tzinfo=timezone.utc))
    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        send_input_fn=_noop_send_input,
        terminal_exists_fn=lambda _terminal_id: True,
        resolve_provider_fn=lambda _profile, _fallback: ProviderType.CODEX.value,
        now_fn=clock.now,
    )

    started = service.start(
        OrchestrationStartRequest(
            name="status-liveness-terminal-gone",
            jobs=[
                OrchestrationJobSpec(agent_profile="developer", message="work", role="developer")
            ],
        )
    )
    attempt = runtime.store.list_attempts(run_id=started.run_id)[0]
    assert attempt.terminal_id is not None

    _upsert_terminal_metadata(
        runtime=runtime,
        terminal_id=attempt.terminal_id,
        last_active=datetime(2026, 4, 20, 11, 59, 30, tzinfo=timezone.utc),
    )
    _delete_terminal_metadata(runtime=runtime, terminal_id=attempt.terminal_id)

    log_updated_at = datetime(2026, 4, 20, 11, 59, 45, tzinfo=timezone.utc)
    runtime.store.upsert_terminal_log_offset(
        terminal_id=attempt.terminal_id,
        byte_offset=321,
        updated_at=log_updated_at,
    )

    status = service.status(
        OrchestrationStatusRequest(run_id=started.run_id, include_terminal_refs=True)
    )
    assert status.terminal_refs is not None
    ref = status.terminal_refs[0]

    assert ref.terminal_id == attempt.terminal_id
    assert ref.terminal_present is False
    assert ref.terminal_last_active_at is None
    assert ref.log_offset == 321
    assert ref.last_log_activity_at == log_updated_at
    assert ref.activity_age_sec == 15
    assert ref.tmux_session is not None
    assert ref.tmux_window is not None


def test_status_liveness_handles_missing_log_offset(runtime: OrchestrationRuntime) -> None:
    factory = _TerminalFactory()
    clock = _Clock(datetime(2026, 4, 20, 12, 0, tzinfo=timezone.utc))
    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        send_input_fn=_noop_send_input,
        terminal_exists_fn=lambda _terminal_id: True,
        resolve_provider_fn=lambda _profile, _fallback: ProviderType.CODEX.value,
        now_fn=clock.now,
    )

    started = service.start(
        OrchestrationStartRequest(
            name="status-liveness-no-log-offset",
            jobs=[
                OrchestrationJobSpec(agent_profile="developer", message="work", role="developer")
            ],
        )
    )
    attempt = runtime.store.list_attempts(run_id=started.run_id)[0]
    assert attempt.terminal_id is not None

    terminal_last_active = datetime(2026, 4, 20, 11, 59, 25, tzinfo=timezone.utc)
    _upsert_terminal_metadata(
        runtime=runtime,
        terminal_id=attempt.terminal_id,
        last_active=terminal_last_active,
    )

    status = service.status(
        OrchestrationStatusRequest(run_id=started.run_id, include_terminal_refs=True)
    )
    assert status.terminal_refs is not None
    ref = status.terminal_refs[0]

    assert ref.terminal_id == attempt.terminal_id
    assert ref.terminal_present is True
    assert ref.terminal_last_active_at == terminal_last_active
    assert ref.log_offset is None
    assert ref.last_log_activity_at is None
    assert ref.activity_age_sec == 35


def test_status_without_terminal_refs_remains_lean(runtime: OrchestrationRuntime) -> None:
    factory = _TerminalFactory()
    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        send_input_fn=_noop_send_input,
        terminal_exists_fn=lambda _terminal_id: True,
        resolve_provider_fn=lambda _profile, _fallback: ProviderType.CODEX.value,
    )

    started = service.start(
        OrchestrationStartRequest(
            name="status-without-liveness",
            jobs=[
                OrchestrationJobSpec(agent_profile="developer", message="work", role="developer")
            ],
        )
    )

    status = service.status(OrchestrationStatusRequest(run_id=started.run_id))

    assert status.terminal_refs is None
    assert status.attempts is None


def test_reconcile_startup_marks_running_attempt_failed_when_terminal_missing(
    runtime: OrchestrationRuntime,
) -> None:
    factory = _TerminalFactory()
    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        send_input_fn=_noop_send_input,
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
        send_input_fn=_noop_send_input,
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


def test_maintenance_marks_running_attempt_failed_when_terminal_exits_without_marker(
    runtime: OrchestrationRuntime,
) -> None:
    factory = _TerminalFactory()
    terminal_alive = {"term-0": True}
    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        send_input_fn=_noop_send_input,
        terminal_exists_fn=lambda terminal_id: terminal_alive.get(terminal_id, False),
        resolve_provider_fn=lambda _profile, _fallback: ProviderType.CODEX.value,
    )

    started = service.start(
        OrchestrationStartRequest(
            name="maintenance-reconcile-missing-terminal",
            jobs=[
                OrchestrationJobSpec(agent_profile="developer", message="work", role="developer")
            ],
        )
    )

    attempt = runtime.store.list_attempts(run_id=started.run_id)[0]
    assert attempt.status == AttemptStatus.RUNNING.value
    terminal_alive[attempt.terminal_id] = False

    status = service.status(OrchestrationStatusRequest(run_id=started.run_id))
    refreshed_attempt = runtime.store.get_attempt(attempt_id=attempt.attempt_id)
    refreshed_job = runtime.store.get_job(job_id=attempt.job_id)

    assert refreshed_attempt is not None
    assert refreshed_job is not None
    assert refreshed_attempt.status == AttemptStatus.FAILED.value
    assert refreshed_job.status == JobStatus.FAILED.value
    assert status.run.status == RunStatus.IDLE.value
    assert refreshed_attempt.result_summary == "terminal_missing"


def test_maintenance_recovers_completion_marker_from_full_log_when_terminal_exits(
    runtime: OrchestrationRuntime, tmp_path: Path
) -> None:
    factory = _TerminalFactory()
    terminal_alive = {"term-0": True}
    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        send_input_fn=_noop_send_input,
        terminal_exists_fn=lambda terminal_id: terminal_alive.get(terminal_id, False),
        terminal_log_dir=tmp_path,
        resolve_provider_fn=lambda _profile, _fallback: ProviderType.CODEX.value,
    )

    started = service.start(
        OrchestrationStartRequest(
            name="maintenance-recover-missing-callback",
            jobs=[
                OrchestrationJobSpec(agent_profile="reviewer", message="review", role="reviewer")
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
            "result": {"summary": "review complete"},
            "nonce": "maintenance-recover-marker",
        }
    )
    wrapped_marker = _wrap_marker_like_ansi_terminal_output(marker)
    log_path = tmp_path / f"{attempt.terminal_id}.log"
    log_path.write_text(f"{wrapped_marker}\n", encoding="utf-8")

    # Simulate a prior parser miss that already advanced persisted offset to EOF.
    runtime.store.upsert_terminal_log_offset(
        terminal_id=attempt.terminal_id,
        byte_offset=log_path.stat().st_size,
    )

    terminal_alive[attempt.terminal_id] = False
    status = service.status(OrchestrationStatusRequest(run_id=started.run_id))

    refreshed_attempt = runtime.store.get_attempt(attempt_id=attempt.attempt_id)
    refreshed_job = runtime.store.get_job(job_id=attempt.job_id)

    assert refreshed_attempt is not None
    assert refreshed_job is not None
    assert refreshed_attempt.status == AttemptStatus.SUCCEEDED.value
    assert refreshed_job.status == JobStatus.SUCCEEDED.value
    assert status.run.status == RunStatus.IDLE.value


def test_cancel_run_reaps_successful_worker_terminals(runtime: OrchestrationRuntime) -> None:
    factory = _TerminalFactory()
    deleted: List[str] = []

    def _delete(terminal_id: str) -> bool:
        deleted.append(terminal_id)
        return True

    service = OrchestrationService(
        runtime=runtime,
        create_session_fn=factory,
        send_input_fn=_noop_send_input,
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
        send_input_fn=_noop_send_input,
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
