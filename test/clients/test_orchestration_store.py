"""Tests for the orchestration store repository layer."""

from pathlib import Path

import pytest

from cli_agent_orchestrator.clients.database import run_schema_migrations
from cli_agent_orchestrator.clients.orchestration_store import OrchestrationStore
from cli_agent_orchestrator.models.orchestration import (
    AttemptStatus,
    EventType,
    JobStatus,
    RunStatus,
)


@pytest.fixture
def store(tmp_path: Path) -> OrchestrationStore:
    db_file = tmp_path / "orchestration-store.db"
    run_schema_migrations(database_file=db_file)
    return OrchestrationStore(database_file=db_file)


def test_run_job_attempt_crud(store: OrchestrationStore) -> None:
    run = store.create_run(run_id="run-1", name="Build", metadata={"team": "core"})
    assert run.run_id == "run-1"
    assert run.status == RunStatus.RUNNING.value

    run = store.update_run(run_id="run-1", status=RunStatus.IDLE, finalized_at=run.updated_at)
    assert run is not None
    assert run.status == RunStatus.IDLE.value

    job = store.create_job(
        job_id="job-1",
        run_id="run-1",
        agent_profile="developer",
        message="Implement feature",
        role="developer",
        timeout_sec=120,
        idempotency_key="job-1-key",
    )
    assert job.status == JobStatus.CREATED.value

    by_key = store.get_job_by_idempotency_key(run_id="run-1", idempotency_key="job-1-key")
    assert by_key is not None
    assert by_key.job_id == "job-1"

    updated_job = store.update_job(job_id="job-1", status=JobStatus.RUNNING)
    assert updated_job is not None
    assert updated_job.status == JobStatus.RUNNING.value

    attempt = store.create_attempt(attempt_id="attempt-1", run_id="run-1", job_id="job-1")
    assert attempt.status == AttemptStatus.CREATED.value

    updated_attempt = store.update_attempt(
        attempt_id="attempt-1",
        status=AttemptStatus.SUCCEEDED,
        terminal_id="term-1",
        result_summary="done",
        result_data={"ok": True},
    )
    assert updated_attempt is not None
    assert updated_attempt.status == AttemptStatus.SUCCEEDED.value
    assert updated_attempt.result_data == {"ok": True}


def test_append_event_dedupe_and_cursor_reads(store: OrchestrationStore) -> None:
    store.create_run(run_id="run-1")
    store.create_job(
        job_id="job-1",
        run_id="run-1",
        agent_profile="developer",
        message="Work",
    )

    event_1 = store.append_event(
        run_id="run-1",
        job_id="job-1",
        event_type=EventType.JOB_CREATED,
        payload={"job": {"status": JobStatus.CREATED.value}},
    )
    deduped_first = store.append_event(
        run_id="run-1",
        event_type=EventType.JOB_QUEUED,
        job_id="job-1",
        payload={"job": {"status": JobStatus.QUEUED.value}},
        dedupe_key="job-1-queued",
    )
    deduped_second = store.append_event(
        run_id="run-1",
        event_type=EventType.JOB_QUEUED,
        job_id="job-1",
        payload={"job": {"status": JobStatus.QUEUED.value}},
        dedupe_key="job-1-queued",
    )

    assert deduped_first.event_id == deduped_second.event_id

    events_after_first = store.read_events(run_id="run-1", cursor=event_1.event_id)
    assert [event.event_type for event in events_after_first] == [EventType.JOB_QUEUED.value]

    filtered_events = store.read_events(
        run_id="run-1",
        cursor=0,
        event_types=[EventType.JOB_CREATED],
        job_ids=["job-1"],
    )
    assert [event.event_id for event in filtered_events] == [event_1.event_id]

    assert store.get_latest_event_cursor(run_id="run-1") == deduped_first.event_id


def test_worker_terminal_linkage_and_subscription_cursor(store: OrchestrationStore) -> None:
    store.create_run(run_id="run-1")
    store.create_job(
        job_id="job-1",
        run_id="run-1",
        agent_profile="developer",
        message="Work",
    )
    store.create_attempt(attempt_id="attempt-1", run_id="run-1", job_id="job-1")

    link = store.link_worker_terminal(
        terminal_id="term-1",
        run_id="run-1",
        job_id="job-1",
        attempt_id="attempt-1",
        provider="codex",
        tmux_session="cao-abc",
        tmux_window="w-1",
    )
    assert link["terminal_id"] == "term-1"

    active_links = store.list_worker_terminals(run_id="run-1", active_only=True)
    assert [item["terminal_id"] for item in active_links] == ["term-1"]

    assert store.release_worker_terminal(terminal_id="term-1") is True
    assert store.list_worker_terminals(run_id="run-1", active_only=True) == []

    sub = store.upsert_subscription(run_id="run-1", subscriber_id="waiter-1", cursor=2)
    assert sub["cursor"] == 2

    # Upsert should preserve monotonic cursor progression.
    stale_upsert = store.upsert_subscription(run_id="run-1", subscriber_id="waiter-1", cursor=1)
    assert stale_upsert["cursor"] == 2

    forward_upsert = store.upsert_subscription(run_id="run-1", subscriber_id="waiter-1", cursor=4)
    assert forward_upsert["cursor"] == 4

    advanced = store.advance_subscription_cursor(run_id="run-1", subscriber_id="waiter-1", cursor=5)
    assert advanced is not None
    assert advanced["cursor"] == 5

    # Cursor should not move backwards.
    rewound = store.advance_subscription_cursor(run_id="run-1", subscriber_id="waiter-1", cursor=3)
    assert rewound is not None
    assert rewound["cursor"] == 5

    assert store.delete_subscription(run_id="run-1", subscriber_id="waiter-1") is True
    assert store.get_subscription(run_id="run-1", subscriber_id="waiter-1") is None


def test_terminal_log_offset_persistence(store: OrchestrationStore) -> None:
    assert store.get_terminal_log_offset(terminal_id="term-1") is None

    stored_offset = store.upsert_terminal_log_offset(terminal_id="term-1", byte_offset=123)
    assert stored_offset == 123
    assert store.get_terminal_log_offset(terminal_id="term-1") == 123

    # Negative offsets are normalized to 0.
    updated_offset = store.upsert_terminal_log_offset(terminal_id="term-1", byte_offset=-99)
    assert updated_offset == 0
    assert store.get_terminal_log_offset(terminal_id="term-1") == 0


def test_run_snapshot_aggregates_counts_and_active_ids(store: OrchestrationStore) -> None:
    store.create_run(run_id="run-1")

    store.create_job(
        job_id="job-created",
        run_id="run-1",
        agent_profile="developer",
        message="create",
        status=JobStatus.CREATED,
    )
    store.create_job(
        job_id="job-running",
        run_id="run-1",
        agent_profile="developer",
        message="run",
        status=JobStatus.RUNNING,
    )
    store.create_job(
        job_id="job-succeeded",
        run_id="run-1",
        agent_profile="reviewer",
        message="done",
        status=JobStatus.SUCCEEDED,
    )

    store.create_attempt(
        attempt_id="attempt-running",
        run_id="run-1",
        job_id="job-running",
        status=AttemptStatus.RUNNING,
    )
    store.create_attempt(
        attempt_id="attempt-failed",
        run_id="run-1",
        job_id="job-succeeded",
        status=AttemptStatus.FAILED,
    )

    event = store.append_event(run_id="run-1", event_type=EventType.RUN_CREATED)

    snapshot = store.get_run_snapshot(run_id="run-1")
    assert snapshot is not None
    assert snapshot.run_id == "run-1"
    assert snapshot.jobs_total == 3
    assert snapshot.attempts_total == 2
    assert snapshot.jobs_by_status.created == 1
    assert snapshot.jobs_by_status.running == 1
    assert snapshot.jobs_by_status.succeeded == 1
    assert snapshot.attempts_by_status.running == 1
    assert snapshot.attempts_by_status.failed == 1
    assert snapshot.active_job_ids == ["job-created", "job-running"]
    assert snapshot.active_attempt_ids == ["attempt-running"]
    assert snapshot.latest_event_cursor == event.event_id


def test_list_runs_supports_status_filter(store: OrchestrationStore) -> None:
    store.create_run(run_id="run-running", status=RunStatus.RUNNING)
    store.create_run(run_id="run-idle", status=RunStatus.IDLE)
    store.create_run(run_id="run-finalized", status=RunStatus.FINALIZED)

    all_runs = store.list_runs()
    assert {run.run_id for run in all_runs} == {"run-running", "run-idle", "run-finalized"}

    filtered = store.list_runs(statuses=[RunStatus.RUNNING, RunStatus.IDLE])
    assert {run.run_id for run in filtered} == {"run-running", "run-idle"}
