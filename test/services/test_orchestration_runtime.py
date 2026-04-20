"""Tests for orchestration runtime bootstrap and in-process signaling."""

from pathlib import Path

import pytest

from cli_agent_orchestrator.clients.database import run_schema_migrations
from cli_agent_orchestrator.clients.orchestration_store import OrchestrationStore
from cli_agent_orchestrator.models.orchestration import AttemptStatus, JobStatus
from cli_agent_orchestrator.services.orchestration_callbacks import encode_worker_callback_marker
from cli_agent_orchestrator.services.orchestration_runtime import OrchestrationRuntime


@pytest.fixture
def store(tmp_path: Path) -> OrchestrationStore:
    db_file = tmp_path / "orchestration-runtime.db"
    run_schema_migrations(database_file=db_file)
    return OrchestrationStore(database_file=db_file)


@pytest.mark.asyncio
async def test_runtime_start_stop_and_run_signal(store: OrchestrationStore) -> None:
    runtime = OrchestrationRuntime(store=store)

    await runtime.start()
    assert runtime.is_running() is True

    run_id = "run-signal"
    baseline = runtime.current_signal_cursor(run_id=run_id)
    runtime.notify_run_update(run_id=run_id)

    updated = await runtime.wait_for_run_update(run_id=run_id, cursor=baseline, timeout_sec=1)
    assert updated > baseline

    await runtime.stop()
    assert runtime.is_running() is False


@pytest.mark.asyncio
async def test_runtime_ingests_existing_log_markers_on_first_observation(
    store: OrchestrationStore, tmp_path: Path
) -> None:
    store.create_run(run_id="run-1")
    store.create_job(
        job_id="job-1",
        run_id="run-1",
        agent_profile="developer",
        message="Implement feature",
        status=JobStatus.RUNNING,
    )
    store.create_attempt(
        attempt_id="attempt-1",
        run_id="run-1",
        job_id="job-1",
        status=AttemptStatus.RUNNING,
    )

    runtime = OrchestrationRuntime(store=store)
    await runtime.start()

    marker = encode_worker_callback_marker(
        {
            "version": 1,
            "run_id": "run-1",
            "job_id": "job-1",
            "attempt_id": "attempt-1",
            "type": "job.completed",
            "status": "succeeded",
            "result": {"summary": "done"},
            "nonce": "evt-runtime-1",
        }
    )

    log_path = tmp_path / "term-1.log"
    log_path.write_text(f"initial\n{marker}\n", encoding="utf-8")

    baseline_cursor = runtime.current_signal_cursor(run_id="run-1")
    first_result = runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)

    assert first_result.markers_seen == 1
    assert first_result.markers_ingested == 1
    assert first_result.events_appended == 2
    assert first_result.parse_failures == []
    assert first_result.ingestion_failures == []

    updated_cursor = await runtime.wait_for_run_update(
        run_id="run-1",
        cursor=baseline_cursor,
        timeout_sec=1,
    )
    assert updated_cursor > baseline_cursor

    events = store.read_events(run_id="run-1", cursor=0)
    assert len(events) == 2

    await runtime.stop()


@pytest.mark.asyncio
async def test_runtime_uses_persisted_offsets_across_restart(
    store: OrchestrationStore, tmp_path: Path
) -> None:
    store.create_run(run_id="run-1")
    store.create_job(
        job_id="job-1",
        run_id="run-1",
        agent_profile="developer",
        message="Implement feature",
        status=JobStatus.RUNNING,
    )
    store.create_attempt(
        attempt_id="attempt-1",
        run_id="run-1",
        job_id="job-1",
        status=AttemptStatus.RUNNING,
    )

    marker_one = encode_worker_callback_marker(
        {
            "version": 1,
            "run_id": "run-1",
            "job_id": "job-1",
            "attempt_id": "attempt-1",
            "type": "job.progress",
            "status": "running",
            "result": {"summary": "started"},
            "nonce": "evt-runtime-restart-1",
        }
    )
    marker_two = encode_worker_callback_marker(
        {
            "version": 1,
            "run_id": "run-1",
            "job_id": "job-1",
            "attempt_id": "attempt-1",
            "type": "job.completed",
            "status": "succeeded",
            "result": {"summary": "done"},
            "nonce": "evt-runtime-restart-2",
        }
    )

    log_path = tmp_path / "term-1.log"
    log_path.write_text(f"{marker_one}\n", encoding="utf-8")

    runtime = OrchestrationRuntime(store=store)
    await runtime.start()
    first_result = runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert first_result.markers_seen == 1
    assert first_result.markers_ingested == 1
    await runtime.stop()

    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"{marker_two}\n")

    restarted_runtime = OrchestrationRuntime(store=store)
    await restarted_runtime.start()
    second_result = restarted_runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)

    assert second_result.markers_seen == 1
    assert second_result.markers_ingested == 1
    assert second_result.ingestion_failures == []
    assert second_result.parse_failures == []

    events = store.read_events(run_id="run-1", cursor=0)
    assert len(events) == 4

    await restarted_runtime.stop()
