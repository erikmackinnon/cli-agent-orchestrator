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


def _seed_running_attempt(store: OrchestrationStore) -> None:
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
    _seed_running_attempt(store)

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
    _seed_running_attempt(store)

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

    runtime = OrchestrationRuntime(store=store, log_read_overlap_bytes=8 * 1024)
    await runtime.start()
    first_result = runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert first_result.markers_seen == 1
    assert first_result.markers_ingested == 1
    await runtime.stop()

    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"{marker_two}\n")

    restarted_runtime = OrchestrationRuntime(store=store, log_read_overlap_bytes=8 * 1024)
    await restarted_runtime.start()
    second_result = restarted_runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)

    assert second_result.markers_seen == 1
    assert second_result.markers_ingested == 1
    assert second_result.events_appended == 2
    assert second_result.duplicates == 0
    assert second_result.ingestion_failures == []
    assert second_result.parse_failures == []

    events = store.read_events(run_id="run-1", cursor=0)
    assert len(events) == 4

    await restarted_runtime.stop()


@pytest.mark.asyncio
async def test_runtime_ingests_marker_split_across_incremental_updates(
    store: OrchestrationStore, tmp_path: Path
) -> None:
    _seed_running_attempt(store)
    runtime = OrchestrationRuntime(store=store, log_read_overlap_bytes=8 * 1024)
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
            "nonce": "evt-runtime-split-1",
        }
    )
    split_index = len(marker) // 2
    first_chunk = marker[:split_index]
    second_chunk = marker[split_index:]

    log_path = tmp_path / "term-1.log"
    log_path.write_text(first_chunk, encoding="utf-8")

    first_result = runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert first_result.markers_seen == 0
    assert first_result.markers_ingested == 0
    assert first_result.events_appended == 0
    assert first_result.duplicates == 0

    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(second_chunk)

    second_result = runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert second_result.markers_seen == 1
    assert second_result.markers_ingested == 1
    assert second_result.events_appended == 2
    assert second_result.duplicates == 0
    assert second_result.ingestion_failures == []
    assert second_result.parse_failures == []

    attempt = store.get_attempt(attempt_id="attempt-1")
    assert attempt is not None
    assert attempt.status == AttemptStatus.SUCCEEDED.value

    job = store.get_job(job_id="job-1")
    assert job is not None
    assert job.status == JobStatus.SUCCEEDED.value

    events = store.read_events(run_id="run-1", cursor=0)
    assert len(events) == 2

    await runtime.stop()


@pytest.mark.asyncio
async def test_runtime_ingests_large_marker_split_across_multiple_updates(
    store: OrchestrationStore, tmp_path: Path
) -> None:
    _seed_running_attempt(store)
    runtime = OrchestrationRuntime(store=store, log_read_overlap_bytes=8 * 1024)
    await runtime.start()

    marker = encode_worker_callback_marker(
        {
            "version": 1,
            "run_id": "run-1",
            "job_id": "job-1",
            "attempt_id": "attempt-1",
            "type": "job.completed",
            "status": "succeeded",
            "result": {"summary": "x" * (24 * 1024)},
            "nonce": "evt-runtime-large-split",
        }
    )
    assert len(marker) > 20 * 1024

    first_chunk_end = 7 * 1024
    second_chunk_end = 14 * 1024
    first_chunk = marker[:first_chunk_end]
    second_chunk = marker[first_chunk_end:second_chunk_end]
    third_chunk = marker[second_chunk_end:]

    log_path = tmp_path / "term-1.log"
    log_path.write_text(first_chunk, encoding="utf-8")
    first_result = runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert first_result.markers_seen == 0
    assert first_result.markers_ingested == 0
    assert first_result.events_appended == 0

    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(second_chunk)
    second_result = runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert second_result.markers_seen == 0
    assert second_result.markers_ingested == 0
    assert second_result.events_appended == 0

    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(third_chunk)
    third_result = runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert third_result.markers_seen == 1
    assert third_result.markers_ingested == 1
    assert third_result.events_appended == 2
    assert third_result.duplicates == 0

    attempt = store.get_attempt(attempt_id="attempt-1")
    assert attempt is not None
    assert attempt.status == AttemptStatus.SUCCEEDED.value

    job = store.get_job(job_id="job-1")
    assert job is not None
    assert job.status == JobStatus.SUCCEEDED.value

    events = store.read_events(run_id="run-1", cursor=0)
    assert len(events) == 2

    await runtime.stop()


@pytest.mark.asyncio
async def test_runtime_incremental_reads_do_not_reingest_previous_marker(
    store: OrchestrationStore, tmp_path: Path
) -> None:
    _seed_running_attempt(store)
    runtime = OrchestrationRuntime(store=store, log_read_overlap_bytes=8 * 1024)
    await runtime.start()

    marker_running = encode_worker_callback_marker(
        {
            "version": 1,
            "run_id": "run-1",
            "job_id": "job-1",
            "attempt_id": "attempt-1",
            "type": "job.progress",
            "status": "running",
            "result": {"summary": "started"},
            "nonce": "evt-runtime-overlap-1",
        }
    )
    marker_completed = encode_worker_callback_marker(
        {
            "version": 1,
            "run_id": "run-1",
            "job_id": "job-1",
            "attempt_id": "attempt-1",
            "type": "job.completed",
            "status": "succeeded",
            "result": {"summary": "done"},
            "nonce": "evt-runtime-overlap-2",
        }
    )

    log_path = tmp_path / "term-1.log"
    log_path.write_text(f"{marker_running}\n", encoding="utf-8")
    first_result = runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert first_result.markers_seen == 1
    assert first_result.events_appended == 2
    assert first_result.duplicates == 0

    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"{marker_completed}\n")

    second_result = runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert second_result.markers_seen == 1
    assert second_result.markers_ingested == 1
    assert second_result.events_appended == 2
    assert second_result.duplicates == 0

    events = store.read_events(run_id="run-1", cursor=0)
    assert len(events) == 4

    await runtime.stop()


@pytest.mark.asyncio
async def test_runtime_ingests_split_ansi_wrapped_marker(
    store: OrchestrationStore, tmp_path: Path
) -> None:
    _seed_running_attempt(store)
    runtime = OrchestrationRuntime(store=store, log_read_overlap_bytes=8 * 1024)
    await runtime.start()

    marker = encode_worker_callback_marker(
        {
            "version": 1,
            "run_id": "run-1",
            "job_id": "job-1",
            "attempt_id": "attempt-1",
            "type": "job.completed",
            "status": "succeeded",
            "result": {"summary": "ansi wrapped"},
            "nonce": "evt-runtime-ansi-split",
        }
    )
    wrapped_marker = _wrap_marker_like_ansi_terminal_output(marker)
    split_index = len(wrapped_marker) // 2

    log_path = tmp_path / "term-1.log"
    log_path.write_text(wrapped_marker[:split_index], encoding="utf-8")

    first_result = runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert first_result.events_appended == 0
    assert first_result.markers_seen == 0

    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(wrapped_marker[split_index:])

    second_result = runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert second_result.markers_seen == 1
    assert second_result.markers_ingested == 1
    assert second_result.events_appended == 2
    assert second_result.parse_failures == []
    assert second_result.ingestion_failures == []

    events = store.read_events(run_id="run-1", cursor=0)
    assert len(events) == 2

    await runtime.stop()


@pytest.mark.asyncio
async def test_runtime_preserves_pending_marker_fragment_across_restart(
    store: OrchestrationStore, tmp_path: Path
) -> None:
    _seed_running_attempt(store)

    marker = encode_worker_callback_marker(
        {
            "version": 1,
            "run_id": "run-1",
            "job_id": "job-1",
            "attempt_id": "attempt-1",
            "type": "job.completed",
            "status": "succeeded",
            "result": {"summary": "done after restart"},
            "nonce": "evt-runtime-restart-split",
        }
    )
    split_index = len(marker) // 2
    first_chunk = marker[:split_index]
    second_chunk = marker[split_index:]

    log_path = tmp_path / "term-1.log"
    log_path.write_text(first_chunk, encoding="utf-8")

    runtime = OrchestrationRuntime(store=store, log_read_overlap_bytes=8 * 1024)
    await runtime.start()
    first_result = runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert first_result.markers_seen == 0
    assert first_result.markers_ingested == 0
    assert first_result.events_appended == 0
    assert store.get_terminal_log_offset(terminal_id="term-1") == 0
    await runtime.stop()

    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(second_chunk)

    restarted_runtime = OrchestrationRuntime(store=store, log_read_overlap_bytes=8 * 1024)
    await restarted_runtime.start()
    second_result = restarted_runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert second_result.markers_seen == 1
    assert second_result.markers_ingested == 1
    assert second_result.events_appended == 2
    assert second_result.duplicates == 0

    events = store.read_events(run_id="run-1", cursor=0)
    assert len(events) == 2

    await restarted_runtime.stop()


@pytest.mark.asyncio
async def test_runtime_drops_benign_marker_seed_tail_after_non_marker_continuation(
    store: OrchestrationStore, tmp_path: Path
) -> None:
    _seed_running_attempt(store)
    runtime = OrchestrationRuntime(store=store, log_read_overlap_bytes=8 * 1024)
    await runtime.start()

    benign_prefix = "worker output...\n"
    log_path = tmp_path / "term-1.log"
    log_path.write_text(f"{benign_prefix}⟦CAO-EVENT-", encoding="utf-8")

    first_result = runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert first_result.markers_seen == 0
    assert first_result.events_appended == 0
    assert store.get_terminal_log_offset(terminal_id="term-1") == log_path.stat().st_size

    with log_path.open("a", encoding="utf-8") as handle:
        handle.write("not-a-marker\n")

    second_result = runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert second_result.markers_seen == 0
    assert second_result.events_appended == 0
    assert store.get_terminal_log_offset(terminal_id="term-1") == log_path.stat().st_size

    marker = encode_worker_callback_marker(
        {
            "version": 1,
            "run_id": "run-1",
            "job_id": "job-1",
            "attempt_id": "attempt-1",
            "type": "job.completed",
            "status": "succeeded",
            "result": {"summary": "done after benign seed"},
            "nonce": "evt-runtime-benign-seed-tail",
        }
    )
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"{marker}\n")

    third_result = runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert third_result.markers_seen == 1
    assert third_result.markers_ingested == 1
    assert third_result.events_appended == 2
    assert third_result.duplicates == 0

    events = store.read_events(run_id="run-1", cursor=0)
    assert len(events) == 2

    await runtime.stop()


@pytest.mark.asyncio
async def test_runtime_preserves_seed_split_marker_across_multiple_appends(
    store: OrchestrationStore, tmp_path: Path
) -> None:
    _seed_running_attempt(store)
    runtime = OrchestrationRuntime(store=store, log_read_overlap_bytes=8 * 1024)
    await runtime.start()

    marker = encode_worker_callback_marker(
        {
            "version": 1,
            "run_id": "run-1",
            "job_id": "job-1",
            "attempt_id": "attempt-1",
            "type": "job.completed",
            "status": "succeeded",
            "result": {"summary": "done after seed split"},
            "nonce": "evt-runtime-seed-split",
        }
    )
    seed = "⟦CAO-EVENT-"
    assert marker.startswith(seed)
    seed_boundary = len(seed)
    second_boundary = seed_boundary + 8
    first_chunk = marker[:seed_boundary]
    second_chunk = marker[seed_boundary:second_boundary]
    third_chunk = marker[second_boundary:]

    log_path = tmp_path / "term-1.log"
    log_path.write_text(first_chunk, encoding="utf-8")
    first_result = runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert first_result.markers_seen == 0
    assert first_result.events_appended == 0
    assert store.get_terminal_log_offset(terminal_id="term-1") == 0

    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(second_chunk)
    second_result = runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert second_result.markers_seen == 0
    assert second_result.events_appended == 0
    assert store.get_terminal_log_offset(terminal_id="term-1") == 0

    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(third_chunk)
    third_result = runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert third_result.markers_seen == 1
    assert third_result.markers_ingested == 1
    assert third_result.events_appended == 2
    assert third_result.duplicates == 0

    events = store.read_events(run_id="run-1", cursor=0)
    assert len(events) == 2

    await runtime.stop()


@pytest.mark.asyncio
async def test_runtime_drops_benign_marker_seed_tail_after_restart(
    store: OrchestrationStore, tmp_path: Path
) -> None:
    _seed_running_attempt(store)
    benign_prefix = "worker output...\n"
    log_path = tmp_path / "term-1.log"
    log_path.write_text(f"{benign_prefix}⟦CAO-EVENT-", encoding="utf-8")

    first_runtime = OrchestrationRuntime(store=store, log_read_overlap_bytes=8 * 1024)
    await first_runtime.start()
    first_result = first_runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert first_result.markers_seen == 0
    assert store.get_terminal_log_offset(terminal_id="term-1") == log_path.stat().st_size
    await first_runtime.stop()

    restarted_runtime = OrchestrationRuntime(store=store, log_read_overlap_bytes=8 * 1024)
    await restarted_runtime.start()
    second_result = restarted_runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert second_result.markers_seen == 0
    assert second_result.events_appended == 0
    assert store.get_terminal_log_offset(terminal_id="term-1") == log_path.stat().st_size

    with log_path.open("a", encoding="utf-8") as handle:
        handle.write("still not a marker\n")

    third_result = restarted_runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert third_result.markers_seen == 0
    assert third_result.events_appended == 0
    assert store.get_terminal_log_offset(terminal_id="term-1") == log_path.stat().st_size

    marker = encode_worker_callback_marker(
        {
            "version": 1,
            "run_id": "run-1",
            "job_id": "job-1",
            "attempt_id": "attempt-1",
            "type": "job.completed",
            "status": "succeeded",
            "result": {"summary": "done after restart benign seed"},
            "nonce": "evt-runtime-benign-seed-restart",
        }
    )
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"{marker}\n")

    fourth_result = restarted_runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert fourth_result.markers_seen == 1
    assert fourth_result.markers_ingested == 1
    assert fourth_result.events_appended == 2
    assert fourth_result.duplicates == 0

    events = store.read_events(run_id="run-1", cursor=0)
    assert len(events) == 2

    await restarted_runtime.stop()


@pytest.mark.asyncio
async def test_runtime_duplicate_only_reread_does_not_notify_run_waiters(
    store: OrchestrationStore, tmp_path: Path
) -> None:
    _seed_running_attempt(store)
    marker = encode_worker_callback_marker(
        {
            "version": 1,
            "run_id": "run-1",
            "job_id": "job-1",
            "attempt_id": "attempt-1",
            "type": "job.completed",
            "status": "succeeded",
            "result": {"summary": "done"},
            "nonce": "evt-runtime-duplicate-only",
        }
    )
    log_path = tmp_path / "term-1.log"
    log_path.write_text(f"{marker}\n", encoding="utf-8")

    initial_runtime = OrchestrationRuntime(store=store, log_read_overlap_bytes=8 * 1024)
    await initial_runtime.start()
    first_result = initial_runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)
    assert first_result.events_appended == 2
    await initial_runtime.stop()

    # Force a duplicate-only ingest path by rewinding the persisted cursor.
    store.upsert_terminal_log_offset(terminal_id="term-1", byte_offset=0)

    reread_runtime = OrchestrationRuntime(store=store, log_read_overlap_bytes=8 * 1024)
    await reread_runtime.start()
    baseline_cursor = reread_runtime.current_signal_cursor(run_id="run-1")
    second_result = reread_runtime.ingest_log_update(terminal_id="term-1", log_path=log_path)

    assert second_result.markers_seen == 1
    assert second_result.markers_ingested == 1
    assert second_result.events_appended == 0
    assert second_result.duplicates == 2

    updated_cursor = await reread_runtime.wait_for_run_update(
        run_id="run-1",
        cursor=baseline_cursor,
        timeout_sec=1,
    )
    assert updated_cursor == baseline_cursor

    await reread_runtime.stop()
