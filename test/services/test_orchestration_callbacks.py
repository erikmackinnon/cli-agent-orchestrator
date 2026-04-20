"""Tests for orchestration callback marker protocol and ingestion."""

from pathlib import Path

import pytest

from cli_agent_orchestrator.clients.database import run_schema_migrations
from cli_agent_orchestrator.clients.orchestration_store import OrchestrationStore
from cli_agent_orchestrator.models.orchestration import AttemptStatus, EventType, JobStatus
from cli_agent_orchestrator.services.orchestration_callbacks import (
    OrchestrationCallbackIngestor,
    WorkerCallbackMarker,
    encode_worker_callback_marker,
    extract_worker_callback_markers,
    inject_orchestration_worker_instructions,
    parse_worker_callback_marker,
)


@pytest.fixture
def store(tmp_path: Path) -> OrchestrationStore:
    db_file = tmp_path / "orchestration-callbacks.db"
    run_schema_migrations(database_file=db_file)
    return OrchestrationStore(database_file=db_file)


@pytest.fixture
def seeded_store(store: OrchestrationStore) -> OrchestrationStore:
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
    return store


def test_marker_round_trip_parse() -> None:
    payload = WorkerCallbackMarker(
        version=1,
        run_id="run-1",
        job_id="job-1",
        attempt_id="attempt-1",
        type="job.completed",
        status="succeeded",
        result={"summary": "done"},
        nonce="evt-1",
    )

    marker = encode_worker_callback_marker(payload)
    parsed = parse_worker_callback_marker(marker)

    assert parsed.version == 1
    assert parsed.run_id == "run-1"
    assert parsed.job_id == "job-1"
    assert parsed.attempt_id == "attempt-1"
    assert parsed.type == "job.completed"
    assert parsed.status == "succeeded"
    assert parsed.result == {"summary": "done"}
    assert parsed.nonce == "evt-1"


def test_extract_markers_reports_malformed_base64() -> None:
    output = "some text ⟦CAO-EVENT-v1:not_base64!⟧"

    parsed, failures = extract_worker_callback_markers(output)

    assert parsed == []
    assert len(failures) == 1
    assert failures[0].code == "invalid_marker_base64"


def test_prompt_injection_helper_contains_stable_ids() -> None:
    message = inject_orchestration_worker_instructions(
        message="Implement endpoint",
        run_id="run-1",
        job_id="job-1",
        attempt_id="attempt-1",
        chain_id="chain-1",
    )

    assert "You are part of CAO orchestration run run-1." in message
    assert "Your job_id is job-1." in message
    assert "Your attempt_id is attempt-1." in message
    assert "Your chain_id is chain-1." in message
    assert "⟦CAO-EVENT-v1:<base64url-json>⟧" in message


def test_ingestion_persists_lifecycle_events(seeded_store: OrchestrationStore) -> None:
    ingestor = OrchestrationCallbackIngestor(store=seeded_store)
    marker = encode_worker_callback_marker(
        {
            "version": 1,
            "run_id": "run-1",
            "job_id": "job-1",
            "attempt_id": "attempt-1",
            "type": "job.completed",
            "status": "succeeded",
            "result": {"summary": "Implemented changes", "artifacts": ["src/file.py"]},
            "nonce": "evt-1",
        }
    )

    result = ingestor.ingest_terminal_output(terminal_id="term-1", output=f"prefix {marker} suffix")

    assert result.markers_seen == 1
    assert result.markers_ingested == 1
    assert result.events_appended == 2
    assert result.duplicates == 0
    assert result.parse_failures == []
    assert result.ingestion_failures == []
    assert result.affected_run_ids == ["run-1"]

    attempt = seeded_store.get_attempt(attempt_id="attempt-1")
    assert attempt is not None
    assert attempt.status == AttemptStatus.SUCCEEDED.value
    assert attempt.terminal_id == "term-1"
    assert attempt.nonce == "evt-1"
    assert attempt.result_summary == "Implemented changes"
    assert attempt.result_data == {"summary": "Implemented changes", "artifacts": ["src/file.py"]}

    job = seeded_store.get_job(job_id="job-1")
    assert job is not None
    assert job.status == JobStatus.SUCCEEDED.value

    events = seeded_store.read_events(run_id="run-1", cursor=0)
    assert [event.event_type for event in events] == [
        EventType.ATTEMPT_SUCCEEDED.value,
        EventType.JOB_SUCCEEDED.value,
    ]
    assert events[0].payload.metadata == {
        "marker_type": "job.completed",
        "marker_status": "succeeded",
        "marker_version": 1,
        "marker_nonce": "evt-1",
        "source_terminal_id": "term-1",
    }


def test_ingestion_dedupes_repeated_markers(seeded_store: OrchestrationStore) -> None:
    ingestor = OrchestrationCallbackIngestor(store=seeded_store)
    marker = encode_worker_callback_marker(
        {
            "version": 1,
            "run_id": "run-1",
            "job_id": "job-1",
            "attempt_id": "attempt-1",
            "type": "job.completed",
            "status": "succeeded",
            "result": {"summary": "Implemented changes"},
            "nonce": "evt-dedupe",
        }
    )

    first = ingestor.ingest_terminal_output(terminal_id="term-1", output=marker)
    second = ingestor.ingest_terminal_output(terminal_id="term-1", output=marker)

    assert first.events_appended == 2
    assert first.duplicates == 0
    assert second.events_appended == 0
    assert second.duplicates == 2

    events = seeded_store.read_events(run_id="run-1", cursor=0)
    assert len(events) == 2


def test_ingestion_handles_unknown_run_deterministically(store: OrchestrationStore) -> None:
    ingestor = OrchestrationCallbackIngestor(store=store)
    marker = encode_worker_callback_marker(
        {
            "version": 1,
            "run_id": "run-missing",
            "job_id": "job-1",
            "attempt_id": "attempt-1",
            "type": "job.completed",
            "status": "succeeded",
            "result": {"summary": "done"},
            "nonce": "evt-unknown",
        }
    )

    result = ingestor.ingest_terminal_output(terminal_id="term-1", output=marker)

    assert result.markers_seen == 1
    assert result.markers_ingested == 0
    assert result.events_appended == 0
    assert result.duplicates == 0
    assert len(result.ingestion_failures) == 1
    assert result.ingestion_failures[0].code == "ingestion_validation_error"
    assert result.ingestion_failures[0].detail == "unknown_run_id:run-missing"
