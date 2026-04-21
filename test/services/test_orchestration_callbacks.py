"""Tests for orchestration callback marker protocol and ingestion."""

import base64
import json
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


def _wrap_marker_like_terminal_output(marker: str) -> str:
    assert marker.startswith("⟦CAO-EVENT-v1:")
    assert marker.endswith("⟧")
    payload = marker[len("⟦CAO-EVENT-v1:") : -1]
    return f"⟦CAO-EVENT-\nv1:{payload[:18]}\n{payload[18:44]}\n{payload[44:]}⟧"


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


def _encode_raw_marker(payload: dict[str, object]) -> str:
    payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    encoded = base64.urlsafe_b64encode(payload_json.encode("utf-8")).decode("ascii").rstrip("=")
    return f"⟦CAO-EVENT-v1:{encoded}⟧"


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


@pytest.mark.parametrize("raw_version", [1, "1", "v1"])
def test_parse_marker_accepts_live_version_shapes(raw_version: object) -> None:
    marker = _encode_raw_marker(
        {
            "version": raw_version,
            "run_id": "run-1",
            "job_id": "job-1",
            "attempt_id": "attempt-1",
            "type": "job.completed",
            "status": "succeeded",
            "result": {"summary": "done"},
            "nonce": "evt-live",
        }
    )

    parsed = parse_worker_callback_marker(marker)

    assert parsed.version == 1
    assert parsed.run_id == "run-1"
    assert parsed.nonce == "evt-live"


@pytest.mark.parametrize("raw_version", [2, "2", "v2", "one", None, True])
def test_parse_marker_rejects_unsupported_version_shapes(raw_version: object) -> None:
    marker = _encode_raw_marker(
        {
            "version": raw_version,
            "run_id": "run-1",
            "job_id": "job-1",
            "attempt_id": "attempt-1",
            "type": "job.completed",
            "status": "succeeded",
            "result": {"summary": "done"},
            "nonce": "evt-bad-version",
        }
    )

    with pytest.raises(ValueError, match="invalid_marker_payload_shape"):
        parse_worker_callback_marker(marker)


def test_extract_markers_reports_malformed_framing() -> None:
    output = "some text ⟦CAO-EVENT-v1:not_base64!⟧"

    parsed, failures = extract_worker_callback_markers(output)

    assert parsed == []
    assert len(failures) == 1
    assert failures[0].code == "invalid_marker_framing"


def test_extract_markers_parses_wrapped_marker_shape() -> None:
    marker = encode_worker_callback_marker(
        {
            "version": 1,
            "run_id": "run-1",
            "job_id": "job-1",
            "attempt_id": "attempt-1",
            "type": "job.completed",
            "status": "succeeded",
            "result": {"summary": "done"},
            "nonce": "evt-wrapped",
        }
    )
    wrapped_marker = _wrap_marker_like_terminal_output(marker)

    parsed, failures = extract_worker_callback_markers(f"prefix\n{wrapped_marker}\nsuffix")

    assert len(parsed) == 1
    assert failures == []
    assert parsed[0].raw_marker == wrapped_marker
    assert parsed[0].marker.run_id == "run-1"
    assert parsed[0].marker.nonce == "evt-wrapped"

    parsed_direct = parse_worker_callback_marker(wrapped_marker)
    assert parsed_direct.run_id == "run-1"
    assert parsed_direct.nonce == "evt-wrapped"


def test_extract_markers_parses_ansi_wrapped_marker_shape() -> None:
    marker = encode_worker_callback_marker(
        {
            "version": 1,
            "run_id": "run-1",
            "job_id": "job-1",
            "attempt_id": "attempt-1",
            "type": "job.completed",
            "status": "succeeded",
            "result": {"summary": "done"},
            "nonce": "evt-ansi-wrapped",
        }
    )
    wrapped_marker = _wrap_marker_like_ansi_terminal_output(marker)

    parsed, failures = extract_worker_callback_markers(f"prefix\n{wrapped_marker}\nsuffix")

    assert len(parsed) == 1
    assert failures == []
    assert parsed[0].marker.run_id == "run-1"
    assert parsed[0].marker.nonce == "evt-ansi-wrapped"


@pytest.mark.parametrize("injected_whitespace", [" ", "\t"])
def test_extract_rejects_marker_with_injected_payload_whitespace(
    injected_whitespace: str,
) -> None:
    marker = encode_worker_callback_marker(
        {
            "version": 1,
            "run_id": "run-1",
            "job_id": "job-1",
            "attempt_id": "attempt-1",
            "type": "job.completed",
            "status": "succeeded",
            "result": {"summary": "done"},
            "nonce": "evt-injected-whitespace",
        }
    )
    payload = marker[len("⟦CAO-EVENT-v1:") : -1]
    malformed = f"⟦CAO-EVENT-v1:{payload[:16]}" f"{injected_whitespace}" f"{payload[16:]}⟧"

    parsed, failures = extract_worker_callback_markers(f"prefix {malformed} suffix")

    assert parsed == []
    assert len(failures) == 1
    assert failures[0].code == "invalid_marker_framing"


def test_parse_marker_accepts_crlf_wrapped_marker() -> None:
    marker = encode_worker_callback_marker(
        {
            "version": 1,
            "run_id": "run-1",
            "job_id": "job-1",
            "attempt_id": "attempt-1",
            "type": "job.completed",
            "status": "succeeded",
            "result": {"summary": "done"},
            "nonce": "evt-crlf",
        }
    )
    payload = marker[len("⟦CAO-EVENT-v1:") : -1]
    wrapped_marker = f"⟦CAO-EVENT-\r\nv1:{payload[:20]}\r\n{payload[20:]}⟧"

    parsed = parse_worker_callback_marker(wrapped_marker)

    assert parsed.run_id == "run-1"
    assert parsed.nonce == "evt-crlf"


@pytest.mark.parametrize(
    ("marker_text", "error_code"),
    [
        (
            "⟦CAO-EVENT- v1:abc⟧",
            "invalid_marker_framing",
        ),
        (
            "⟦CAO-EVENT-v1:ab\tcd⟧",
            "invalid_marker_framing",
        ),
    ],
)
def test_parse_marker_rejects_disallowed_internal_whitespace(
    marker_text: str,
    error_code: str,
) -> None:
    with pytest.raises(ValueError, match=error_code):
        parse_worker_callback_marker(marker_text)


def test_extract_wrapped_malformed_marker_reports_framing_failure() -> None:
    output = "log line ⟦CAO-EVENT-\nv1:not_base64!⟧"

    parsed, failures = extract_worker_callback_markers(output)

    assert parsed == []
    assert len(failures) == 1
    assert failures[0].code == "invalid_marker_framing"


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
    assert 'Set version to integer 1 (not string "1" and not string "v1").' in message


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
    assert result.run_ids_with_new_events == ["run-1"]

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


def test_ingestion_persists_wrapped_completion_marker(seeded_store: OrchestrationStore) -> None:
    ingestor = OrchestrationCallbackIngestor(store=seeded_store)
    marker = encode_worker_callback_marker(
        {
            "version": 1,
            "run_id": "run-1",
            "job_id": "job-1",
            "attempt_id": "attempt-1",
            "type": "job.completed",
            "status": "succeeded",
            "result": {"summary": "Implemented wrapped marker changes"},
            "nonce": "evt-wrapped-ingest",
        }
    )
    wrapped_marker = _wrap_marker_like_terminal_output(marker)

    result = ingestor.ingest_terminal_output(
        terminal_id="term-1",
        output=f"worker output:\n{wrapped_marker}",
    )

    assert result.markers_seen == 1
    assert result.markers_ingested == 1
    assert result.events_appended == 2
    assert result.parse_failures == []
    assert result.ingestion_failures == []

    attempt = seeded_store.get_attempt(attempt_id="attempt-1")
    assert attempt is not None
    assert attempt.status == AttemptStatus.SUCCEEDED.value
    assert attempt.nonce == "evt-wrapped-ingest"

    events = seeded_store.read_events(run_id="run-1", cursor=0)
    assert [event.event_type for event in events] == [
        EventType.ATTEMPT_SUCCEEDED.value,
        EventType.JOB_SUCCEEDED.value,
    ]


def test_ingestion_persists_ansi_wrapped_completion_marker(
    seeded_store: OrchestrationStore,
) -> None:
    ingestor = OrchestrationCallbackIngestor(store=seeded_store)
    marker = encode_worker_callback_marker(
        {
            "version": 1,
            "run_id": "run-1",
            "job_id": "job-1",
            "attempt_id": "attempt-1",
            "type": "job.completed",
            "status": "succeeded",
            "result": {"summary": "Implemented ANSI wrapped marker changes"},
            "nonce": "evt-ansi-wrapped-ingest",
        }
    )
    wrapped_marker = _wrap_marker_like_ansi_terminal_output(marker)

    result = ingestor.ingest_terminal_output(
        terminal_id="term-1",
        output=f"worker output:\n{wrapped_marker}",
    )

    assert result.markers_seen == 1
    assert result.markers_ingested == 1
    assert result.events_appended == 2
    assert result.parse_failures == []
    assert result.ingestion_failures == []

    attempt = seeded_store.get_attempt(attempt_id="attempt-1")
    assert attempt is not None
    assert attempt.status == AttemptStatus.SUCCEEDED.value
    assert attempt.nonce == "evt-ansi-wrapped-ingest"


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
    assert first.run_ids_with_new_events == ["run-1"]
    assert second.events_appended == 0
    assert second.duplicates == 2
    assert second.run_ids_with_new_events == []

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
