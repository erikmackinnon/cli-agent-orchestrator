"""Worker callback marker parsing and durable orchestration event ingestion."""

from __future__ import annotations

import base64
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from cli_agent_orchestrator.clients.orchestration_store import OrchestrationStore
from cli_agent_orchestrator.models.orchestration import (
    AttemptStatePayload,
    AttemptStatus,
    EventType,
    JobStatePayload,
    JobStatus,
    OrchestrationEventPayload,
)

logger = logging.getLogger(__name__)

MARKER_PREFIX = "⟦CAO-EVENT-v1:"
MARKER_SUFFIX = "⟧"
MARKER_PATTERN = re.compile(r"⟦[^⟧]+⟧")
EXACT_MARKER_PATTERN = re.compile(r"^⟦CAO-EVENT-v1:(?P<encoded>[A-Za-z0-9_-]+)⟧$")
CANONICAL_MARKER_VERSION = 1
LEGACY_MARKER_VERSION_STRINGS = {"1", "v1"}
# Strip common ANSI/VT100 escape sequences that may appear in tmux log captures.
ANSI_ESCAPE_PATTERN = re.compile(
    r"\x1B(?:"
    r"\[[0-?]*[ -/]*[@-~]"  # CSI
    r"|\][^\x1B\x07]*(?:\x1B\\|\x07)"  # OSC
    r"|[@-Z\\-_]"  # 7-bit C1 controls
    r")"
)

# Prompt fragment for orchestration-spawned workers only.
# This must not alter legacy assign/handoff prompt behavior.
ORCHESTRATION_WORKER_INSTRUCTIONS_TEMPLATE = """You are part of CAO orchestration run {run_id}.
Your job_id is {job_id}.
Your attempt_id is {attempt_id}.
Your chain_id is {chain_id}.

When finished, emit exactly one CAO completion marker using this format:
{marker_format}

The marker JSON payload must include: version, run_id, job_id, attempt_id, type, status, result, nonce.
Set version to integer 1 (not string "1" and not string "v1").
Do not emit the completion marker until the work is actually done.
If blocked or unable to complete, emit a failure marker with failure_type and explanation in result.
"""


class WorkerCallbackMarker(BaseModel):
    """Validated CAO callback marker payload."""

    model_config = ConfigDict(extra="forbid")

    version: Literal[1]
    run_id: str = Field(min_length=1)
    job_id: str = Field(min_length=1)
    attempt_id: str = Field(min_length=1)
    type: str = Field(min_length=1)
    status: str = Field(min_length=1)
    result: Optional[Any] = None
    nonce: str = Field(min_length=1)

    @field_validator("version", mode="before")
    @classmethod
    def _normalize_marker_version(cls, value: Any) -> int:
        if isinstance(value, bool):
            raise ValueError("unsupported_marker_version")
        if isinstance(value, int) and value == CANONICAL_MARKER_VERSION:
            return CANONICAL_MARKER_VERSION
        if isinstance(value, str) and value.strip().lower() in LEGACY_MARKER_VERSION_STRINGS:
            return CANONICAL_MARKER_VERSION
        raise ValueError("unsupported_marker_version")


@dataclass(frozen=True)
class MarkerParseFailure:
    """Deterministic parse failure for a marker candidate."""

    code: str
    marker: str
    detail: str


@dataclass(frozen=True)
class ParsedWorkerCallbackMarker:
    """A parsed marker plus the original marker text."""

    marker: WorkerCallbackMarker
    raw_marker: str


@dataclass
class CallbackIngestionResult:
    """Outcome of callback ingestion for a terminal output chunk."""

    markers_seen: int = 0
    markers_ingested: int = 0
    events_appended: int = 0
    duplicates: int = 0
    parse_failures: List[MarkerParseFailure] = field(default_factory=list)
    ingestion_failures: List[MarkerParseFailure] = field(default_factory=list)
    affected_run_ids: List[str] = field(default_factory=list)
    run_ids_with_new_events: List[str] = field(default_factory=list)


def _base64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(f"{data}{padding}")


def _base64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _strip_terminal_artifacts(text: str) -> str:
    """Remove ANSI escape/control sequences from marker text."""

    return ANSI_ESCAPE_PATTERN.sub("", text)


def _canonicalize_marker_text(
    marker_text: str,
    *,
    collapse_wrapped_whitespace: bool = False,
) -> str:
    """Normalize terminal-wrapped marker text into its canonical single-line form."""

    stripped = marker_text.strip()
    if not (stripped.startswith("⟦") and stripped.endswith("⟧")):
        return stripped

    inner = _strip_terminal_artifacts(stripped[1:-1])
    if collapse_wrapped_whitespace:
        # Real tmux logs can indent wrapped lines inside the marker payload.
        # Only remove hard wraps and leading spaces on continuation lines;
        # do not delete arbitrary internal whitespace.
        lines = inner.replace("\r", "").split("\n")
        if len(lines) == 1:
            inner_no_linebreaks = lines[0]
        else:
            rebuilt = [lines[0]]
            rebuilt.extend(line.lstrip(" ") for line in lines[1:])
            inner_no_linebreaks = "".join(rebuilt)
    else:
        # Strict parsing: only normalize hard line breaks.
        inner_no_linebreaks = inner.replace("\n", "").replace("\r", "")
    return f"⟦{inner_no_linebreaks}⟧"


def parse_worker_callback_marker(marker_text: str) -> WorkerCallbackMarker:
    """Parse one full callback marker into a validated payload."""

    marker_text = _canonicalize_marker_text(marker_text)
    marker_match = EXACT_MARKER_PATTERN.match(marker_text)
    if marker_match is None:
        raise ValueError("invalid_marker_framing")

    encoded_payload = marker_match.group("encoded")

    try:
        payload_json = _base64url_decode(encoded_payload).decode("utf-8")
    except Exception as exc:
        raise ValueError("invalid_marker_base64") from exc

    try:
        decoded = json.loads(payload_json)
    except json.JSONDecodeError as exc:
        raise ValueError("invalid_marker_json") from exc

    if not isinstance(decoded, dict):
        raise ValueError("invalid_marker_payload_shape")

    try:
        return WorkerCallbackMarker.model_validate(decoded)
    except ValidationError as exc:
        raise ValueError("invalid_marker_payload_shape") from exc


def extract_worker_callback_markers(
    output_text: str,
) -> Tuple[List[ParsedWorkerCallbackMarker], List[MarkerParseFailure]]:
    """Extract and parse callback markers from terminal output."""

    parsed: List[ParsedWorkerCallbackMarker] = []
    failures: List[MarkerParseFailure] = []

    for marker_match in MARKER_PATTERN.finditer(output_text):
        raw_marker = marker_match.group(0)
        canonical_marker = _canonicalize_marker_text(
            raw_marker,
            collapse_wrapped_whitespace=True,
        )
        if not canonical_marker.startswith(MARKER_PREFIX):
            continue

        try:
            parsed_marker = parse_worker_callback_marker(canonical_marker)
        except ValueError as exc:
            failures.append(MarkerParseFailure(code=str(exc), marker=raw_marker, detail=str(exc)))
            continue

        parsed.append(ParsedWorkerCallbackMarker(marker=parsed_marker, raw_marker=raw_marker))

    return parsed, failures


def encode_worker_callback_marker(payload: WorkerCallbackMarker | Dict[str, Any]) -> str:
    """Encode a validated payload as a CAO callback marker string."""

    marker_payload = WorkerCallbackMarker.model_validate(payload)
    payload_json = json.dumps(
        marker_payload.model_dump(mode="json", exclude_none=False),
        separators=(",", ":"),
        sort_keys=True,
    )
    encoded = _base64url_encode(payload_json.encode("utf-8"))
    return f"{MARKER_PREFIX}{encoded}{MARKER_SUFFIX}"


def build_orchestration_worker_instructions(
    *,
    run_id: str,
    job_id: str,
    attempt_id: str,
    chain_id: Optional[str] = None,
) -> str:
    """Build orchestration-only callback instructions for worker prompt injection."""

    return ORCHESTRATION_WORKER_INSTRUCTIONS_TEMPLATE.format(
        run_id=run_id,
        job_id=job_id,
        attempt_id=attempt_id,
        chain_id=chain_id or "none",
        marker_format=f"{MARKER_PREFIX}<base64url-json>{MARKER_SUFFIX}",
    ).strip()


def inject_orchestration_worker_instructions(
    *,
    message: str,
    run_id: str,
    job_id: str,
    attempt_id: str,
    chain_id: Optional[str] = None,
) -> str:
    """Append orchestration callback instructions to a worker message."""

    instructions = build_orchestration_worker_instructions(
        run_id=run_id,
        job_id=job_id,
        attempt_id=attempt_id,
        chain_id=chain_id,
    )
    stripped_message = message.rstrip()
    if not stripped_message:
        return instructions
    return f"{stripped_message}\n\n{instructions}"


class OrchestrationCallbackIngestor:
    """Parse callback markers and persist normalized lifecycle events."""

    _STATUS_MAP: Dict[str, tuple[AttemptStatus, EventType, JobStatus, EventType, bool]] = {
        "running": (
            AttemptStatus.RUNNING,
            EventType.ATTEMPT_STARTED,
            JobStatus.RUNNING,
            EventType.JOB_STARTED,
            False,
        ),
        "succeeded": (
            AttemptStatus.SUCCEEDED,
            EventType.ATTEMPT_SUCCEEDED,
            JobStatus.SUCCEEDED,
            EventType.JOB_SUCCEEDED,
            True,
        ),
        "failed": (
            AttemptStatus.FAILED,
            EventType.ATTEMPT_FAILED,
            JobStatus.FAILED,
            EventType.JOB_FAILED,
            True,
        ),
        "cancelled": (
            AttemptStatus.CANCELLED,
            EventType.ATTEMPT_CANCELLED,
            JobStatus.CANCELLED,
            EventType.JOB_CANCELLED,
            True,
        ),
        "timed_out": (
            AttemptStatus.TIMED_OUT,
            EventType.ATTEMPT_TIMED_OUT,
            JobStatus.TIMED_OUT,
            EventType.JOB_TIMED_OUT,
            True,
        ),
    }

    def __init__(self, store: OrchestrationStore):
        self._store = store

    def ingest_terminal_output(self, *, terminal_id: str, output: str) -> CallbackIngestionResult:
        """Ingest callback markers found in terminal output text."""

        parsed_markers, parse_failures = extract_worker_callback_markers(output)
        result = CallbackIngestionResult(
            markers_seen=len(parsed_markers) + len(parse_failures),
            parse_failures=parse_failures,
        )

        for parsed_marker in parsed_markers:
            try:
                appended_events, duplicate_events = self._ingest_parsed_marker(
                    terminal_id=terminal_id,
                    parsed=parsed_marker,
                )
            except ValueError as exc:
                result.ingestion_failures.append(
                    MarkerParseFailure(
                        code="ingestion_validation_error",
                        marker=parsed_marker.raw_marker,
                        detail=str(exc),
                    )
                )
                continue

            result.markers_ingested += 1
            result.events_appended += appended_events
            result.duplicates += duplicate_events
            if parsed_marker.marker.run_id not in result.affected_run_ids:
                result.affected_run_ids.append(parsed_marker.marker.run_id)
            if (
                appended_events > 0
                and parsed_marker.marker.run_id not in result.run_ids_with_new_events
            ):
                result.run_ids_with_new_events.append(parsed_marker.marker.run_id)

        return result

    def _ingest_parsed_marker(
        self,
        *,
        terminal_id: str,
        parsed: ParsedWorkerCallbackMarker,
    ) -> tuple[int, int]:
        marker = parsed.marker

        run = self._store.get_run(run_id=marker.run_id)
        if run is None:
            raise ValueError(f"unknown_run_id:{marker.run_id}")

        job = self._store.get_job(job_id=marker.job_id)
        if job is None:
            raise ValueError(f"unknown_job_id:{marker.job_id}")
        if job.run_id != marker.run_id:
            raise ValueError("job_run_mismatch")

        attempt = self._store.get_attempt(attempt_id=marker.attempt_id)
        if attempt is None:
            raise ValueError(f"unknown_attempt_id:{marker.attempt_id}")
        if attempt.run_id != marker.run_id:
            raise ValueError("attempt_run_mismatch")
        if attempt.job_id != marker.job_id:
            raise ValueError("attempt_job_mismatch")

        status_key = marker.status.strip().lower()
        status_mapping = self._STATUS_MAP.get(status_key)
        if status_mapping is None:
            raise ValueError(f"unsupported_status:{marker.status}")

        attempt_status, attempt_event_type, job_status, job_event_type, is_terminal = status_mapping

        result_summary, result_data, reason = self._normalize_result(marker.result)
        now = datetime.now(timezone.utc)

        if is_terminal:
            self._store.update_attempt(
                attempt_id=marker.attempt_id,
                status=attempt_status,
                terminal_id=terminal_id,
                nonce=marker.nonce,
                result_summary=result_summary,
                result_data=result_data,
                completed_at=now,
            )
        else:
            self._store.update_attempt(
                attempt_id=marker.attempt_id,
                status=attempt_status,
                terminal_id=terminal_id,
                nonce=marker.nonce,
                result_summary=result_summary,
                result_data=result_data,
                started_at=now,
            )

        self._store.update_job(job_id=marker.job_id, status=job_status)

        metadata: Dict[str, Any] = {
            "marker_type": marker.type,
            "marker_status": status_key,
            "marker_version": marker.version,
            "marker_nonce": marker.nonce,
            "source_terminal_id": terminal_id,
        }

        attempt_payload = OrchestrationEventPayload(
            attempt=AttemptStatePayload(
                status=attempt_status,
                terminal_id=terminal_id,
                nonce=marker.nonce,
                result_summary=result_summary,
                result_data=result_data,
            ),
            metadata=metadata,
        )

        job_payload = OrchestrationEventPayload(
            job=JobStatePayload(
                status=job_status,
                terminal_id=terminal_id,
                reason=reason,
            ),
            metadata=metadata,
        )

        dedupe_base = f"{marker.run_id}:{marker.job_id}:{marker.attempt_id}:{marker.nonce}"
        attempt_dedupe_key = f"callback:{dedupe_base}:attempt:{attempt_event_type.value}"
        job_dedupe_key = f"callback:{dedupe_base}:job:{job_event_type.value}"

        appended_events = 0
        duplicate_events = 0

        appended, duplicate = self._append_event_with_dedupe(
            run_id=marker.run_id,
            job_id=marker.job_id,
            attempt_id=marker.attempt_id,
            event_type=attempt_event_type,
            payload=attempt_payload,
            dedupe_key=attempt_dedupe_key,
        )
        appended_events += appended
        duplicate_events += duplicate

        appended, duplicate = self._append_event_with_dedupe(
            run_id=marker.run_id,
            job_id=marker.job_id,
            attempt_id=marker.attempt_id,
            event_type=job_event_type,
            payload=job_payload,
            dedupe_key=job_dedupe_key,
        )
        appended_events += appended
        duplicate_events += duplicate

        return appended_events, duplicate_events

    def _append_event_with_dedupe(
        self,
        *,
        run_id: str,
        job_id: str,
        attempt_id: str,
        event_type: EventType,
        payload: OrchestrationEventPayload,
        dedupe_key: str,
    ) -> tuple[int, int]:
        existing = self._store.get_event_by_dedupe_key(run_id=run_id, dedupe_key=dedupe_key)
        if existing is not None:
            return 0, 1

        self._store.append_event(
            run_id=run_id,
            job_id=job_id,
            attempt_id=attempt_id,
            event_type=event_type,
            payload=payload,
            dedupe_key=dedupe_key,
        )
        return 1, 0

    @staticmethod
    def _normalize_result(
        result: Optional[Any],
    ) -> tuple[Optional[str], Optional[Dict[str, Any]], Optional[str]]:
        if result is None:
            return None, None, None

        if isinstance(result, dict):
            summary = result.get("summary") if isinstance(result.get("summary"), str) else None
            reason = None
            for key in ("reason", "explanation", "error", "failure_type"):
                value = result.get(key)
                if isinstance(value, str) and value:
                    reason = value
                    break
            return summary, result, reason

        if isinstance(result, str):
            truncated = result.strip()[:500]
            return truncated or None, {"value": result}, truncated or None

        return None, {"value": result}, None
