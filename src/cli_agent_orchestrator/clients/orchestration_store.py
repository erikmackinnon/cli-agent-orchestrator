"""Dedicated persistence layer for orchestration run/job/attempt/event state."""

from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, TypedDict

from cli_agent_orchestrator.constants import DATABASE_FILE
from cli_agent_orchestrator.models.orchestration import (
    AttemptRecord,
    AttemptStatus,
    AttemptStatusCounts,
    EventType,
    JobRecord,
    JobStatus,
    JobStatusCounts,
    OrchestrationEvent,
    OrchestrationEventPayload,
    RunRecord,
    RunSnapshot,
    RunStatus,
)


class WorkerTerminalLink(TypedDict):
    """Worker terminal linkage row."""

    terminal_id: str
    run_id: str
    job_id: str
    attempt_id: str
    provider: Optional[str]
    tmux_session: Optional[str]
    tmux_window: Optional[str]
    created_at: datetime
    updated_at: datetime
    released_at: Optional[datetime]


class SubscriptionCursor(TypedDict):
    """Subscription cursor row."""

    subscription_id: str
    run_id: str
    subscriber_id: str
    cursor: int
    created_at: datetime
    updated_at: datetime


class AttemptTerminalRefRow(TypedDict):
    """Joined attempt/terminal/log liveness row."""

    attempt_id: str
    job_id: str
    terminal_id: Optional[str]
    terminal_present: Optional[bool]
    terminal_last_active_at: Optional[datetime]
    log_offset: Optional[int]
    last_log_activity_at: Optional[datetime]
    tmux_session: Optional[str]
    tmux_window: Optional[str]
    worker_terminal_released_at: Optional[datetime]


class OrchestrationStore:
    """Repository facade for orchestration persistence."""

    def __init__(self, database_file: Optional[Path] = None):
        self._database_file = database_file or DATABASE_FILE

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self._database_file))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
        finally:
            conn.close()

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _to_iso(value: Optional[datetime]) -> Optional[str]:
        if value is None:
            return None
        return value.isoformat()

    @staticmethod
    def _from_iso(value: Optional[str]) -> Optional[datetime]:
        if value is None:
            return None
        return datetime.fromisoformat(value)

    @staticmethod
    def _coerce_datetime(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        raise ValueError(f"Unsupported datetime value type: {type(value).__name__}")

    @staticmethod
    def _json_dump(value: Optional[Dict[str, Any]]) -> Optional[str]:
        if value is None:
            return None
        return json.dumps(value)

    @staticmethod
    def _json_load(value: Optional[str]) -> Optional[Dict[str, Any]]:
        if value is None:
            return None
        data = json.loads(value)
        if not isinstance(data, dict):
            raise ValueError("Expected JSON object payload")
        return data

    @staticmethod
    def _payload_dump(
        payload: Optional[OrchestrationEventPayload | Dict[str, Any]],
    ) -> str:
        if payload is None:
            payload_model = OrchestrationEventPayload()
        elif isinstance(payload, OrchestrationEventPayload):
            payload_model = payload
        else:
            payload_model = OrchestrationEventPayload.model_validate(payload)

        return json.dumps(payload_model.model_dump(mode="json", exclude_none=True))

    @staticmethod
    def _status_value(status: RunStatus | JobStatus | AttemptStatus | EventType) -> str:
        return str(status.value)

    @staticmethod
    def _table_exists(conn: sqlite3.Connection, *, table_name: str) -> bool:
        row = conn.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table' AND name = ?
            LIMIT 1
            """,
            (table_name,),
        ).fetchone()
        return row is not None

    def _row_to_run_record(self, row: sqlite3.Row) -> RunRecord:
        return RunRecord(
            run_id=row["run_id"],
            name=row["name"],
            status=row["status"],
            metadata=self._json_load(row["metadata"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            finalized_at=self._from_iso(row["finalized_at"]),
        )

    def _row_to_job_record(self, row: sqlite3.Row) -> JobRecord:
        return JobRecord(
            job_id=row["job_id"],
            run_id=row["run_id"],
            agent_profile=row["agent_profile"],
            message=row["message"],
            status=row["status"],
            role=row["role"],
            kind=row["kind"],
            parent_job_id=row["parent_job_id"],
            chain_id=row["chain_id"],
            timeout_sec=row["timeout_sec"],
            metadata=self._json_load(row["metadata"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    def _row_to_attempt_record(self, row: sqlite3.Row) -> AttemptRecord:
        return AttemptRecord(
            attempt_id=row["attempt_id"],
            run_id=row["run_id"],
            job_id=row["job_id"],
            status=row["status"],
            terminal_id=row["terminal_id"],
            nonce=row["nonce"],
            result_summary=row["result_summary"],
            result_data=self._json_load(row["result_data"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            started_at=self._from_iso(row["started_at"]),
            completed_at=self._from_iso(row["completed_at"]),
        )

    def _row_to_event(self, row: sqlite3.Row) -> OrchestrationEvent:
        payload_data = json.loads(row["payload"])
        return OrchestrationEvent(
            event_id=row["event_id"],
            run_id=row["run_id"],
            event_type=row["event_type"],
            created_at=datetime.fromisoformat(row["created_at"]),
            payload=OrchestrationEventPayload.model_validate(payload_data),
            job_id=row["job_id"],
            attempt_id=row["attempt_id"],
        )

    def _row_to_worker_terminal(self, row: sqlite3.Row) -> WorkerTerminalLink:
        return {
            "terminal_id": row["terminal_id"],
            "run_id": row["run_id"],
            "job_id": row["job_id"],
            "attempt_id": row["attempt_id"],
            "provider": row["provider"],
            "tmux_session": row["tmux_session"],
            "tmux_window": row["tmux_window"],
            "created_at": datetime.fromisoformat(row["created_at"]),
            "updated_at": datetime.fromisoformat(row["updated_at"]),
            "released_at": self._from_iso(row["released_at"]),
        }

    def _row_to_subscription(self, row: sqlite3.Row) -> SubscriptionCursor:
        return {
            "subscription_id": row["subscription_id"],
            "run_id": row["run_id"],
            "subscriber_id": row["subscriber_id"],
            "cursor": int(row["cursor"]),
            "created_at": datetime.fromisoformat(row["created_at"]),
            "updated_at": datetime.fromisoformat(row["updated_at"]),
        }

    def create_run(
        self,
        *,
        run_id: str,
        status: RunStatus = RunStatus.RUNNING,
        name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        created_at: Optional[datetime] = None,
    ) -> RunRecord:
        ts = created_at or self._now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO orchestration_runs (
                    run_id, name, status, metadata, created_at, updated_at, finalized_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    name,
                    self._status_value(status),
                    self._json_dump(metadata),
                    self._to_iso(ts),
                    self._to_iso(ts),
                    None,
                ),
            )
            conn.commit()
        return self.get_run(run_id=run_id)  # type: ignore[return-value]

    def get_run(self, *, run_id: str) -> Optional[RunRecord]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM orchestration_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_run_record(row)

    def list_runs(self, *, statuses: Optional[Sequence[RunStatus]] = None) -> List[RunRecord]:
        """List orchestration runs, optionally filtered by run status values."""

        query = "SELECT * FROM orchestration_runs"
        params: List[Any] = []
        if statuses:
            placeholders = ", ".join(["?"] * len(statuses))
            query += f" WHERE status IN ({placeholders})"
            params.extend(self._status_value(status) for status in statuses)
        query += " ORDER BY created_at ASC, run_id ASC"

        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_run_record(row) for row in rows]

    def update_run(
        self,
        *,
        run_id: str,
        status: Optional[RunStatus] = None,
        name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        finalized_at: Optional[datetime] = None,
    ) -> Optional[RunRecord]:
        assignments: List[str] = []
        params: List[Any] = []

        if status is not None:
            assignments.append("status = ?")
            params.append(self._status_value(status))
        if name is not None:
            assignments.append("name = ?")
            params.append(name)
        if metadata is not None:
            assignments.append("metadata = ?")
            params.append(self._json_dump(metadata))
        if finalized_at is not None:
            assignments.append("finalized_at = ?")
            params.append(self._to_iso(finalized_at))

        if not assignments:
            return self.get_run(run_id=run_id)

        assignments.append("updated_at = ?")
        params.append(self._to_iso(self._now()))
        params.append(run_id)

        with self._connect() as conn:
            cursor = conn.execute(
                f"UPDATE orchestration_runs SET {', '.join(assignments)} WHERE run_id = ?",
                params,
            )
            conn.commit()
            if cursor.rowcount == 0:
                return None

        return self.get_run(run_id=run_id)

    def create_job(
        self,
        *,
        job_id: str,
        run_id: str,
        agent_profile: str,
        message: str,
        status: JobStatus = JobStatus.CREATED,
        role: Optional[str] = None,
        kind: Optional[str] = None,
        parent_job_id: Optional[str] = None,
        chain_id: Optional[str] = None,
        timeout_sec: Optional[int] = None,
        idempotency_key: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        created_at: Optional[datetime] = None,
    ) -> JobRecord:
        ts = created_at or self._now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO orchestration_jobs (
                    job_id, run_id, agent_profile, message, status, role, kind,
                    parent_job_id, chain_id, timeout_sec, idempotency_key, metadata,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    run_id,
                    agent_profile,
                    message,
                    self._status_value(status),
                    role,
                    kind,
                    parent_job_id,
                    chain_id,
                    timeout_sec,
                    idempotency_key,
                    self._json_dump(metadata),
                    self._to_iso(ts),
                    self._to_iso(ts),
                ),
            )
            conn.commit()
        return self.get_job(job_id=job_id)  # type: ignore[return-value]

    def get_job(self, *, job_id: str) -> Optional[JobRecord]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM orchestration_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_job_record(row)

    def list_jobs(self, *, run_id: str) -> List[JobRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM orchestration_jobs
                WHERE run_id = ?
                ORDER BY created_at ASC, job_id ASC
                """,
                (run_id,),
            ).fetchall()
        return [self._row_to_job_record(row) for row in rows]

    def get_job_by_idempotency_key(
        self, *, run_id: str, idempotency_key: str
    ) -> Optional[JobRecord]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM orchestration_jobs
                WHERE run_id = ? AND idempotency_key = ?
                """,
                (run_id, idempotency_key),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_job_record(row)

    def update_job(
        self,
        *,
        job_id: str,
        status: Optional[JobStatus] = None,
        role: Optional[str] = None,
        kind: Optional[str] = None,
        parent_job_id: Optional[str] = None,
        chain_id: Optional[str] = None,
        timeout_sec: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[JobRecord]:
        assignments: List[str] = []
        params: List[Any] = []

        if status is not None:
            assignments.append("status = ?")
            params.append(self._status_value(status))
        if role is not None:
            assignments.append("role = ?")
            params.append(role)
        if kind is not None:
            assignments.append("kind = ?")
            params.append(kind)
        if parent_job_id is not None:
            assignments.append("parent_job_id = ?")
            params.append(parent_job_id)
        if chain_id is not None:
            assignments.append("chain_id = ?")
            params.append(chain_id)
        if timeout_sec is not None:
            assignments.append("timeout_sec = ?")
            params.append(timeout_sec)
        if metadata is not None:
            assignments.append("metadata = ?")
            params.append(self._json_dump(metadata))

        if not assignments:
            return self.get_job(job_id=job_id)

        assignments.append("updated_at = ?")
        params.append(self._to_iso(self._now()))
        params.append(job_id)

        with self._connect() as conn:
            cursor = conn.execute(
                f"UPDATE orchestration_jobs SET {', '.join(assignments)} WHERE job_id = ?",
                params,
            )
            conn.commit()
            if cursor.rowcount == 0:
                return None

        return self.get_job(job_id=job_id)

    def create_attempt(
        self,
        *,
        attempt_id: str,
        run_id: str,
        job_id: str,
        status: AttemptStatus = AttemptStatus.CREATED,
        terminal_id: Optional[str] = None,
        nonce: Optional[str] = None,
        result_summary: Optional[str] = None,
        result_data: Optional[Dict[str, Any]] = None,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
        created_at: Optional[datetime] = None,
    ) -> AttemptRecord:
        ts = created_at or self._now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO orchestration_attempts (
                    attempt_id, run_id, job_id, status, terminal_id, nonce,
                    result_summary, result_data, created_at, updated_at,
                    started_at, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    attempt_id,
                    run_id,
                    job_id,
                    self._status_value(status),
                    terminal_id,
                    nonce,
                    result_summary,
                    self._json_dump(result_data),
                    self._to_iso(ts),
                    self._to_iso(ts),
                    self._to_iso(started_at),
                    self._to_iso(completed_at),
                ),
            )
            conn.commit()
        return self.get_attempt(attempt_id=attempt_id)  # type: ignore[return-value]

    def get_attempt(self, *, attempt_id: str) -> Optional[AttemptRecord]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM orchestration_attempts WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_attempt_record(row)

    def list_attempts(
        self, *, run_id: Optional[str] = None, job_id: Optional[str] = None
    ) -> List[AttemptRecord]:
        if run_id is None and job_id is None:
            raise ValueError("run_id or job_id is required")

        predicates: List[str] = []
        params: List[Any] = []
        if run_id is not None:
            predicates.append("run_id = ?")
            params.append(run_id)
        if job_id is not None:
            predicates.append("job_id = ?")
            params.append(job_id)

        where_clause = " AND ".join(predicates)

        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM orchestration_attempts
                WHERE {where_clause}
                ORDER BY created_at ASC, attempt_id ASC
                """,
                params,
            ).fetchall()
        return [self._row_to_attempt_record(row) for row in rows]

    def update_attempt(
        self,
        *,
        attempt_id: str,
        status: Optional[AttemptStatus] = None,
        terminal_id: Optional[str] = None,
        nonce: Optional[str] = None,
        result_summary: Optional[str] = None,
        result_data: Optional[Dict[str, Any]] = None,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
    ) -> Optional[AttemptRecord]:
        assignments: List[str] = []
        params: List[Any] = []

        if status is not None:
            assignments.append("status = ?")
            params.append(self._status_value(status))
        if terminal_id is not None:
            assignments.append("terminal_id = ?")
            params.append(terminal_id)
        if nonce is not None:
            assignments.append("nonce = ?")
            params.append(nonce)
        if result_summary is not None:
            assignments.append("result_summary = ?")
            params.append(result_summary)
        if result_data is not None:
            assignments.append("result_data = ?")
            params.append(self._json_dump(result_data))
        if started_at is not None:
            assignments.append("started_at = ?")
            params.append(self._to_iso(started_at))
        if completed_at is not None:
            assignments.append("completed_at = ?")
            params.append(self._to_iso(completed_at))

        if not assignments:
            return self.get_attempt(attempt_id=attempt_id)

        assignments.append("updated_at = ?")
        params.append(self._to_iso(self._now()))
        params.append(attempt_id)

        with self._connect() as conn:
            cursor = conn.execute(
                f"UPDATE orchestration_attempts SET {', '.join(assignments)} WHERE attempt_id = ?",
                params,
            )
            conn.commit()
            if cursor.rowcount == 0:
                return None

        return self.get_attempt(attempt_id=attempt_id)

    def append_event(
        self,
        *,
        run_id: str,
        event_type: EventType,
        payload: Optional[OrchestrationEventPayload | Dict[str, Any]] = None,
        job_id: Optional[str] = None,
        attempt_id: Optional[str] = None,
        dedupe_key: Optional[str] = None,
        created_at: Optional[datetime] = None,
    ) -> OrchestrationEvent:
        ts = created_at or self._now()
        payload_json = self._payload_dump(payload)

        with self._connect() as conn:
            if dedupe_key:
                existing = conn.execute(
                    """
                    SELECT * FROM orchestration_events
                    WHERE run_id = ? AND dedupe_key = ?
                    """,
                    (run_id, dedupe_key),
                ).fetchone()
                if existing is not None:
                    return self._row_to_event(existing)

            try:
                cursor = conn.execute(
                    """
                    INSERT INTO orchestration_events (
                        run_id, job_id, attempt_id, event_type, payload, dedupe_key, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        job_id,
                        attempt_id,
                        self._status_value(event_type),
                        payload_json,
                        dedupe_key,
                        self._to_iso(ts),
                    ),
                )
                conn.commit()
                if cursor.lastrowid is None:
                    raise RuntimeError("Inserted orchestration event has no rowid")
                event_id = int(cursor.lastrowid)
            except sqlite3.IntegrityError:
                if not dedupe_key:
                    raise
                existing = conn.execute(
                    """
                    SELECT * FROM orchestration_events
                    WHERE run_id = ? AND dedupe_key = ?
                    """,
                    (run_id, dedupe_key),
                ).fetchone()
                if existing is None:
                    raise
                return self._row_to_event(existing)

            row = conn.execute(
                "SELECT * FROM orchestration_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if row is None:
                raise RuntimeError("Inserted orchestration event could not be reloaded")
            return self._row_to_event(row)

    def read_events(
        self,
        *,
        run_id: str,
        cursor: int = 0,
        max_events: int = 100,
        event_types: Optional[Sequence[EventType]] = None,
        job_ids: Optional[Sequence[str]] = None,
    ) -> List[OrchestrationEvent]:
        if max_events < 1:
            raise ValueError("max_events must be at least 1")

        predicates = ["run_id = ?", "event_id > ?"]
        params: List[Any] = [run_id, cursor]

        if event_types:
            placeholders = ", ".join(["?"] * len(event_types))
            predicates.append(f"event_type IN ({placeholders})")
            params.extend(self._status_value(event_type) for event_type in event_types)

        if job_ids:
            placeholders = ", ".join(["?"] * len(job_ids))
            predicates.append(f"job_id IN ({placeholders})")
            params.extend(job_ids)

        where_clause = " AND ".join(predicates)

        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM orchestration_events
                WHERE {where_clause}
                ORDER BY event_id ASC
                LIMIT ?
                """,
                [*params, max_events],
            ).fetchall()

        return [self._row_to_event(row) for row in rows]

    def get_event_by_dedupe_key(
        self, *, run_id: str, dedupe_key: str
    ) -> Optional[OrchestrationEvent]:
        """Fetch an event by dedupe key within a run."""
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM orchestration_events
                WHERE run_id = ? AND dedupe_key = ?
                """,
                (run_id, dedupe_key),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_event(row)

    def get_latest_event_cursor(self, *, run_id: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(event_id), 0) AS max_event_id FROM orchestration_events WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        if row is None:
            return 0
        return int(row["max_event_id"])

    def link_worker_terminal(
        self,
        *,
        terminal_id: str,
        run_id: str,
        job_id: str,
        attempt_id: str,
        provider: Optional[str] = None,
        tmux_session: Optional[str] = None,
        tmux_window: Optional[str] = None,
        created_at: Optional[datetime] = None,
        released_at: Optional[datetime] = None,
    ) -> WorkerTerminalLink:
        ts = created_at or self._now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO worker_terminals (
                    terminal_id, run_id, job_id, attempt_id, provider,
                    tmux_session, tmux_window, created_at, updated_at, released_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(terminal_id) DO UPDATE SET
                    run_id = excluded.run_id,
                    job_id = excluded.job_id,
                    attempt_id = excluded.attempt_id,
                    provider = excluded.provider,
                    tmux_session = excluded.tmux_session,
                    tmux_window = excluded.tmux_window,
                    updated_at = excluded.updated_at,
                    released_at = excluded.released_at
                """,
                (
                    terminal_id,
                    run_id,
                    job_id,
                    attempt_id,
                    provider,
                    tmux_session,
                    tmux_window,
                    self._to_iso(ts),
                    self._to_iso(self._now()),
                    self._to_iso(released_at),
                ),
            )
            conn.commit()
            row = conn.execute(
                "SELECT * FROM worker_terminals WHERE terminal_id = ?",
                (terminal_id,),
            ).fetchone()
            if row is None:
                raise RuntimeError("Upserted worker terminal link could not be reloaded")
            return self._row_to_worker_terminal(row)

    def get_worker_terminal(self, *, terminal_id: str) -> Optional[WorkerTerminalLink]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM worker_terminals WHERE terminal_id = ?",
                (terminal_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_worker_terminal(row)

    def list_worker_terminals(
        self, *, run_id: str, active_only: bool = False
    ) -> List[WorkerTerminalLink]:
        query = "SELECT * FROM worker_terminals WHERE run_id = ?"
        params: List[Any] = [run_id]
        if active_only:
            query += " AND released_at IS NULL"
        query += " ORDER BY created_at ASC, terminal_id ASC"

        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()

        return [self._row_to_worker_terminal(row) for row in rows]

    def release_worker_terminal(
        self,
        *,
        terminal_id: str,
        released_at: Optional[datetime] = None,
    ) -> bool:
        ts = released_at or self._now()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE worker_terminals
                SET released_at = ?, updated_at = ?
                WHERE terminal_id = ?
                """,
                (
                    self._to_iso(ts),
                    self._to_iso(self._now()),
                    terminal_id,
                ),
            )
            conn.commit()
        return cursor.rowcount > 0

    def upsert_subscription(
        self,
        *,
        run_id: str,
        subscriber_id: str,
        cursor: int = 0,
        subscription_id: Optional[str] = None,
        created_at: Optional[datetime] = None,
    ) -> SubscriptionCursor:
        ts = created_at or self._now()
        new_subscription_id = subscription_id or str(uuid.uuid4())

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO orchestration_subscriptions (
                    subscription_id, run_id, subscriber_id, cursor, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, subscriber_id) DO UPDATE SET
                    cursor = MAX(orchestration_subscriptions.cursor, excluded.cursor),
                    updated_at = excluded.updated_at
                """,
                (
                    new_subscription_id,
                    run_id,
                    subscriber_id,
                    cursor,
                    self._to_iso(ts),
                    self._to_iso(self._now()),
                ),
            )
            conn.commit()

            row = conn.execute(
                """
                SELECT * FROM orchestration_subscriptions
                WHERE run_id = ? AND subscriber_id = ?
                """,
                (run_id, subscriber_id),
            ).fetchone()
            if row is None:
                raise RuntimeError("Upserted subscription cursor could not be reloaded")
            return self._row_to_subscription(row)

    def get_subscription(self, *, run_id: str, subscriber_id: str) -> Optional[SubscriptionCursor]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM orchestration_subscriptions
                WHERE run_id = ? AND subscriber_id = ?
                """,
                (run_id, subscriber_id),
            ).fetchone()

        if row is None:
            return None
        return self._row_to_subscription(row)

    def advance_subscription_cursor(
        self,
        *,
        run_id: str,
        subscriber_id: str,
        cursor: int,
    ) -> Optional[SubscriptionCursor]:
        with self._connect() as conn:
            result = conn.execute(
                """
                UPDATE orchestration_subscriptions
                SET
                    cursor = CASE WHEN cursor > ? THEN cursor ELSE ? END,
                    updated_at = ?
                WHERE run_id = ? AND subscriber_id = ?
                """,
                (
                    cursor,
                    cursor,
                    self._to_iso(self._now()),
                    run_id,
                    subscriber_id,
                ),
            )
            conn.commit()
            if result.rowcount == 0:
                return None

            row = conn.execute(
                """
                SELECT * FROM orchestration_subscriptions
                WHERE run_id = ? AND subscriber_id = ?
                """,
                (run_id, subscriber_id),
            ).fetchone()
            if row is None:
                return None
            return self._row_to_subscription(row)

    def delete_subscription(self, *, run_id: str, subscriber_id: str) -> bool:
        with self._connect() as conn:
            result = conn.execute(
                """
                DELETE FROM orchestration_subscriptions
                WHERE run_id = ? AND subscriber_id = ?
                """,
                (run_id, subscriber_id),
            )
            conn.commit()
        return result.rowcount > 0

    def get_terminal_log_offset(self, *, terminal_id: str) -> Optional[int]:
        """Return persisted byte offset for a terminal log, if any."""
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT byte_offset
                FROM orchestration_log_offsets
                WHERE terminal_id = ?
                """,
                (terminal_id,),
            ).fetchone()
        if row is None:
            return None
        return int(row["byte_offset"])

    def list_attempt_terminal_refs(self, *, run_id: str) -> List[AttemptTerminalRefRow]:
        """Return per-attempt terminal/log references for status/liveness surfaces."""
        with self._connect() as conn:
            has_terminals_table = self._table_exists(conn, table_name="terminals")
            terminals_select = (
                "t.id AS terminal_present_id, t.last_active AS terminal_last_active_at"
                if has_terminals_table
                else "NULL AS terminal_present_id, NULL AS terminal_last_active_at"
            )
            terminals_join = (
                "LEFT JOIN terminals t ON t.id = COALESCE(a.terminal_id, wt.terminal_id)"
                if has_terminals_table
                else ""
            )

            rows = conn.execute(
                f"""
                WITH latest_worker_terminals AS (
                    SELECT
                        wt.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY wt.attempt_id
                            ORDER BY
                                CASE WHEN wt.released_at IS NULL THEN 0 ELSE 1 END ASC,
                                wt.updated_at DESC,
                                wt.created_at DESC,
                                wt.terminal_id DESC
                        ) AS rn
                    FROM worker_terminals wt
                    WHERE wt.run_id = ?
                )
                SELECT
                    a.attempt_id,
                    a.job_id,
                    COALESCE(a.terminal_id, wt.terminal_id) AS resolved_terminal_id,
                    {terminals_select},
                    lo.byte_offset AS log_offset,
                    lo.updated_at AS last_log_activity_at,
                    wt.tmux_session,
                    wt.tmux_window,
                    wt.released_at AS worker_terminal_released_at
                FROM orchestration_attempts a
                LEFT JOIN latest_worker_terminals wt
                    ON wt.attempt_id = a.attempt_id
                    AND wt.rn = 1
                LEFT JOIN orchestration_log_offsets lo
                    ON lo.terminal_id = COALESCE(a.terminal_id, wt.terminal_id)
                {terminals_join}
                WHERE a.run_id = ?
                ORDER BY a.created_at ASC, a.attempt_id ASC
                """,
                (run_id, run_id),
            ).fetchall()

        refs: List[AttemptTerminalRefRow] = []
        for row in rows:
            terminal_id = row["resolved_terminal_id"]
            terminal_present: Optional[bool] = None
            if terminal_id is not None:
                terminal_present = (
                    bool(row["terminal_present_id"]) if has_terminals_table else False
                )

            refs.append(
                {
                    "attempt_id": row["attempt_id"],
                    "job_id": row["job_id"],
                    "terminal_id": terminal_id,
                    "terminal_present": terminal_present,
                    "terminal_last_active_at": self._coerce_datetime(
                        row["terminal_last_active_at"]
                    ),
                    "log_offset": (
                        int(row["log_offset"]) if row["log_offset"] is not None else None
                    ),
                    "last_log_activity_at": self._coerce_datetime(row["last_log_activity_at"]),
                    "tmux_session": row["tmux_session"],
                    "tmux_window": row["tmux_window"],
                    "worker_terminal_released_at": self._coerce_datetime(
                        row["worker_terminal_released_at"]
                    ),
                }
            )

        return refs

    def upsert_terminal_log_offset(
        self,
        *,
        terminal_id: str,
        byte_offset: int,
        updated_at: Optional[datetime] = None,
    ) -> int:
        """Persist a terminal log byte offset and return the stored offset."""
        normalized_offset = max(0, int(byte_offset))
        ts = updated_at or self._now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO orchestration_log_offsets (
                    terminal_id, byte_offset, updated_at
                ) VALUES (?, ?, ?)
                ON CONFLICT(terminal_id) DO UPDATE SET
                    byte_offset = excluded.byte_offset,
                    updated_at = excluded.updated_at
                """,
                (
                    terminal_id,
                    normalized_offset,
                    self._to_iso(ts),
                ),
            )
            conn.commit()
        return normalized_offset

    def get_run_snapshot(self, *, run_id: str) -> Optional[RunSnapshot]:
        run = self.get_run(run_id=run_id)
        if run is None:
            return None

        jobs_total = 0
        attempts_total = 0

        jobs_by_status_map: Dict[str, int] = {
            JobStatus.CREATED.value: 0,
            JobStatus.QUEUED.value: 0,
            JobStatus.RUNNING.value: 0,
            JobStatus.SUCCEEDED.value: 0,
            JobStatus.FAILED.value: 0,
            JobStatus.CANCELLED.value: 0,
            JobStatus.TIMED_OUT.value: 0,
        }
        attempts_by_status_map: Dict[str, int] = {
            AttemptStatus.CREATED.value: 0,
            AttemptStatus.RUNNING.value: 0,
            AttemptStatus.SUCCEEDED.value: 0,
            AttemptStatus.FAILED.value: 0,
            AttemptStatus.CANCELLED.value: 0,
            AttemptStatus.TIMED_OUT.value: 0,
        }

        with self._connect() as conn:
            job_rows = conn.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM orchestration_jobs
                WHERE run_id = ?
                GROUP BY status
                """,
                (run_id,),
            ).fetchall()
            for row in job_rows:
                status = row["status"]
                count = int(row["count"])
                jobs_total += count
                if status in jobs_by_status_map:
                    jobs_by_status_map[status] = count

            attempt_rows = conn.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM orchestration_attempts
                WHERE run_id = ?
                GROUP BY status
                """,
                (run_id,),
            ).fetchall()
            for row in attempt_rows:
                status = row["status"]
                count = int(row["count"])
                attempts_total += count
                if status in attempts_by_status_map:
                    attempts_by_status_map[status] = count

            active_job_rows = conn.execute(
                """
                SELECT job_id
                FROM orchestration_jobs
                WHERE run_id = ? AND status IN (?, ?, ?)
                ORDER BY created_at ASC, job_id ASC
                """,
                (
                    run_id,
                    JobStatus.CREATED.value,
                    JobStatus.QUEUED.value,
                    JobStatus.RUNNING.value,
                ),
            ).fetchall()

            active_attempt_rows = conn.execute(
                """
                SELECT attempt_id
                FROM orchestration_attempts
                WHERE run_id = ? AND status IN (?, ?)
                ORDER BY created_at ASC, attempt_id ASC
                """,
                (run_id, AttemptStatus.CREATED.value, AttemptStatus.RUNNING.value),
            ).fetchall()

        return RunSnapshot(
            run_id=run_id,
            run_status=run.status,
            jobs_total=jobs_total,
            attempts_total=attempts_total,
            jobs_by_status=JobStatusCounts(**jobs_by_status_map),
            attempts_by_status=AttemptStatusCounts(**attempts_by_status_map),
            active_job_ids=[row["job_id"] for row in active_job_rows],
            active_attempt_ids=[row["attempt_id"] for row in active_attempt_rows],
            latest_event_cursor=self.get_latest_event_cursor(run_id=run_id),
        )
