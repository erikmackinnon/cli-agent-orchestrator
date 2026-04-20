"""Schema migration tests for orchestration persistence tables."""

import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from cli_agent_orchestrator.clients.database import run_schema_migrations


def _list_tables(db_file: Path) -> set[str]:
    with sqlite3.connect(str(db_file)) as conn:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {row[0] for row in rows}


def test_run_schema_migrations_creates_orchestration_tables(tmp_path: Path) -> None:
    db_file = tmp_path / "orchestration.db"

    run_schema_migrations(database_file=db_file)

    tables = _list_tables(db_file)
    assert "orchestration_runs" in tables
    assert "orchestration_jobs" in tables
    assert "orchestration_attempts" in tables
    assert "orchestration_events" in tables
    assert "worker_terminals" in tables
    assert "orchestration_subscriptions" in tables
    assert "orchestration_log_offsets" in tables


def test_run_schema_migrations_is_idempotent(tmp_path: Path) -> None:
    db_file = tmp_path / "orchestration.db"

    run_schema_migrations(database_file=db_file)
    run_schema_migrations(database_file=db_file)

    tables = _list_tables(db_file)
    assert "orchestration_runs" in tables
    assert "orchestration_events" in tables


def test_run_schema_migrations_preserves_existing_tables(tmp_path: Path) -> None:
    db_file = tmp_path / "legacy.db"

    with sqlite3.connect(str(db_file)) as conn:
        conn.execute("""
            CREATE TABLE terminals (
                id TEXT PRIMARY KEY,
                tmux_session TEXT NOT NULL,
                tmux_window TEXT NOT NULL,
                provider TEXT NOT NULL,
                agent_profile TEXT,
                last_active TEXT
            )
            """)
        conn.execute("""
            CREATE TABLE inbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender_id TEXT NOT NULL,
                receiver_id TEXT NOT NULL,
                message TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT
            )
            """)
        conn.execute("""
            CREATE TABLE flows (
                name TEXT PRIMARY KEY,
                file_path TEXT NOT NULL,
                schedule TEXT NOT NULL,
                agent_profile TEXT NOT NULL,
                provider TEXT NOT NULL,
                script TEXT,
                last_run TEXT,
                next_run TEXT,
                enabled INTEGER
            )
            """)

        conn.execute("""
            INSERT INTO terminals (id, tmux_session, tmux_window, provider, agent_profile, last_active)
            VALUES ('t-1', 's-1', 'w-1', 'kiro_cli', 'developer', '2026-04-18T00:00:00')
            """)
        conn.execute("""
            INSERT INTO inbox (sender_id, receiver_id, message, status, created_at)
            VALUES ('a', 'b', 'hello', 'pending', '2026-04-18T00:00:00')
            """)
        conn.execute("""
            INSERT INTO flows (name, file_path, schedule, agent_profile, provider, script, enabled)
            VALUES ('flow-1', '/tmp/f.yaml', '* * * * *', 'developer', 'kiro_cli', 'echo ok', 1)
            """)
        conn.commit()

    run_schema_migrations(database_file=db_file)

    with sqlite3.connect(str(db_file)) as conn:
        terminal_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(terminals)").fetchall()
        }
        terminal_count = conn.execute("SELECT COUNT(*) FROM terminals").fetchone()[0]
        inbox_count = conn.execute("SELECT COUNT(*) FROM inbox").fetchone()[0]
        flows_count = conn.execute("SELECT COUNT(*) FROM flows").fetchone()[0]

    assert "allowed_tools" in terminal_columns
    assert terminal_count == 1
    assert inbox_count == 1
    assert flows_count == 1


def test_run_schema_migrations_raises_on_schema_failure(tmp_path: Path) -> None:
    db_file = tmp_path / "orchestration.db"

    with patch(
        "cli_agent_orchestrator.clients.database._migrate_orchestration_schema",
        side_effect=sqlite3.OperationalError("simulated DDL failure"),
    ):
        with pytest.raises(sqlite3.OperationalError, match="simulated DDL failure"):
            run_schema_migrations(database_file=db_file)
