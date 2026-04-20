"""API tests for orchestration public HTTP endpoints."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from cli_agent_orchestrator.models.orchestration import (
    AttemptTerminalRef,
    OrchestrationCancelResponse,
    OrchestrationFinalizeResponse,
    OrchestrationSpawnResponse,
    OrchestrationStartResponse,
    OrchestrationStatusResponse,
    OrchestrationWaitResponse,
    RunRecord,
    RunSnapshot,
    RunStatus,
)


def test_orchestration_start_success(client):
    mock_service = MagicMock()
    mock_service.start.return_value = OrchestrationStartResponse(
        run_id="run-1",
        job_ids=["job-1"],
        status=RunStatus.RUNNING,
    )

    with patch(
        "cli_agent_orchestrator.api.main.get_orchestration_service", return_value=mock_service
    ):
        response = client.post(
            "/orchestration/start",
            json={
                "name": "test-run",
                "jobs": [{"agent_profile": "developer", "message": "Implement X"}],
            },
        )

    assert response.status_code == 201
    data = response.json()
    assert data["run_id"] == "run-1"
    assert data["job_ids"] == ["job-1"]
    assert data["status"] == "running"


def test_orchestration_spawn_not_found_returns_404(client):
    mock_service = MagicMock()
    mock_service.spawn.side_effect = ValueError("run run-missing not found")

    with patch(
        "cli_agent_orchestrator.api.main.get_orchestration_service", return_value=mock_service
    ):
        response = client.post(
            "/orchestration/spawn",
            json={
                "run_id": "run-missing",
                "agent_profile": "developer",
                "message": "Implement Y",
            },
        )

    assert response.status_code == 404
    assert "not found" in response.json()["detail"]


def test_orchestration_spawn_success(client):
    mock_service = MagicMock()
    mock_service.spawn.return_value = OrchestrationSpawnResponse(
        run_id="run-1",
        job_id="job-2",
        attempt_id="attempt-2",
        status="running",
        terminal_id="term-2",
    )

    with patch(
        "cli_agent_orchestrator.api.main.get_orchestration_service", return_value=mock_service
    ):
        response = client.post(
            "/orchestration/spawn",
            json={
                "run_id": "run-1",
                "agent_profile": "reviewer",
                "message": "Review this patch",
            },
        )

    assert response.status_code == 200
    assert response.json()["job_id"] == "job-2"
    assert response.json()["attempt_id"] == "attempt-2"


def test_orchestration_wait_validation_error_when_max_lt_min(client):
    response = client.post(
        "/orchestration/wait",
        json={
            "run_id": "run-1",
            "min_events": 2,
            "max_events": 1,
        },
    )

    assert response.status_code == 422


def test_orchestration_wait_success(client):
    mock_service = MagicMock()
    mock_service.wait = AsyncMock(
        return_value=OrchestrationWaitResponse(
            run_id="run-1",
            cursor=10,
            events=[],
            timeout=True,
            run_status=RunStatus.RUNNING,
            snapshot=None,
        )
    )

    with patch(
        "cli_agent_orchestrator.api.main.get_orchestration_service", return_value=mock_service
    ):
        response = client.post(
            "/orchestration/wait",
            json={
                "run_id": "run-1",
                "cursor": 5,
                "wait_timeout_sec": 1,
            },
        )

    assert response.status_code == 200
    assert response.json()["cursor"] == 10
    assert response.json()["timeout"] is True


def test_orchestration_status_success(client):
    now = datetime.now(timezone.utc)
    mock_service = MagicMock()
    mock_service.status.return_value = OrchestrationStatusResponse(
        run=RunRecord(
            run_id="run-1",
            status=RunStatus.RUNNING,
            created_at=now,
            updated_at=now,
        ),
        snapshot=RunSnapshot(
            run_id="run-1",
            run_status=RunStatus.RUNNING,
            latest_event_cursor=7,
        ),
    )

    with patch(
        "cli_agent_orchestrator.api.main.get_orchestration_service", return_value=mock_service
    ):
        response = client.post(
            "/orchestration/status",
            json={"run_id": "run-1", "include_jobs": False},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["run"]["run_id"] == "run-1"
    assert data["snapshot"]["latest_event_cursor"] == 7


def test_orchestration_status_includes_terminal_refs_when_present(client):
    now = datetime.now(timezone.utc)
    mock_service = MagicMock()
    mock_service.status.return_value = OrchestrationStatusResponse(
        run=RunRecord(
            run_id="run-1",
            status=RunStatus.RUNNING,
            created_at=now,
            updated_at=now,
        ),
        snapshot=RunSnapshot(
            run_id="run-1",
            run_status=RunStatus.RUNNING,
            latest_event_cursor=8,
        ),
        terminal_refs=[
            AttemptTerminalRef(
                attempt_id="attempt-1",
                job_id="job-1",
                terminal_id="term-1",
                terminal_present=True,
                terminal_last_active_at=now,
                log_offset=12,
                last_log_activity_at=now,
                activity_age_sec=0,
                tmux_session="cao-run-1",
                tmux_window="w-1",
            )
        ],
    )

    with patch(
        "cli_agent_orchestrator.api.main.get_orchestration_service", return_value=mock_service
    ):
        response = client.post(
            "/orchestration/status",
            json={"run_id": "run-1", "include_terminal_refs": True},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["terminal_refs"][0]["attempt_id"] == "attempt-1"
    assert data["terminal_refs"][0]["terminal_present"] is True


def test_orchestration_cancel_scope_validation(client):
    response = client.post(
        "/orchestration/cancel",
        json={
            "run_id": "run-1",
            "scope": "job",
        },
    )

    assert response.status_code == 422


def test_orchestration_cancel_rejects_attempt_scope(client):
    response = client.post(
        "/orchestration/cancel",
        json={
            "run_id": "run-1",
            "scope": "attempt",
        },
    )

    assert response.status_code == 422


def test_orchestration_cancel_success(client):
    mock_service = MagicMock()
    mock_service.cancel.return_value = OrchestrationCancelResponse(
        cancelled_jobs=["job-1"],
        cancelled_attempts=["attempt-1"],
        events=[],
    )

    with patch(
        "cli_agent_orchestrator.api.main.get_orchestration_service", return_value=mock_service
    ):
        response = client.post(
            "/orchestration/cancel",
            json={"run_id": "run-1", "scope": "run", "reason": "abort"},
        )

    assert response.status_code == 200
    assert response.json()["cancelled_jobs"] == ["job-1"]


def test_orchestration_finalize_conflict_returns_409(client):
    mock_service = MagicMock()
    mock_service.finalize.side_effect = ValueError(
        "run run-1 has active jobs and cannot be finalized: job-1"
    )

    with patch(
        "cli_agent_orchestrator.api.main.get_orchestration_service", return_value=mock_service
    ):
        response = client.post(
            "/orchestration/finalize",
            json={"run_id": "run-1", "outcome": "succeeded"},
        )

    assert response.status_code == 409
    assert "cannot be finalized" in response.json()["detail"]


def test_orchestration_finalize_success(client):
    now = datetime.now(timezone.utc)
    mock_service = MagicMock()
    mock_service.finalize.return_value = OrchestrationFinalizeResponse(
        run_id="run-1",
        status=RunStatus.FINALIZED,
        outcome="succeeded",
        finalized_at=now,
        summary="done",
    )

    with patch(
        "cli_agent_orchestrator.api.main.get_orchestration_service", return_value=mock_service
    ):
        response = client.post(
            "/orchestration/finalize",
            json={"run_id": "run-1", "outcome": "succeeded", "summary": "done"},
        )

    assert response.status_code == 200
    assert response.json()["status"] == "finalized"
    assert response.json()["outcome"] == "succeeded"
