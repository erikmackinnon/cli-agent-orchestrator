"""Tests for orchestration MCP tool adapter helpers."""

from unittest.mock import MagicMock, patch

from cli_agent_orchestrator.mcp_server import server as mcp_server


def test_orchestration_start_impl_forwards_payload_and_parses_response():
    response = MagicMock()
    response.json.return_value = {
        "run_id": "run-1",
        "job_ids": ["job-1"],
        "status": "running",
    }

    with patch.object(mcp_server, "_api_post_json", return_value=response) as mock_post:
        result = mcp_server._orchestration_start_impl(
            name="demo-run",
            jobs=[{"agent_profile": "developer", "message": "Implement feature"}],
        )

    assert result["run_id"] == "run-1"
    assert result["job_ids"] == ["job-1"]
    assert result["status"] == "running"

    assert mock_post.call_args.args[0] == "/orchestration/start"
    payload = mock_post.call_args.kwargs["payload"]
    assert payload["name"] == "demo-run"
    assert payload["jobs"][0]["agent_profile"] == "developer"


def test_orchestration_spawn_impl_forwards_payload_and_parses_response():
    response = MagicMock()
    response.json.return_value = {
        "run_id": "run-1",
        "job_id": "job-2",
        "attempt_id": "attempt-2",
        "status": "running",
        "terminal_id": "term-2",
    }

    with patch.object(mcp_server, "_api_post_json", return_value=response) as mock_post:
        result = mcp_server._orchestration_spawn_impl(
            run_id="run-1",
            agent_profile="reviewer",
            message="Review this patch",
        )

    assert result["job_id"] == "job-2"
    assert result["attempt_id"] == "attempt-2"
    assert mock_post.call_args.args[0] == "/orchestration/spawn"


def test_orchestration_wait_impl_forwards_timeout_budget():
    response = MagicMock()
    response.json.return_value = {
        "run_id": "run-1",
        "cursor": 12,
        "events": [],
        "timeout": False,
        "run_status": "running",
    }

    with patch.object(mcp_server, "_api_post_json", return_value=response) as mock_post:
        result = mcp_server._orchestration_wait_impl(
            run_id="run-1",
            wait_timeout_sec=30,
        )

    assert result["run_id"] == "run-1"
    assert result["cursor"] == 12
    assert mock_post.call_args.args[0] == "/orchestration/wait"
    assert mock_post.call_args.kwargs["timeout"] == 35.0


def test_orchestration_status_impl_forwards_payload_and_parses_response():
    response = MagicMock()
    response.json.return_value = {
        "run": {
            "run_id": "run-1",
            "status": "running",
            "created_at": "2026-04-18T00:00:00Z",
            "updated_at": "2026-04-18T00:00:00Z",
        },
        "snapshot": {
            "run_id": "run-1",
            "run_status": "running",
            "jobs_total": 0,
            "attempts_total": 0,
            "jobs_by_status": {
                "created": 0,
                "queued": 0,
                "running": 0,
                "succeeded": 0,
                "failed": 0,
                "cancelled": 0,
                "timed_out": 0,
            },
            "attempts_by_status": {
                "created": 0,
                "running": 0,
                "succeeded": 0,
                "failed": 0,
                "cancelled": 0,
                "timed_out": 0,
            },
            "active_job_ids": [],
            "active_attempt_ids": [],
            "latest_event_cursor": 3,
        },
    }

    with patch.object(mcp_server, "_api_post_json", return_value=response) as mock_post:
        result = mcp_server._orchestration_status_impl(run_id="run-1", include_jobs=True)

    assert result["run"]["run_id"] == "run-1"
    assert result["snapshot"]["latest_event_cursor"] == 3
    assert mock_post.call_args.args[0] == "/orchestration/status"


def test_orchestration_cancel_impl_returns_error_for_invalid_scope_payload():
    result = mcp_server._orchestration_cancel_impl(run_id="run-1", scope="job")

    assert result["success"] is False
    assert "job_id is required" in result["error"]


def test_orchestration_cancel_impl_rejects_attempt_scope():
    result = mcp_server._orchestration_cancel_impl(run_id="run-1", scope="attempt")

    assert result["success"] is False
    assert "scope" in result["error"]


def test_orchestration_finalize_impl_propagates_api_runtime_error():
    with patch.object(
        mcp_server,
        "_api_post_json",
        side_effect=RuntimeError("API POST failed: /orchestration/finalize"),
    ):
        result = mcp_server._orchestration_finalize_impl(
            run_id="run-1",
            outcome="succeeded",
        )

    assert result["success"] is False
    assert "orchestration/finalize" in result["error"]
