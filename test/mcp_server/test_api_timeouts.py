"""Tests for bounded API request behavior in MCP server helpers."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from cli_agent_orchestrator.constants import API_BASE_URL
from cli_agent_orchestrator.mcp_server import server as mcp_server


def test_api_get_uses_configured_timeout():
    response = MagicMock()
    response.raise_for_status.return_value = None

    with patch.object(mcp_server.requests, "get", return_value=response) as mock_get:
        result = mcp_server._api_get("/health")

    assert result is response
    mock_get.assert_called_once_with(
        f"{API_BASE_URL}/health",
        params=None,
        timeout=mcp_server.API_REQUEST_TIMEOUT_SECONDS,
    )


def test_api_post_raises_runtime_error_on_timeout():
    with patch.object(
        mcp_server.requests, "post", side_effect=requests.Timeout("socket timed out")
    ):
        with pytest.raises(RuntimeError, match=r"API POST timed out"):
            mcp_server._api_post("/sessions", params={"provider": "codex"})


def test_create_terminal_fails_fast_when_supervisor_lookup_times_out():
    with patch.dict(mcp_server.os.environ, {"CAO_TERMINAL_ID": "sup-123"}):
        with patch.object(
            mcp_server.requests, "get", side_effect=requests.Timeout("socket timed out")
        ):
            with pytest.raises(RuntimeError, match=r"API GET timed out"):
                mcp_server._create_terminal("developer")


def test_create_terminal_forwards_orchestration_context_to_spawn_api():
    metadata_response = MagicMock()
    metadata_response.json.return_value = {
        "provider": "codex",
        "session_name": "cao-parent",
        "allowed_tools": ["*"],
    }
    working_dir_response = MagicMock()
    working_dir_response.json.return_value = {"working_directory": "/workspace"}
    create_response = MagicMock()
    create_response.json.return_value = {"id": "worker-1"}

    with (
        patch.dict(mcp_server.os.environ, {"CAO_TERMINAL_ID": "sup-123"}),
        patch.object(
            mcp_server,
            "_api_get",
            side_effect=[metadata_response, working_dir_response],
        ),
        patch.object(mcp_server, "_resolve_child_allowed_tools", return_value=None),
        patch.object(mcp_server, "_api_post", return_value=create_response) as mock_post,
    ):
        terminal_id, provider = mcp_server._create_terminal(
            "developer",
            orchestration_run_id="run-1",
            orchestration_job_id="job-1",
            orchestration_attempt_id="attempt-1",
            orchestration_chain_id="chain-1",
        )

    assert terminal_id == "worker-1"
    assert provider == "codex"
    assert mock_post.call_args.args[0] == "/sessions/cao-parent/terminals"
    assert mock_post.call_args.kwargs["params"]["orchestration_run_id"] == "run-1"
    assert mock_post.call_args.kwargs["params"]["orchestration_job_id"] == "job-1"
    assert mock_post.call_args.kwargs["params"]["orchestration_attempt_id"] == "attempt-1"
    assert mock_post.call_args.kwargs["params"]["orchestration_chain_id"] == "chain-1"
