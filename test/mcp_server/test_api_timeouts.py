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
