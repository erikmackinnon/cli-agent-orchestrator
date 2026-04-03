"""Tests for MCP server entrypoint behavior."""

from unittest.mock import patch

from cli_agent_orchestrator.mcp_server import server as mcp_server


def test_main_disables_fastmcp_banner_for_stdio():
    with patch.object(mcp_server.mcp, "run") as mock_run:
        mcp_server.main()

    mock_run.assert_called_once_with(show_banner=False)
