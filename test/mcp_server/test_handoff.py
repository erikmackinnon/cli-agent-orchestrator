"""Tests for MCP server handoff logic."""

import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch

from cli_agent_orchestrator.mcp_server import server as mcp_server


def test_send_direct_input_handoff_includes_callback_protocol_for_codex():
    """Codex handoff messages should include callback protocol and codex guidance."""
    with patch.object(mcp_server, "_send_direct_input") as mock_send:
        mcp_server._send_direct_input_handoff(
            terminal_id="worker-1",
            provider="codex",
            message="Implement hello world",
            supervisor_id="supervisor-abc123",
            handoff_id="deadbeef",
        )

    mock_send.assert_called_once()
    sent_message = mock_send.call_args[0][1]
    assert sent_message.startswith("[CAO Handoff]")
    assert "supervisor_terminal_id=supervisor-abc123" in sent_message
    assert "[CAO_HANDOFF_COMPLETE:deadbeef]" in sent_message
    assert "Codex-specific note" in sent_message
    assert sent_message.endswith("Implement hello world")


def test_send_direct_input_handoff_for_non_codex_still_includes_callback_protocol():
    """Non-codex providers should still receive the required callback protocol header."""
    with patch.object(mcp_server, "_send_direct_input") as mock_send:
        mcp_server._send_direct_input_handoff(
            terminal_id="worker-2",
            provider="claude_code",
            message="Review this change",
            supervisor_id="supervisor-xyz789",
            handoff_id="cafebabe",
        )

    mock_send.assert_called_once()
    sent_message = mock_send.call_args[0][1]
    assert sent_message.startswith("[CAO Handoff]")
    assert "supervisor_terminal_id=supervisor-xyz789" in sent_message
    assert "[CAO_HANDOFF_COMPLETE:cafebabe]" in sent_message
    assert "Codex-specific note" not in sent_message
    assert sent_message.endswith("Review this change")


def test_extract_handoff_callback_summary_removes_marker():
    """Marker-prefixed callback messages should return only summary content."""
    summary = mcp_server._extract_handoff_callback_summary(
        message="[CAO_HANDOFF_COMPLETE:1234abcd] Files changed: x.py",
        handoff_id="1234abcd",
    )

    assert summary == "Files changed: x.py"


def test_handoff_impl_success_uses_callback_and_exits_worker():
    """Handoff should succeed only after callback marker is received."""
    fake_uuid = MagicMock()
    fake_uuid.hex = "deadbeefcafebabe"

    with (
        patch.object(mcp_server, "_create_terminal", return_value=("dev-terminal-1", "codex")),
        patch.object(mcp_server, "wait_until_terminal_status", return_value=True),
        patch.object(mcp_server, "_send_direct_input_handoff") as mock_send_handoff,
        patch.object(
            mcp_server,
            "_wait_for_handoff_callback",
            return_value="[CAO_HANDOFF_COMPLETE:deadbeef] completed summary",
        ) as mock_wait_callback,
        patch.object(
            mcp_server,
            "_extract_handoff_callback_summary",
            return_value="completed summary",
        ) as mock_extract_summary,
        patch.object(mcp_server, "_cleanup_handoff_terminal") as mock_cleanup,
        patch.object(mcp_server, "uuid4", return_value=fake_uuid),
        patch.object(mcp_server.asyncio, "sleep", new=AsyncMock()),
        patch.dict(os.environ, {"CAO_TERMINAL_ID": "supervisor-abc123"}),
    ):
        result = asyncio.run(
            mcp_server._handoff_impl("developer", "Implement feature", timeout=120)
        )

    assert result.success is True
    assert result.output == "completed summary"
    assert result.terminal_id == "dev-terminal-1"
    assert "Successfully handed off" in result.message

    mock_send_handoff.assert_called_once_with(
        "dev-terminal-1",
        "codex",
        "Implement feature",
        "supervisor-abc123",
        "deadbeef",
    )
    mock_wait_callback.assert_called_once_with(
        "supervisor-abc123", "dev-terminal-1", "deadbeef", 120
    )
    mock_extract_summary.assert_called_once_with(
        "[CAO_HANDOFF_COMPLETE:deadbeef] completed summary",
        "deadbeef",
    )
    mock_cleanup.assert_called_once_with("dev-terminal-1", "deadbeef", reason="handoff_success")


def test_handoff_impl_times_out_waiting_for_callback_and_returns_last_output():
    """If callback never arrives, handoff should fail with timeout and include last worker output."""
    fake_uuid = MagicMock()
    fake_uuid.hex = "deadbeefcafebabe"
    output_response = MagicMock()
    output_response.json.return_value = {"output": "partial progress update"}

    with (
        patch.object(mcp_server, "_create_terminal", return_value=("dev-terminal-2", "codex")),
        patch.object(mcp_server, "wait_until_terminal_status", return_value=True),
        patch.object(mcp_server, "_send_direct_input_handoff"),
        patch.object(mcp_server, "_wait_for_handoff_callback", return_value=None),
        patch.object(mcp_server, "_api_get", return_value=output_response),
        patch.object(mcp_server, "_cleanup_handoff_terminal") as mock_cleanup,
        patch.object(mcp_server, "PRESERVE_TIMEOUT_HANDOFF_TERMINALS", False),
        patch.object(mcp_server, "uuid4", return_value=fake_uuid),
        patch.object(mcp_server.asyncio, "sleep", new=AsyncMock()),
        patch.dict(os.environ, {"CAO_TERMINAL_ID": "supervisor-abc123"}),
    ):
        result = asyncio.run(mcp_server._handoff_impl("developer", "Implement feature", timeout=60))

    assert result.success is False
    assert result.terminal_id == "dev-terminal-2"
    assert result.output == "partial progress update"
    assert "timed out after 60 seconds" in result.message
    assert "[CAO_HANDOFF_COMPLETE:deadbeef]" in result.message
    mock_cleanup.assert_called_once_with("dev-terminal-2", "deadbeef", reason="callback_timeout")


def test_handoff_impl_timeout_preserves_worker_when_configured():
    """Timeout cleanup should be skipped when preserve-timeout flag is enabled."""
    fake_uuid = MagicMock()
    fake_uuid.hex = "deadbeefcafebabe"
    output_response = MagicMock()
    output_response.json.return_value = {"output": "partial progress update"}

    with (
        patch.object(mcp_server, "_create_terminal", return_value=("dev-terminal-9", "codex")),
        patch.object(mcp_server, "wait_until_terminal_status", return_value=True),
        patch.object(mcp_server, "_send_direct_input_handoff"),
        patch.object(mcp_server, "_wait_for_handoff_callback", return_value=None),
        patch.object(mcp_server, "_api_get", return_value=output_response),
        patch.object(mcp_server, "_cleanup_handoff_terminal") as mock_cleanup,
        patch.object(mcp_server, "PRESERVE_TIMEOUT_HANDOFF_TERMINALS", True),
        patch.object(mcp_server, "uuid4", return_value=fake_uuid),
        patch.object(mcp_server.asyncio, "sleep", new=AsyncMock()),
        patch.dict(os.environ, {"CAO_TERMINAL_ID": "supervisor-abc123"}),
    ):
        result = asyncio.run(mcp_server._handoff_impl("developer", "Implement feature", timeout=60))

    assert result.success is False
    assert result.terminal_id == "dev-terminal-9"
    mock_cleanup.assert_not_called()


def test_handoff_impl_fails_when_worker_never_reaches_ready_status():
    """If worker never reaches IDLE/COMPLETED readiness, handoff should fail early."""
    fake_uuid = MagicMock()
    fake_uuid.hex = "deadbeefcafebabe"

    with (
        patch.object(mcp_server, "_create_terminal", return_value=("dev-terminal-3", "codex")),
        patch.object(mcp_server, "wait_until_terminal_status", return_value=False),
        patch.object(mcp_server, "_send_direct_input_handoff") as mock_send_handoff,
        patch.object(mcp_server, "_cleanup_handoff_terminal") as mock_cleanup,
        patch.object(mcp_server, "uuid4", return_value=fake_uuid),
    ):
        result = asyncio.run(mcp_server._handoff_impl("developer", "Do work"))

    assert result.success is False
    assert result.terminal_id == "dev-terminal-3"
    assert "did not reach ready status" in result.message
    assert result.output is None
    mock_send_handoff.assert_not_called()
    mock_cleanup.assert_called_once_with("dev-terminal-3", "deadbeef", reason="worker_not_ready")
