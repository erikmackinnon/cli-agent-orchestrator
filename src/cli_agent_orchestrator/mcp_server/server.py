"""CLI Agent Orchestrator MCP Server implementation."""

import asyncio
import logging
import os
import time
from typing import Any, Dict, Optional, Tuple, Union
from uuid import uuid4

import requests
from fastmcp import FastMCP
from pydantic import Field

from cli_agent_orchestrator.constants import API_BASE_URL, DEFAULT_PROVIDER
from cli_agent_orchestrator.mcp_server.models import HandoffResult
from cli_agent_orchestrator.models.terminal import TerminalStatus
from cli_agent_orchestrator.utils.terminal import generate_session_name, wait_until_terminal_status

logger = logging.getLogger(__name__)

# Environment variable to enable/disable working_directory parameter
ENABLE_WORKING_DIRECTORY = os.getenv("CAO_ENABLE_WORKING_DIRECTORY", "false").lower() == "true"

# Environment variable to enable/disable automatic sender terminal ID injection
ENABLE_SENDER_ID_INJECTION = os.getenv("CAO_ENABLE_SENDER_ID_INJECTION", "false").lower() == "true"
PRESERVE_TIMEOUT_HANDOFF_TERMINALS = (
    os.getenv("CAO_PRESERVE_TIMED_OUT_HANDOFF_TERMINALS", "false").lower() == "true"
)

# API request timeout for MCP server -> CAO API calls.
# Keep this bounded so tool calls fail fast instead of hanging indefinitely.
DEFAULT_API_REQUEST_TIMEOUT = 15.0
WORKDIR_API_REQUEST_TIMEOUT = 5.0


def _load_api_timeout_seconds() -> float:
    raw = os.getenv("CAO_API_REQUEST_TIMEOUT", str(DEFAULT_API_REQUEST_TIMEOUT))
    try:
        value = float(raw)
        if value <= 0:
            raise ValueError("must be > 0")
        return value
    except ValueError:
        logger.warning(
            "Invalid CAO_API_REQUEST_TIMEOUT=%r; falling back to %.1fs",
            raw,
            DEFAULT_API_REQUEST_TIMEOUT,
        )
        return DEFAULT_API_REQUEST_TIMEOUT


API_REQUEST_TIMEOUT_SECONDS = _load_api_timeout_seconds()

# Create MCP server
mcp = FastMCP(
    "cao-mcp-server",
    instructions="""
    # CLI Agent Orchestrator MCP Server

    This server provides tools to facilitate terminal delegation within CLI Agent Orchestrator sessions.

    ## Best Practices

    - Use specific agent profiles and providers
    - Provide clear and concise messages
    - Ensure you're running within a CAO terminal (CAO_TERMINAL_ID must be set)
    """,
)

LOAD_SKILL_TOOL_DESCRIPTION = """Retrieve the full Markdown body of an available skill from cao-server.

Use this tool when your prompt lists a CAO skill and you need its full instructions at runtime.

Args:
    name: Name of the skill to retrieve

Returns:
    The skill content on success, or a dict with success=False and an error message on failure
"""


def _resolve_child_allowed_tools(
    parent_allowed_tools: Optional[list], child_profile_name: str
) -> Optional[str]:
    """Resolve allowed_tools for a child terminal via intersection.

    The child gets at most the union of: what the parent allows + what the
    child profile specifies. If the parent is unrestricted ("*"), the child
    profile's allowedTools are used as-is.

    Returns:
        Comma-separated string of allowed tools, or None for unrestricted.
    """
    from cli_agent_orchestrator.utils.agent_profiles import load_agent_profile
    from cli_agent_orchestrator.utils.tool_mapping import resolve_allowed_tools

    try:
        child_profile = load_agent_profile(child_profile_name)
        mcp_server_names = (
            list(child_profile.mcpServers.keys()) if child_profile.mcpServers else None
        )
        child_allowed = resolve_allowed_tools(
            child_profile.allowedTools, child_profile.role, mcp_server_names
        )
    except FileNotFoundError:
        child_allowed = None

    # If parent is unrestricted or has no restrictions, use child's tools
    if parent_allowed_tools is None or "*" in parent_allowed_tools:
        if child_allowed:
            return ",".join(child_allowed)
        return None

    # If child has no opinion (None), inherit parent's restrictions
    if child_allowed is None:
        return ",".join(parent_allowed_tools)

    # If child explicitly requests unrestricted ("*"), honor it
    if "*" in child_allowed:
        return None

    # Both have restrictions: child gets its own profile tools
    # (the child profile defines what it needs; parent's restrictions
    # are enforced by the parent not delegating unauthorized work)
    return ",".join(child_allowed)


def _api_get(
    path: str, *, params: Optional[Dict[str, Any]] = None, timeout: Optional[float] = None
):
    """Issue a GET to CAO API with bounded timeout and actionable errors."""
    url = f"{API_BASE_URL}{path}"
    timeout_sec = timeout if timeout is not None else API_REQUEST_TIMEOUT_SECONDS
    try:
        response = requests.get(url, params=params, timeout=timeout_sec)
        response.raise_for_status()
        return response
    except requests.Timeout as e:
        raise RuntimeError(f"API GET timed out after {timeout_sec}s: {url}") from e
    except requests.RequestException as e:
        raise RuntimeError(f"API GET failed: {url} ({e})") from e


def _api_post(
    path: str, *, params: Optional[Dict[str, Any]] = None, timeout: Optional[float] = None
):
    """Issue a POST to CAO API with bounded timeout and actionable errors."""
    url = f"{API_BASE_URL}{path}"
    timeout_sec = timeout if timeout is not None else API_REQUEST_TIMEOUT_SECONDS
    try:
        response = requests.post(url, params=params, timeout=timeout_sec)
        response.raise_for_status()
        return response
    except requests.Timeout as e:
        raise RuntimeError(f"API POST timed out after {timeout_sec}s: {url}") from e
    except requests.RequestException as e:
        raise RuntimeError(f"API POST failed: {url} ({e})") from e


def _api_delete(
    path: str, *, params: Optional[Dict[str, Any]] = None, timeout: Optional[float] = None
):
    """Issue a DELETE to CAO API with bounded timeout and actionable errors."""
    url = f"{API_BASE_URL}{path}"
    timeout_sec = timeout if timeout is not None else API_REQUEST_TIMEOUT_SECONDS
    try:
        response = requests.delete(url, params=params, timeout=timeout_sec)
        response.raise_for_status()
        return response
    except requests.Timeout as e:
        raise RuntimeError(f"API DELETE timed out after {timeout_sec}s: {url}") from e
    except requests.RequestException as e:
        raise RuntimeError(f"API DELETE failed: {url} ({e})") from e


def _create_terminal(
    agent_profile: str, working_directory: Optional[str] = None
) -> Tuple[str, str]:
    """Create a new terminal with the specified agent profile.

    Args:
        agent_profile: Agent profile for the terminal
        working_directory: Optional working directory for the terminal

    Returns:
        Tuple of (terminal_id, provider)

    Raises:
        Exception: If terminal creation fails
    """
    provider = DEFAULT_PROVIDER
    parent_allowed_tools = None

    # Get current terminal ID from environment
    current_terminal_id = os.environ.get("CAO_TERMINAL_ID")
    if current_terminal_id:
        # Get terminal metadata via API
        response = _api_get(f"/terminals/{current_terminal_id}")
        terminal_metadata = response.json()

        provider = terminal_metadata["provider"]
        session_name = terminal_metadata["session_name"]
        parent_allowed_tools = terminal_metadata.get("allowed_tools")

        # If no working_directory specified, get conductor's current directory
        if working_directory is None:
            try:
                response = _api_get(
                    f"/terminals/{current_terminal_id}/working-directory",
                    timeout=WORKDIR_API_REQUEST_TIMEOUT,
                )
                working_directory = response.json().get("working_directory")
                logger.info(f"Inherited working directory from conductor: {working_directory}")
            except Exception as e:
                logger.warning(
                    f"Error fetching conductor's working directory: {e}, will use server default"
                )

        # Resolve child's allowed_tools via inheritance
        child_allowed_tools = _resolve_child_allowed_tools(parent_allowed_tools, agent_profile)

        # Create new terminal in existing session - always pass working_directory
        params = {"provider": provider, "agent_profile": agent_profile}
        if working_directory:
            params["working_directory"] = working_directory
        if child_allowed_tools:
            params["allowed_tools"] = child_allowed_tools

        terminal = _api_post(f"/sessions/{session_name}/terminals", params=params).json()
    else:
        # Create new session with terminal
        session_name = generate_session_name()
        params = {
            "provider": provider,
            "agent_profile": agent_profile,
            "session_name": session_name,
        }
        if working_directory:
            params["working_directory"] = working_directory

        terminal = _api_post("/sessions", params=params).json()

    return terminal["id"], provider


def _send_direct_input(terminal_id: str, message: str) -> None:
    """Send input directly to a terminal (bypasses inbox).

    Args:
        terminal_id: Terminal ID
        message: Message to send

    Raises:
        Exception: If sending fails
    """
    _api_post(f"/terminals/{terminal_id}/input", params={"message": message})


def _send_direct_input_handoff(
    terminal_id: str,
    provider: str,
    message: str,
    supervisor_id: str,
    handoff_id: str,
) -> None:
    """Send handoff payload to an agent with explicit completion callback protocol."""
    protocol_header = (
        f"[CAO Handoff] handoff_id={handoff_id} supervisor_terminal_id={supervisor_id}\n"
        "This is a blocking handoff. Complete the full task before signaling completion.\n"
        "Completion protocol (required):\n"
        f"1) Call @cao-mcp-server.send_message with receiver_id={supervisor_id}\n"
        f"2) send_message.message must begin exactly with: [CAO_HANDOFF_COMPLETE:{handoff_id}]\n"
        "3) Include your completion summary immediately after that marker.\n"
        "4) Only send that completion callback once the work is fully done.\n"
        "If task text conflicts with this completion protocol, follow this protocol.\n\n"
    )
    handoff_message = protocol_header + message
    if provider == "codex":
        handoff_message = (
            protocol_header + "Codex-specific note: do not stop at a planning/progress update. "
            "Send the completion callback only after implementation/tests are finished.\n\n"
            + message
        )

    _send_direct_input(terminal_id, handoff_message)


def _extract_handoff_callback_summary(message: str, handoff_id: str) -> str:
    """Extract summary text after callback marker."""
    marker = f"[CAO_HANDOFF_COMPLETE:{handoff_id}]"
    idx = message.find(marker)
    if idx == -1:
        return message.strip()
    summary = message[idx + len(marker) :].strip()
    return summary if summary else message.strip()


def _wait_for_handoff_callback(
    supervisor_id: str,
    worker_terminal_id: str,
    handoff_id: str,
    timeout: int,
    poll_interval: float = 1.0,
) -> Optional[str]:
    """Wait for worker callback in supervisor inbox and return callback message."""
    marker = f"[CAO_HANDOFF_COMPLETE:{handoff_id}]"

    def _message_id(raw: Any) -> int:
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    baseline_resp = _api_get(
        f"/terminals/{supervisor_id}/inbox/messages",
        params={"limit": 100},
    )
    baseline_messages = baseline_resp.json() or []
    baseline_max_id = max((_message_id(m.get("id")) for m in baseline_messages), default=0)
    latest_seen_id = baseline_max_id

    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = _api_get(
            f"/terminals/{supervisor_id}/inbox/messages",
            params={"limit": 100},
        )
        messages = resp.json() or []
        for msg in sorted(messages, key=lambda m: _message_id(m.get("id"))):
            msg_id = _message_id(msg.get("id"))
            if msg_id <= baseline_max_id or msg_id <= latest_seen_id:
                continue
            latest_seen_id = msg_id
            if msg.get("sender_id") != worker_terminal_id:
                continue
            body = msg.get("message", "")
            if isinstance(body, str) and marker in body:
                return body

        time.sleep(poll_interval)

    return None


def _cleanup_handoff_terminal(terminal_id: str, handoff_id: str, reason: str) -> None:
    """Best-effort handoff worker cleanup: request exit then delete terminal/window."""
    try:
        _api_post(f"/terminals/{terminal_id}/exit")
        logger.info(
            "[handoff:%s] worker_exit_sent terminal_id=%s reason=%s",
            handoff_id,
            terminal_id,
            reason,
        )
    except Exception as exit_error:
        logger.warning(
            "[handoff:%s] worker_exit_failed terminal_id=%s reason=%s error=%s",
            handoff_id,
            terminal_id,
            reason,
            exit_error,
        )

    try:
        _api_delete(f"/terminals/{terminal_id}")
        logger.info(
            "[handoff:%s] worker_deleted terminal_id=%s reason=%s",
            handoff_id,
            terminal_id,
            reason,
        )
    except Exception as delete_error:
        logger.warning(
            "[handoff:%s] worker_delete_failed terminal_id=%s reason=%s error=%s",
            handoff_id,
            terminal_id,
            reason,
            delete_error,
        )


def _send_direct_input_assign(terminal_id: str, message: str) -> None:
    """Send assign payload to a worker agent, appending callback instructions."""
    # Auto-inject sender terminal ID suffix when enabled
    if ENABLE_SENDER_ID_INJECTION:
        sender_id = os.environ.get("CAO_TERMINAL_ID", "unknown")
        message += (
            f"\n\n[Assigned by terminal {sender_id}. "
            f"When done, send results back to terminal {sender_id} using send_message]"
        )

    _send_direct_input(terminal_id, message)


def _send_to_inbox(receiver_id: str, message: str) -> Dict[str, Any]:
    """Send message to another terminal's inbox (queued delivery when IDLE).

    Args:
        receiver_id: Target terminal ID
        message: Message content

    Returns:
        Dict with message details

    Raises:
        ValueError: If CAO_TERMINAL_ID not set
        Exception: If API call fails
    """
    sender_id = os.getenv("CAO_TERMINAL_ID")
    if not sender_id:
        raise ValueError("CAO_TERMINAL_ID not set - cannot determine sender")

    response = _api_post(
        f"/terminals/{receiver_id}/inbox/messages",
        params={"sender_id": sender_id, "message": message},
    )
    return response.json()


def _extract_error_detail(response: requests.Response, fallback: str) -> str:
    """Extract a human-readable error detail from an API response."""
    try:
        payload = response.json()
    except ValueError:
        return fallback

    detail = payload.get("detail")
    if isinstance(detail, str) and detail:
        return detail
    return fallback


def _load_skill_impl(name: str) -> Union[str, Dict[str, Any]]:
    """Fetch a skill body from cao-server and return content or a structured error."""
    try:
        response = requests.get(f"{API_BASE_URL}/skills/{name}")
        response.raise_for_status()
        return response.json()["content"]
    except requests.HTTPError as exc:
        detail = str(exc)
        if exc.response is not None:
            detail = _extract_error_detail(exc.response, detail)
        return {"success": False, "error": detail}
    except requests.ConnectionError:
        return {
            "success": False,
            "error": "Failed to connect to cao-server. The server may not be running.",
        }
    except Exception as exc:
        return {"success": False, "error": f"Failed to retrieve skill: {str(exc)}"}


# Implementation functions
async def _handoff_impl(
    agent_profile: str, message: str, timeout: int = 600, working_directory: Optional[str] = None
) -> HandoffResult:
    """Implementation of handoff logic."""
    start_time = time.time()
    terminal_id: Optional[str] = None
    handoff_id = uuid4().hex[:8]
    supervisor_id = os.environ.get("CAO_TERMINAL_ID", "unknown")
    logger.info(
        "[handoff:%s] start supervisor=%s agent_profile=%s timeout=%ss working_directory=%s",
        handoff_id,
        supervisor_id,
        agent_profile,
        timeout,
        working_directory,
    )

    try:
        # Create terminal
        terminal_id, provider = _create_terminal(agent_profile, working_directory)
        logger.info(
            "[handoff:%s] worker_terminal_created terminal_id=%s provider=%s",
            handoff_id,
            terminal_id,
            provider,
        )

        # Wait for terminal to be ready (IDLE or COMPLETED) before sending
        # the handoff message. Accept COMPLETED in addition to IDLE because
        # providers that use an initial prompt flag process the system prompt
        # as the first user message and produce a response, reaching COMPLETED
        # without ever showing a bare IDLE state.
        # Both states indicate the provider is ready to accept input.
        #
        # Use a generous timeout (120s) because provider initialization can be
        # slow: shell warm-up (~5s), CLI startup with MCP server registration
        # (~10-30s), and API authentication (~5-10s). If the provider's own
        # initialize() timed out (60-90s), this acts as a fallback to catch
        # cases where the CLI starts slightly after the provider timeout.
        # Provider initialization can be slow (~15-45s depending on provider).
        if not wait_until_terminal_status(
            terminal_id,
            {TerminalStatus.IDLE, TerminalStatus.COMPLETED},
            timeout=120.0,
        ):
            logger.warning(
                "[handoff:%s] worker_not_ready terminal_id=%s timeout=120s",
                handoff_id,
                terminal_id,
            )
            _cleanup_handoff_terminal(terminal_id, handoff_id, reason="worker_not_ready")
            return HandoffResult(
                success=False,
                message=f"Terminal {terminal_id} did not reach ready status within 120 seconds",
                output=None,
                terminal_id=terminal_id,
            )

        await asyncio.sleep(2)  # wait another 2s

        # Send message to terminal with callback protocol so completion is explicit
        _send_direct_input_handoff(terminal_id, provider, message, supervisor_id, handoff_id)
        logger.info("[handoff:%s] worker_input_sent terminal_id=%s", handoff_id, terminal_id)

        callback_message = _wait_for_handoff_callback(
            supervisor_id,
            terminal_id,
            handoff_id,
            timeout,
        )
        if callback_message is None:
            timeout_output = None
            try:
                response = _api_get(f"/terminals/{terminal_id}/output", params={"mode": "last"})
                timeout_output = response.json().get("output")
            except Exception as output_error:
                logger.warning(
                    "[handoff:%s] failed_to_fetch_last_output terminal_id=%s error=%s",
                    handoff_id,
                    terminal_id,
                    output_error,
                )

            logger.warning(
                "[handoff:%s] callback_timeout terminal_id=%s timeout=%ss marker=%s",
                handoff_id,
                terminal_id,
                timeout,
                f"[CAO_HANDOFF_COMPLETE:{handoff_id}]",
            )
            if PRESERVE_TIMEOUT_HANDOFF_TERMINALS:
                logger.info(
                    "[handoff:%s] preserving_timed_out_worker terminal_id=%s",
                    handoff_id,
                    terminal_id,
                )
            else:
                _cleanup_handoff_terminal(terminal_id, handoff_id, reason="callback_timeout")

            return HandoffResult(
                success=False,
                message=(
                    f"Handoff timed out after {timeout} seconds waiting for completion callback "
                    f"[CAO_HANDOFF_COMPLETE:{handoff_id}]"
                ),
                output=timeout_output,
                terminal_id=terminal_id,
            )
        logger.info("[handoff:%s] callback_received terminal_id=%s", handoff_id, terminal_id)

        output = _extract_handoff_callback_summary(callback_message, handoff_id)
        logger.info(
            "[handoff:%s] callback_summary_extracted terminal_id=%s output_chars=%d",
            handoff_id,
            terminal_id,
            len(output) if output else 0,
        )

        _cleanup_handoff_terminal(terminal_id, handoff_id, reason="handoff_success")

        execution_time = time.time() - start_time
        logger.info(
            "[handoff:%s] success terminal_id=%s execution_time=%.2fs",
            handoff_id,
            terminal_id,
            execution_time,
        )

        return HandoffResult(
            success=True,
            message=f"Successfully handed off to {agent_profile} ({provider}) in {execution_time:.2f}s",
            output=output,
            terminal_id=terminal_id,
        )

    except Exception as e:
        if terminal_id:
            _cleanup_handoff_terminal(terminal_id, handoff_id, reason="handoff_exception")
        logger.exception("[handoff:%s] failure error=%s", handoff_id, e)
        return HandoffResult(
            success=False, message=f"Handoff failed: {str(e)}", output=None, terminal_id=None
        )


# Conditional tool registration based on environment variable
if ENABLE_WORKING_DIRECTORY:

    @mcp.tool()
    async def handoff(
        agent_profile: str = Field(
            description='The agent profile to hand off to (e.g., "developer", "analyst")'
        ),
        message: str = Field(description="The message/task to send to the target agent"),
        timeout: int = Field(
            default=600,
            description="Maximum time to wait for the agent to complete the task (in seconds)",
            ge=1,
            le=3600,
        ),
        working_directory: Optional[str] = Field(
            default=None,
            description='Optional working directory where the agent should execute (e.g., "/path/to/workspace/src/Package")',
        ),
    ) -> HandoffResult:
        """Hand off a task to another agent via CAO terminal and wait for completion.

        This tool allows handing off tasks to other agents by creating a new terminal
        in the same session. It sends the message, waits for completion, and captures the output.

        ## Usage

        Use this tool to hand off tasks to another agent and wait for the results.
        The tool will:
        1. Create a new terminal with the specified agent profile and provider
        2. Set the working directory for the terminal (defaults to supervisor's cwd)
        3. Send the message to the terminal
        4. Monitor until completion
        5. Return the agent's response
        6. Clean up the terminal with /exit

        ## Working Directory

        - By default, agents start in the supervisor's current working directory
        - You can specify a custom directory via working_directory parameter
        - Directory must exist and be accessible

        ## Requirements

        - Must be called from within a CAO terminal (CAO_TERMINAL_ID environment variable)
        - Target session must exist and be accessible
        - If working_directory is provided, it must exist and be accessible

        Args:
            agent_profile: The agent profile for the new terminal
            message: The task/message to send
            timeout: Maximum wait time in seconds
            working_directory: Optional directory path where agent should execute

        Returns:
            HandoffResult with success status, message, and agent output
        """
        return await _handoff_impl(agent_profile, message, timeout, working_directory)

else:

    @mcp.tool()
    async def handoff(
        agent_profile: str = Field(
            description='The agent profile to hand off to (e.g., "developer", "analyst")'
        ),
        message: str = Field(description="The message/task to send to the target agent"),
        timeout: int = Field(
            default=600,
            description="Maximum time to wait for the agent to complete the task (in seconds)",
            ge=1,
            le=3600,
        ),
    ) -> HandoffResult:
        """Hand off a task to another agent via CAO terminal and wait for completion.

        This tool allows handing off tasks to other agents by creating a new terminal
        in the same session. It sends the message, waits for completion, and captures the output.

        ## Usage

        Use this tool to hand off tasks to another agent and wait for the results.
        The tool will:
        1. Create a new terminal with the specified agent profile and provider
        2. Send the message to the terminal (starts in supervisor's current directory)
        3. Monitor until completion
        4. Return the agent's response
        5. Clean up the terminal with /exit

        ## Requirements

        - Must be called from within a CAO terminal (CAO_TERMINAL_ID environment variable)
        - Target session must exist and be accessible

        Args:
            agent_profile: The agent profile for the new terminal
            message: The task/message to send
            timeout: Maximum wait time in seconds

        Returns:
            HandoffResult with success status, message, and agent output
        """
        return await _handoff_impl(agent_profile, message, timeout, None)


# Implementation function for assign
def _assign_impl(
    agent_profile: str, message: str, working_directory: Optional[str] = None
) -> Dict[str, Any]:
    """Implementation of assign logic."""
    try:
        # Create terminal
        terminal_id, _ = _create_terminal(agent_profile, working_directory)

        # Send message immediately (auto-injects sender terminal ID suffix when enabled)
        _send_direct_input_assign(terminal_id, message)

        return {
            "success": True,
            "terminal_id": terminal_id,
            "message": f"Task assigned to {agent_profile} (terminal: {terminal_id})",
        }

    except Exception as e:
        return {"success": False, "terminal_id": None, "message": f"Assignment failed: {str(e)}"}


def _build_assign_description(enable_sender_id: bool, enable_workdir: bool) -> str:
    """Build the assign tool description based on feature flags."""
    # Build tool description overview.
    if enable_sender_id:
        desc = """\
Assigns a task to another agent without blocking.

The sender's terminal ID and callback instructions will automatically be appended to the message."""
    else:
        desc = """\
Assigns a task to another agent without blocking.

In the message to the worker agent include instruction to send results back via send_message tool.
**IMPORTANT**: The terminal id of each agent is available in environment variable CAO_TERMINAL_ID.
When assigning, first find out your own CAO_TERMINAL_ID value, then include the terminal_id value in the message to the worker agent to allow callback.
Example message: "Analyze the logs. When done, send results back to terminal ee3f93b3 using send_message tool.\""""

    if enable_workdir:
        desc += """

## Working Directory

- By default, agents start in the supervisor's current working directory
- You can specify a custom directory via working_directory parameter
- Directory must exist and be accessible"""

    desc += """

Args:
    agent_profile: Agent profile for the worker terminal
    message: Task message (include callback instructions)"""

    if enable_workdir:
        desc += """
    working_directory: Optional working directory where the agent should execute"""

    desc += """

Returns:
    Dict with success status, worker terminal_id, and message"""

    return desc


_assign_description = _build_assign_description(
    ENABLE_SENDER_ID_INJECTION, ENABLE_WORKING_DIRECTORY
)
_assign_message_field_desc = (
    "The task message to send to the worker agent."
    if ENABLE_SENDER_ID_INJECTION
    else "The task message to send. Include callback instructions for the worker to send results back."
)

if ENABLE_WORKING_DIRECTORY:

    @mcp.tool(description=_assign_description)
    async def assign(
        agent_profile: str = Field(
            description='The agent profile for the worker agent (e.g., "developer", "analyst")'
        ),
        message: str = Field(description=_assign_message_field_desc),
        working_directory: Optional[str] = Field(
            default=None, description="Optional working directory where the agent should execute"
        ),
    ) -> Dict[str, Any]:
        return _assign_impl(agent_profile, message, working_directory)

else:

    @mcp.tool(description=_assign_description)
    async def assign(
        agent_profile: str = Field(
            description='The agent profile for the worker agent (e.g., "developer", "analyst")'
        ),
        message: str = Field(description=_assign_message_field_desc),
    ) -> Dict[str, Any]:
        return _assign_impl(agent_profile, message, None)


# Implementation function for send_message
def _send_message_impl(receiver_id: str, message: str) -> Dict[str, Any]:
    """Implementation of send_message logic."""
    try:
        # Auto-inject sender terminal ID suffix when enabled
        if ENABLE_SENDER_ID_INJECTION:
            sender_id = os.environ.get("CAO_TERMINAL_ID", "unknown")
            message += (
                f"\n\n[Message from terminal {sender_id}. "
                "Use send_message MCP tool for any follow-up work.]"
            )

        return _send_to_inbox(receiver_id, message)
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
async def send_message(
    receiver_id: str = Field(description="Target terminal ID to send message to"),
    message: str = Field(description="Message content to send"),
) -> Dict[str, Any]:
    """Send a message to another terminal's inbox.

    The message will be delivered when the destination terminal is IDLE.
    Messages are delivered in order (oldest first).

    Args:
        receiver_id: Terminal ID of the receiver
        message: Message content to send

    Returns:
        Dict with success status and message details
    """
    return _send_message_impl(receiver_id, message)


@mcp.tool(description=LOAD_SKILL_TOOL_DESCRIPTION)
async def load_skill(
    name: str = Field(description="Name of the skill to retrieve"),
) -> Any:
    """Retrieve skill content from cao-server."""
    return _load_skill_impl(name)


def main():
    """Main entry point for the MCP server."""
    # Disable FastMCP startup banner/update notices on stdio transport.
    # Any non-protocol stdout can corrupt MCP framing and cause tool calls
    # to hang without reaching server handlers.
    mcp.run(show_banner=False)


if __name__ == "__main__":
    main()
