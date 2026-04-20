"""In-process orchestration runtime bootstrap and run-level wakeup signaling."""

from __future__ import annotations

import asyncio
import logging
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from watchdog.events import DirModifiedEvent, FileModifiedEvent, FileSystemEventHandler

from cli_agent_orchestrator.clients.orchestration_store import OrchestrationStore
from cli_agent_orchestrator.services.orchestration_callbacks import (
    CallbackIngestionResult,
    OrchestrationCallbackIngestor,
)

logger = logging.getLogger(__name__)


@dataclass
class _RunSignal:
    """Per-run in-process signal state for wakeup coordination."""

    condition: asyncio.Condition
    version: int = 0


class OrchestrationRuntime:
    """Server-owned orchestration runtime state and callback ingestion."""

    def __init__(
        self,
        *,
        store: OrchestrationStore,
        callback_ingestor: Optional[OrchestrationCallbackIngestor] = None,
    ):
        self._store = store
        self._callback_ingestor = callback_ingestor or OrchestrationCallbackIngestor(store=store)
        self._running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._run_signals: Dict[str, _RunSignal] = {}
        self._log_offsets: Dict[str, int] = {}
        self._offset_lock = threading.Lock()

    @property
    def store(self) -> OrchestrationStore:
        """Expose the backing store for downstream orchestration services."""

        return self._store

    @property
    def callback_ingestor(self) -> OrchestrationCallbackIngestor:
        """Expose callback ingestor for targeted ingestion calls."""

        return self._callback_ingestor

    def is_running(self) -> bool:
        """Return whether runtime startup has completed."""

        return self._running

    async def start(self) -> None:
        """Start runtime ownership for this process."""

        if self._running:
            return
        self._loop = asyncio.get_running_loop()
        self._running = True
        logger.info("Orchestration runtime started")

    async def stop(self) -> None:
        """Stop runtime ownership and release in-memory signaling state."""

        if not self._running:
            return

        self._running = False

        for signal in list(self._run_signals.values()):
            async with signal.condition:
                signal.version += 1
                signal.condition.notify_all()

        self._run_signals.clear()
        with self._offset_lock:
            self._log_offsets.clear()
        self._loop = None
        logger.info("Orchestration runtime stopped")

    def current_signal_cursor(self, *, run_id: str) -> int:
        """Get current in-process signal cursor for a run."""

        return self._get_or_create_signal(run_id).version

    async def wait_for_run_update(self, *, run_id: str, cursor: int, timeout_sec: int) -> int:
        """Wait for a run signal cursor to advance, bounded by timeout."""

        signal = self._get_or_create_signal(run_id)
        async with signal.condition:
            if signal.version > cursor:
                return signal.version

            try:
                await asyncio.wait_for(signal.condition.wait(), timeout=float(timeout_sec))
            except asyncio.TimeoutError:
                return signal.version

            return signal.version

    def notify_run_update(self, *, run_id: str) -> None:
        """Notify in-process waiters that a run has new state/events."""

        if not self._running or self._loop is None:
            return

        def _schedule() -> None:
            asyncio.create_task(self._bump_signal(run_id=run_id))

        try:
            self._loop.call_soon_threadsafe(_schedule)
        except RuntimeError:
            logger.debug("Skipping run update notification after loop shutdown", exc_info=True)

    async def _bump_signal(self, *, run_id: str) -> None:
        signal = self._get_or_create_signal(run_id)
        async with signal.condition:
            signal.version += 1
            signal.condition.notify_all()

    def ingest_log_update(self, *, terminal_id: str, log_path: Path) -> CallbackIngestionResult:
        """Ingest callback markers from newly appended terminal log text."""

        new_text = self._read_new_log_text(terminal_id=terminal_id, log_path=log_path)
        if not new_text:
            return CallbackIngestionResult()

        result = self._callback_ingestor.ingest_terminal_output(
            terminal_id=terminal_id, output=new_text
        )
        for run_id in result.affected_run_ids:
            self.notify_run_update(run_id=run_id)
        return result

    def build_log_file_handler(self) -> FileSystemEventHandler:
        """Create a watchdog handler for orchestration callback ingestion."""

        return _OrchestrationLogFileHandler(runtime=self)

    def _get_or_create_signal(self, run_id: str) -> _RunSignal:
        signal = self._run_signals.get(run_id)
        if signal is not None:
            return signal
        signal = _RunSignal(condition=asyncio.Condition())
        self._run_signals[run_id] = signal
        return signal

    def _read_new_log_text(self, *, terminal_id: str, log_path: Path) -> str:
        try:
            file_size = log_path.stat().st_size
        except FileNotFoundError:
            return ""

        previous_offset = self._get_known_log_offset(terminal_id=terminal_id)
        if previous_offset is None:
            previous_offset = 0

        read_offset = previous_offset
        if file_size < previous_offset:
            read_offset = 0

        if file_size == read_offset:
            if previous_offset != file_size:
                self._persist_log_offset(terminal_id=terminal_id, byte_offset=file_size)
            return ""

        with log_path.open("r", encoding="utf-8", errors="ignore") as handle:
            handle.seek(read_offset)
            text = handle.read()

        self._persist_log_offset(terminal_id=terminal_id, byte_offset=file_size)

        return text

    def _get_known_log_offset(self, *, terminal_id: str) -> Optional[int]:
        with self._offset_lock:
            cached_offset = self._log_offsets.get(terminal_id)
        if cached_offset is not None:
            return cached_offset

        persisted_offset = self._store.get_terminal_log_offset(terminal_id=terminal_id)
        if persisted_offset is None:
            return None

        normalized_offset = max(0, int(persisted_offset))
        with self._offset_lock:
            self._log_offsets[terminal_id] = normalized_offset
        return normalized_offset

    def _persist_log_offset(self, *, terminal_id: str, byte_offset: int) -> None:
        normalized_offset = self._store.upsert_terminal_log_offset(
            terminal_id=terminal_id,
            byte_offset=byte_offset,
        )
        with self._offset_lock:
            self._log_offsets[terminal_id] = normalized_offset


class _OrchestrationLogFileHandler(FileSystemEventHandler):
    """Watch terminal logs and ingest orchestration callback markers."""

    def __init__(self, *, runtime: OrchestrationRuntime):
        super().__init__()
        self._runtime = runtime

    def on_modified(self, event: DirModifiedEvent | FileModifiedEvent) -> None:
        if not isinstance(event, FileModifiedEvent):
            return

        src_path = (
            event.src_path.decode("utf-8", errors="ignore")
            if isinstance(event.src_path, bytes)
            else event.src_path
        )
        if not src_path.endswith(".log"):
            return

        log_path = Path(src_path)
        terminal_id = log_path.stem

        try:
            self._runtime.ingest_log_update(terminal_id=terminal_id, log_path=log_path)
        except Exception:
            logger.warning(
                "Failed to ingest orchestration callback markers for terminal %s",
                terminal_id,
                exc_info=True,
            )
