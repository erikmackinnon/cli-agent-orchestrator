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
    MARKER_PREFIX,
    MARKER_SUFFIX,
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

    DEFAULT_LOG_READ_OVERLAP_BYTES = 8 * 1024

    def __init__(
        self,
        *,
        store: OrchestrationStore,
        callback_ingestor: Optional[OrchestrationCallbackIngestor] = None,
        log_read_overlap_bytes: int = DEFAULT_LOG_READ_OVERLAP_BYTES,
    ):
        self._store = store
        self._callback_ingestor = callback_ingestor or OrchestrationCallbackIngestor(store=store)
        self._log_read_overlap_bytes = max(0, int(log_read_overlap_bytes))
        self._running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._run_signals: Dict[str, _RunSignal] = {}
        self._log_offsets: Dict[str, int] = {}
        self._offset_lock = threading.Lock()
        marker_start_seed = MARKER_PREFIX.rsplit("v1:", 1)[0]
        self._marker_start_seed = marker_start_seed.encode("utf-8")
        self._marker_version_prefix = MARKER_PREFIX[len(marker_start_seed) :].encode("utf-8")
        self._marker_suffix = MARKER_SUFFIX.encode("utf-8")

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

        chunk = self._read_new_log_chunk(terminal_id=terminal_id, log_path=log_path)
        if chunk is None:
            return CallbackIngestionResult()

        read_offset, file_size, new_bytes = chunk
        output = new_bytes.decode("utf-8", errors="ignore")
        result = (
            self._callback_ingestor.ingest_terminal_output(terminal_id=terminal_id, output=output)
            if output
            else CallbackIngestionResult()
        )

        pending_start = self._find_pending_marker_start(new_bytes)
        next_offset = file_size if pending_start is None else read_offset + pending_start
        self._persist_log_offset(terminal_id=terminal_id, byte_offset=next_offset)

        for run_id in result.run_ids_with_new_events:
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

    def _read_new_log_chunk(
        self, *, terminal_id: str, log_path: Path
    ) -> Optional[tuple[int, int, bytes]]:
        try:
            file_size = log_path.stat().st_size
        except FileNotFoundError:
            return None

        previous_offset = self._get_known_log_offset(terminal_id=terminal_id)
        if previous_offset is None:
            previous_offset = 0

        read_offset = previous_offset
        if file_size < previous_offset:
            read_offset = 0

        if file_size == read_offset:
            if previous_offset != file_size:
                self._persist_log_offset(terminal_id=terminal_id, byte_offset=file_size)
            return None

        with log_path.open("rb") as handle:
            handle.seek(read_offset)
            data = handle.read()

        return read_offset, file_size, data

    def _find_pending_marker_start(self, chunk: bytes) -> Optional[int]:
        if not chunk:
            return None

        last_seed = chunk.rfind(self._marker_start_seed)
        if last_seed != -1:
            suffix_after_seed = chunk.find(
                self._marker_suffix,
                last_seed + len(self._marker_start_seed),
            )
            if suffix_after_seed == -1 and self._looks_like_marker_seed_continuation(
                chunk_before_seed=chunk[:last_seed],
                tail_after_seed=chunk[last_seed + len(self._marker_start_seed) :],
            ):
                return last_seed

        partial_seed_size = self._trailing_marker_seed_overlap(chunk)
        if partial_seed_size > 0:
            return len(chunk) - partial_seed_size
        return None

    def _looks_like_marker_seed_continuation(
        self, *, chunk_before_seed: bytes, tail_after_seed: bytes
    ) -> bool:
        normalized_tail = self._skip_marker_leading_artifacts(tail_after_seed)
        if not normalized_tail:
            return not chunk_before_seed
        return self._marker_version_prefix.startswith(
            normalized_tail
        ) or normalized_tail.startswith(self._marker_version_prefix)

    def _skip_marker_leading_artifacts(self, data: bytes) -> bytes:
        index = 0
        while index < len(data):
            byte = data[index]
            if byte in b" \t\r\n":
                index += 1
                continue

            if byte == 0x1B:
                consumed = self._consume_ansi_escape(data=data, index=index)
                if consumed > index:
                    index = consumed
                    continue
            break
        return data[index:]

    def _consume_ansi_escape(self, *, data: bytes, index: int) -> int:
        if index + 1 >= len(data):
            return len(data)

        esc_type = data[index + 1]
        if esc_type == 0x5B:
            cursor = index + 2
            while cursor < len(data):
                if 0x40 <= data[cursor] <= 0x7E:
                    return cursor + 1
                cursor += 1
            return len(data)

        if esc_type == 0x5D:
            cursor = index + 2
            while cursor < len(data):
                byte = data[cursor]
                if byte == 0x07:
                    return cursor + 1
                if byte == 0x1B and cursor + 1 < len(data) and data[cursor + 1] == 0x5C:
                    return cursor + 2
                cursor += 1
            return len(data)

        if 0x40 <= esc_type <= 0x5F:
            return index + 2

        return len(data)

    def _trailing_marker_seed_overlap(self, chunk: bytes) -> int:
        max_overlap = min(len(self._marker_start_seed) - 1, len(chunk))
        for overlap in range(max_overlap, 0, -1):
            if chunk[-overlap:] == self._marker_start_seed[:overlap]:
                return overlap
        return 0

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
