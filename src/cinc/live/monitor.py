from __future__ import annotations

import queue
import threading
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from ..logs.store import LogStore
from ..logs.types import LogTypeDefinition


class LiveMonitorPlugin(ABC):
    """Interface implemented by the single core-event live monitor."""

    @abstractmethod
    def detect_system(self) -> bool:
        """Return True if the target system is reachable."""

    @abstractmethod
    def get_channels(self) -> list[str]:
        """Return the list of channel names to monitor."""

    @abstractmethod
    def start_session(self, session_id: str, channels: list[str]) -> None:
        """Called once when monitoring starts. Download any backlog here."""

    @abstractmethod
    def poll_events(self) -> list[dict[str, Any]]:
        """Return raw new events since last poll. Called every POLL_INTERVAL seconds."""

    @abstractmethod
    def stop_session(self) -> None:
        """Called when monitoring should stop."""


class SessionManager:
    POLL_INTERVAL = 2.0

    def __init__(
        self,
        store: LogStore,
        monitor: LiveMonitorPlugin,
        log_type: LogTypeDefinition,
    ):
        self._store = store
        self._monitor = monitor
        self._log_type = log_type
        self._active_session_id: str | None = None
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._subscribers: list[queue.Queue] = []

    @property
    def log_type_id(self) -> str:
        return self._log_type.full_id

    @property
    def active_session_id(self) -> str | None:
        with self._lock:
            if self._active_session_id:
                return self._active_session_id
        active = self._store.get_active_live_record(self.log_type_id)
        return active.id if active else None

    def start(self) -> str:
        with self._lock:
            if self._active_session_id or self._store.get_active_live_record(self.log_type_id):
                raise RuntimeError("A live capture is already active")
            if not self._monitor.detect_system():
                raise RuntimeError("System not detected")

            started_at = datetime.now(timezone.utc)
            channels = self._monitor.get_channels()
            record = self._store.create_log(
                self._log_type.full_id,
                self._log_type.plugin_id,
                f"Live capture {started_at.strftime('%Y-%m-%d %H:%M')}",
                {
                    "event_count": 0,
                    "hours": 0,
                    "channels": channels,
                    "payload_header": {
                        "channels": channels,
                        "start": started_at.isoformat(),
                    },
                },
                source="live",
                status="active",
                started_at=started_at,
            )
            self._active_session_id = record.id

        self._monitor.start_session(record.id, channels)
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._poll_loop,
            args=(record.id, channels),
            daemon=True,
        )
        self._thread.start()
        return record.id

    def stop(self) -> None:
        active_id = self.active_session_id
        if not active_id:
            return

        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)
        self._monitor.stop_session()
        if not self._thread:
            self._store.complete_log(active_id)

        with self._lock:
            self._active_session_id = None
            for subscriber in self._subscribers:
                try:
                    subscriber.put_nowait(None)
                except queue.Full:
                    pass
            self._subscribers.clear()

    def _poll_loop(self, log_id: str, channels: list[str]) -> None:
        while not self._stop_event.wait(self.POLL_INTERVAL):
            try:
                raw_events = self._monitor.poll_events()
            except Exception:
                continue

            if not raw_events:
                continue

            payload = {"events": raw_events, "channels": channels}
            record = self._store.get_record(log_id)
            if record and record.started_at:
                payload["start"] = record.started_at.isoformat()
            normalized_events = self._log_type.normalize_events(payload)
            self._store.append_events(log_id, normalized_events, source="live", tags=("live",))

            broadcast = {"type": "events", "events": normalized_events}
            with self._lock:
                dead = []
                for subscriber in self._subscribers:
                    try:
                        subscriber.put_nowait(broadcast)
                    except queue.Full:
                        dead.append(subscriber)
                for subscriber in dead:
                    self._subscribers.remove(subscriber)

        record = self._store.get_record(log_id)
        ended_at = datetime.now(timezone.utc)
        if record:
            metadata = dict(record.metadata or {})
            if record.started_at:
                metadata["hours"] = max(0.0, (ended_at - record.started_at).total_seconds() / 3600)
            header = dict(metadata.get("payload_header") or {})
            header["end"] = ended_at.isoformat()
            metadata["payload_header"] = header
            self._store.update_record_metadata(log_id, metadata)
        self._store.complete_log(log_id, ended_at=ended_at)

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=500)
        with self._lock:
            self._subscribers.append(q)
        return q

    def unsubscribe(self, q: queue.Queue) -> None:
        with self._lock:
            try:
                self._subscribers.remove(q)
            except ValueError:
                pass
