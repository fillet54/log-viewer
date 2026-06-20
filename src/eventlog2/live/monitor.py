from __future__ import annotations

import dataclasses
import json
import queue
import threading
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Callable

from .storage import SessionMeta, SessionStore


class LiveMonitorPlugin(ABC):
    """Abstract interface a plugin implements to support live monitoring."""

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
        store: SessionStore,
        monitor: LiveMonitorPlugin,
        plugin_id: str,
        build_page_data: Callable[[Any], dict[str, Any]],
        normalize_events: Callable[[list[dict[str, Any]], list[str]], list[dict[str, Any]]] | None = None,
    ):
        self._store = store
        self._monitor = monitor
        self._plugin_id = plugin_id
        self._build_page_data = build_page_data
        self._normalize_events = normalize_events
        self._active_session_id: str | None = None
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._subscribers: list[queue.Queue] = []

    @property
    def active_session_id(self) -> str | None:
        with self._lock:
            return self._active_session_id

    def start(self) -> str:
        with self._lock:
            if self._active_session_id:
                raise RuntimeError("A session is already active")
            if not self._monitor.detect_system():
                raise RuntimeError("System not detected")

            session_id = str(uuid.uuid4())
            channels = self._monitor.get_channels()
            meta = SessionMeta(
                id=session_id,
                started_at=datetime.now(timezone.utc),
                ended_at=None,
                channels=channels,
                event_count=0,
                status="active",
                plugin_id=self._plugin_id,
            )
            self._store.create_session(session_id, meta)
            self._active_session_id = session_id

        self._monitor.start_session(session_id, channels)
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._poll_loop,
            args=(session_id,),
            daemon=True,
        )
        self._thread.start()
        return session_id

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)
        self._monitor.stop_session()

        with self._lock:
            self._active_session_id = None
            for q in self._subscribers:
                try:
                    q.put_nowait(None)
                except queue.Full:
                    pass
            self._subscribers.clear()

    def _poll_loop(self, session_id: str) -> None:
        event_count = 0
        meta = self._store.get_meta(session_id)
        channels = meta.channels if meta else []

        while not self._stop_event.wait(self.POLL_INTERVAL):
            try:
                raw_events = self._monitor.poll_events()
            except Exception:
                continue

            if not raw_events:
                continue

            self._store.append_events(session_id, raw_events)
            event_count += len(raw_events)

            meta = self._store.get_meta(session_id)
            if meta:
                self._store.update_meta(dataclasses.replace(meta, event_count=event_count))

            broadcast_events = (
                self._normalize_events(raw_events, channels)
                if self._normalize_events is not None
                else raw_events
            )
            payload = {"type": "events", "events": broadcast_events}
            with self._lock:
                dead = []
                for q in self._subscribers:
                    try:
                        q.put_nowait(payload)
                    except queue.Full:
                        dead.append(q)
                for q in dead:
                    self._subscribers.remove(q)

        meta = self._store.get_meta(session_id)
        if meta:
            self._store.update_meta(
                dataclasses.replace(
                    meta,
                    ended_at=datetime.now(timezone.utc),
                    status="completed",
                    event_count=event_count,
                )
            )

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

    def get_session_page_data(self, session_id: str) -> dict[str, Any] | None:
        meta = self._store.get_meta(session_id)
        if not meta:
            return None
        events = self._store.get_events(session_id)
        payload: dict[str, Any] = {
            "events": events,
            "channels": meta.channels,
        }
        if meta.started_at:
            payload["start"] = meta.started_at.isoformat()
        if meta.ended_at:
            payload["end"] = meta.ended_at.isoformat()
        return self._build_page_data(payload)
