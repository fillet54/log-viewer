"""Live mode: rows posted over HTTP are persisted to a log directory and
pushed to every open viewer of that log."""

from __future__ import annotations

import queue
import threading
from collections import deque
from pathlib import Path
from typing import Any

from .core.assemble import assemble_page_data, normalize_section
from .core.bundle import log_dir_sections
from .core.registry import LogTypeRegistry
from .logdir import LogDir

BACKLOG_BATCHES = 1000


class LiveHub:
    def __init__(self, root: Path, registry: LogTypeRegistry):
        self.root = Path(root)
        self.registry = registry
        self._lock = threading.Lock()
        self._current: LogDir | None = None
        self._next_row_id = 0
        self._seq = 0
        self._backlog: deque[dict[str, Any]] = deque(maxlen=BACKLOG_BATCHES)
        self._subscribers: list[queue.Queue] = []

    @property
    def current(self) -> LogDir | None:
        return self._current

    def start(self, name: str | None = None) -> LogDir:
        with self._lock:
            return self._start(name)

    def _start(self, name: str | None) -> LogDir:
        self._current = LogDir.create(self.root, name)
        self._next_row_id = 0
        self._seq = 0
        self._backlog.clear()
        self._broadcast({"type": "log", "logId": self._current.id})
        return self._current

    def post(
        self,
        type_id: str,
        rows: list[dict[str, Any]],
        header: dict[str, Any] | None = None,
    ) -> LogDir:
        """Persist ``rows`` (and optional type ``header``) to the current log,
        starting one if needed, and push the normalized events to viewers."""
        log_type = self.registry.get(type_id)
        if log_type is None:
            known = ", ".join(item.id for item in self.registry.all())
            raise ValueError(f"Unknown log type {type_id!r}. Registered: {known}")
        if not isinstance(rows, list) or any(not isinstance(r, dict) for r in rows):
            raise ValueError("events must be a list of JSON objects")
        if header is not None and not isinstance(header, dict):
            raise ValueError("header must be a JSON object")

        with self._lock:
            log = self._current
            if header is None:
                header = None if log is None else log.header(type_id) or None
                changed_header = False
            else:
                changed_header = True
            # Normalize first so a bad batch is rejected before anything is stored.
            events = (
                normalize_section(
                    log_type,
                    {**(header or {}), "events": rows},
                    row_id_base=self._next_row_id,
                    mode="live",
                )
                if rows
                else []
            )
            log = log or self._start(None)
            is_new_type = type_id not in log.types()
            if changed_header:
                log.set_header(type_id, header)
            if not rows:
                if changed_header:
                    self._broadcast({"type": "reload"})
                return log
            log.append(type_id, rows)
            self._next_row_id = max([self._next_row_id, *(e["row_id"] for e in events)])
            self._seq += 1
            if is_new_type or changed_header:
                # The viewer needs this type's config, so have it re-fetch.
                message = {"type": "reload", "seq": self._seq}
            else:
                message = {"type": "events", "seq": self._seq, "events": events}
            self._backlog.append(message)
            self._broadcast(message)
            return log

    def page_data(self, log: LogDir) -> dict[str, Any]:
        """Assemble a log for the viewer; for the current log, include the
        stream position so the viewer can pick up exactly where it left off."""
        with self._lock:
            page = assemble_page_data(log_dir_sections(log, self.registry))
            if self._current is not None and log.id == self._current.id:
                # Live ids must not collide with ids the viewer already has.
                self._next_row_id = max(
                    self._next_row_id, len(page["logData"]["events"])
                )
                page["live"] = {"logId": log.id, "seq": self._seq}
            return page

    def subscribe(self, log_id: str | None, after: int) -> queue.Queue:
        subscriber: queue.Queue = queue.Queue(maxsize=BACKLOG_BATCHES)
        with self._lock:
            current_id = self._current.id if self._current else None
            if log_id != current_id:
                if current_id:
                    subscriber.put({"type": "log", "logId": current_id})
            elif self._seq > after:
                missed = [m for m in self._backlog if m["seq"] > after]
                if not missed or missed[0]["seq"] != after + 1:
                    missed = [{"type": "reload", "seq": self._seq}]
                for message in missed:
                    subscriber.put(message)
            self._subscribers.append(subscriber)
        return subscriber

    def unsubscribe(self, subscriber: queue.Queue) -> None:
        with self._lock:
            if subscriber in self._subscribers:
                self._subscribers.remove(subscriber)

    def _broadcast(self, message: dict[str, Any]) -> None:
        for subscriber in list(self._subscribers):
            try:
                subscriber.put_nowait(message)
            except queue.Full:
                # A stalled viewer: drop what it missed and have it re-fetch.
                with subscriber.mutex:
                    subscriber.queue.clear()
                subscriber.put_nowait({"type": "reload", "seq": self._seq})
