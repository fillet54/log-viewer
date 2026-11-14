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
from .core.registry import Registry
from .logdir import LogDir

BACKLOG_BATCHES = 1000


class LiveHub:
    def __init__(self, root: Path, registry: Registry):
        self.root = Path(root)
        self.registry = registry
        self._lock = threading.Lock()
        self._current: LogDir | None = None
        self._seq = 0
        # type id -> {"rows", "events"} for the current log (see post)
        self._sections: dict[str, dict[str, Any]] = {}
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
        self._sections.clear()
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
        starting one if needed, and push the normalized events to viewers.

        The type is re-normalized with all of its rows, not just this batch, so
        anything that depends on earlier rows (core-event SET/CLEAR pairing,
        times relative to the first row) comes out the same as on a full page
        load. Earlier events that changed are sent as ``updates``."""
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
            types = log.types() if log else []
            is_new_type = type_id not in types
            changed_header = header is not None
            if header is None:
                header = log.header(type_id) if log else {}
            slots = sorted({*types, type_id})
            previous = self._section(log, log_type, types, slots)

            # Normalize first so a bad batch is rejected before anything is stored.
            all_rows = previous["rows"] + rows
            events = self._normalize(log_type, header, all_rows, slots) if rows else []
            tracks = log_type.tracks({**(header or {}), "events": rows}) if rows else []
            log = log or self._start(None)
            if changed_header:
                log.set_header(type_id, header)
            if not rows:
                if changed_header:
                    self._sections.clear()
                    self._broadcast({"type": "reload"})
                return log
            log.append(type_id, rows)
            self._seq += 1
            if is_new_type or changed_header:
                # The viewer needs this type's config (and a new type moves
                # every type's id slot), so have it re-fetch the page.
                self._sections.clear()
                message = {"type": "reload", "seq": self._seq}
            else:
                old_events = previous["events"]
                self._sections[type_id] = {"rows": all_rows, "events": events}
                message = {
                    "type": "events",
                    "seq": self._seq,
                    "events": events[len(old_events) :],
                    "updates": [
                        new for old, new in zip(old_events, events) if old != new
                    ],
                }
                if tracks:
                    message["tracks"] = tracks
            self._backlog.append(message)
            self._broadcast(message)
            return log

    def _normalize(self, log_type, header, rows, slots) -> list[dict[str, Any]]:
        return normalize_section(
            log_type,
            {**(header or {}), "events": rows},
            row_id_base=slots.index(log_type.id),
            row_id_step=len(slots),
            mode="live",
        )

    def _section(self, log, log_type, types, slots) -> dict[str, Any]:
        """The current log's rows and events for one type, cached in memory."""
        if log is None or log_type.id not in types:
            return {"rows": [], "events": []}
        cached = self._sections.get(log_type.id)
        if cached is None:
            rows = log.rows(log_type.id)
            header = log.header(log_type.id)
            events = self._normalize(log_type, header, rows, slots) if rows else []
            cached = self._sections[log_type.id] = {"rows": rows, "events": events}
        return cached

    def page_data(self, log: LogDir) -> dict[str, Any]:
        """Assemble a log for the viewer; for the current log, include the
        stream position so the viewer can pick up exactly where it left off."""
        with self._lock:
            page = assemble_page_data(
                log_dir_sections(log, self.registry), self.registry
            )
            if self._current is not None and log.id == self._current.id:
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
