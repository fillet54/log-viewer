"""Live mode: rows posted over HTTP are persisted to a log directory and
pushed to every open viewer of that log."""

from __future__ import annotations

import logging
import queue
import threading
from collections import deque
from pathlib import Path
from typing import Any

from .core.assemble import (
    assemble_page_data,
    derive_events,
    finish_events,
    keyed_events,
    type_config,
)
from .core.log_type import NeedsRebuild
from .core.bundle import log_dir_sections
from .core.registry import Registry
from .logdir import LogDir

BACKLOG_BATCHES = 1000

logger = logging.getLogger(__name__)


class LiveHub:
    def __init__(self, root: Path, registry: Registry):
        self.root = Path(root)
        self.registry = registry
        self._lock = threading.Lock()
        self._current: LogDir | None = None
        self._seq = 0
        # The current log, kept in memory so a post reads nothing from disk:
        # its type ids (None until read), each type's header, and each type's
        # {"rows", "events", "instants"} (see post).
        self._types: list[str] | None = None
        self._headers: dict[str, dict[str, Any]] = {}
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
        self._forget()
        self._seq = 0
        self._backlog.clear()
        self._broadcast({"type": "log", "logId": self._current.id})
        return self._current

    def _forget(self) -> None:
        """Drop what is kept in memory about the current log."""
        self._types = None
        self._headers.clear()
        self._sections.clear()

    def _current_types(self) -> list[str]:
        if self._current is None:
            return []
        if self._types is None:
            self._types = self._current.types()
        return self._types

    def _header(self, type_id: str) -> dict[str, Any]:
        if self._current is None:
            return {}
        if type_id not in self._headers:
            self._headers[type_id] = self._current.header(type_id)
        return self._headers[type_id]

    def post(
        self,
        type_id: str,
        rows: list[dict[str, Any]],
        header: dict[str, Any] | None = None,
    ) -> LogDir:
        """Persist ``rows`` (and optional type ``header``) to the current log,
        starting one if needed, and push the normalized events to viewers.

        Events come out the same as on a full page load, including those that
        depend on earlier rows (core-event SET/CLEAR pairing, times relative
        to the first row): earlier events that changed are sent as
        ``updates``. Only new and changed events are augmented, unless the
        type has a whole-log augmenter (see ``_extend``)."""
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
            types = self._current_types()
            is_new_type = type_id not in types
            changed_header = header is not None
            if header is None:
                header = self._header(type_id)
            slots = sorted({*types, type_id})
            previous = self._section(log_type, types, slots)

            # Normalize first so a bad batch or header is rejected before
            # anything is stored.
            if is_new_type or changed_header:
                # The viewer re-fetches the page, so derive the type in full.
                section = self._derive(
                    log_type, header, previous["rows"] + rows, slots
                )
                if changed_header:
                    type_config(log_type, {**header, "events": section["rows"]})
                added, updates = [], []
            elif rows:
                section, added, updates = self._extend(
                    log_type, header, previous, rows, slots
                )
            else:
                return log or self._start(None)

            log = log or self._start(None)
            if changed_header:
                log.set_header(type_id, header)
                self._headers[type_id] = header
            if is_new_type:
                self._types = sorted({*self._current_types(), type_id})
                # A new type moves every type's id slot.
                self._sections.clear()
            self._sections[type_id] = section
            if not rows:
                self._broadcast({"type": "reload"})
                return log
            log.append(type_id, rows)
            self._seq += 1
            if is_new_type or changed_header:
                # The viewer needs this type's config.
                message = {"type": "reload", "seq": self._seq}
            else:
                message = {
                    "type": "events",
                    "seq": self._seq,
                    "events": added,
                    "updates": updates,
                }
                tracks = log_type.tracks({**header, "events": rows})
                if tracks:
                    message["tracks"] = tracks
            self._backlog.append(message)
            self._broadcast(message)
            return log

    def _derive(self, log_type, header, rows, slots) -> dict[str, Any]:
        """A type's section from all of its rows: ``rows``, ``derived`` (the
        type's own events, before augmenters), and the finished ``events``
        with their ``instants``."""
        payload = {**(header or {}), "events": rows}
        derived = derive_events(
            log_type,
            payload,
            row_id_base=slots.index(log_type.id),
            row_id_step=len(slots),
        )
        events, instants = finish_events(
            log_type, derived, payload=payload, mode="live"
        )
        return {
            "rows": rows,
            "derived": derived,
            "events": events,
            "instants": instants,
        }

    def _extend(self, log_type, header, previous, rows, slots):
        """``previous`` plus ``rows``: ``(section, new events, updated
        events)``. The new section shares (and extends) previous's lists, so
        call this only when the post will be stored."""
        all_rows = previous["rows"] + rows
        if log_type.has_whole_log_augmenters:
            section = self._derive(log_type, header, all_rows, slots)
            old, events = previous["events"], section["events"]
            updates = [new for prior, new in zip(old, events) if prior != new]
            return section, events[len(old) :], updates

        payload = {**(header or {}), "events": all_rows}
        new_derived, changed, stream = self._derive_increment(
            log_type, header, previous, rows, slots
        )
        events, instants = finish_events(
            log_type, changed + new_derived, payload=payload, mode="live"
        )
        updates = events[: len(changed)]
        added = events[len(changed) :]

        derived = previous["derived"]
        finished = previous["events"]
        times = previous["instants"]
        for event, instant, source in zip(updates, instants, changed):
            index = _row_index(source)
            derived[index] = source
            finished[index] = event
            times[index] = instant
        derived.extend(new_derived)
        finished.extend(added)
        times.extend(instants[len(changed) :])
        section = {
            "rows": all_rows,
            "derived": derived,
            "events": finished,
            "instants": times,
            "stream": stream,
        }
        return section, added, updates

    def _derive_increment(self, log_type, header, previous, rows, slots):
        """``(new derived events, earlier derived events that changed, the
        type's live stream)`` when ``rows`` are added to ``previous``.

        Three ways, cheapest first: a ``rows_independent`` type derives just
        ``rows``; a type with a ``live_stream`` appends them to it; any other
        type (or a stream that raises NeedsRebuild) re-derives every row and
        compares."""
        slot, step = slots.index(log_type.id), len(slots)
        count = len(previous["derived"])
        if log_type.rows_independent:
            # Only the new rows; their ids and keys continue from the stored
            # ones, and nothing earlier can change.
            new = derive_events(
                log_type,
                {**(header or {}), "events": rows},
                row_id_base=slot + count * step,
                row_id_step=step,
                row_key_offset=count,
            )
            return new, [], None

        stream = previous.get("stream") or self._replayed_stream(
            log_type, header, previous["rows"], slot, step
        )
        if stream is not None:
            try:
                new, updated = stream.append(rows)
            except NeedsRebuild as exc:
                # Re-derive below; the next post builds a fresh stream.
                logger.warning("%s: re-deriving the whole log: %s", log_type.id, exc)
            else:
                changed = [
                    dict(event, row_key=f"{log_type.id}:{_index_of(event, slot, step)}")
                    for event in updated
                ]
                return keyed_events(log_type, new, count), changed, stream

        derived = derive_events(
            log_type,
            {**(header or {}), "events": previous["rows"] + rows},
            row_id_base=slot,
            row_id_step=step,
        )
        old = previous["derived"]
        changed = [new for prior, new in zip(old, derived) if prior != new]
        return derived[len(old) :], changed, None

    def _replayed_stream(self, log_type, header, rows, slot, step):
        """The type's live stream with ``rows`` (those already stored)
        appended, or None if it has none."""
        stream = log_type.live_stream(
            dict(header or {}), row_id_base=slot, row_id_step=step
        )
        if stream is None:
            return None
        try:
            stream.append(rows)  # one batch: the stream sorts it like a page load
        except NeedsRebuild as exc:
            logger.warning("%s: no live stream: %s", log_type.id, exc)
            return None
        return stream

    def _section(self, log_type, types, slots) -> dict[str, Any]:
        """The current log's section for one type (see ``_derive``), kept in
        memory."""
        if self._current is None or log_type.id not in types:
            return {"rows": [], "derived": [], "events": [], "instants": []}
        cached = self._sections.get(log_type.id)
        if cached is None:
            rows = self._current.rows(log_type.id)
            header = self._header(log_type.id)
            cached = self._sections[log_type.id] = self._derive(
                log_type, header, rows, slots
            )
        return cached

    def page_data(self, log: LogDir) -> dict[str, Any]:
        """Assemble a log for the viewer; for the current log, include the
        stream position so the viewer can pick up exactly where it left off.

        The current log is assembled from what the hub keeps in memory, so a
        page load does not re-read and re-normalize it."""
        with self._lock:
            if self._current is None or log.id != self._current.id:
                return assemble_page_data(
                    log_dir_sections(log, self.registry), self.registry
                )
            types = self._current_types()
            sections, normalized = [], []
            for type_id in types:
                log_type = self.registry.get(type_id)
                if log_type is None:  # the same error as a log read from disk
                    return assemble_page_data(
                        log_dir_sections(log, self.registry), self.registry
                    )
                section = self._section(log_type, types, types)
                sections.append(
                    (log_type, {**self._header(type_id), "events": section["rows"]})
                )
                normalized.append((section["events"], section["instants"]))
            page = assemble_page_data(sections, self.registry, normalized=normalized)
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

    def close(self) -> None:
        """End every open event stream (``None`` tells a stream to stop), so
        server threads holding them finish when the server shuts down."""
        with self._lock:
            subscribers = list(self._subscribers)
        for subscriber in subscribers:
            with subscriber.mutex:
                subscriber.queue.clear()
            subscriber.put_nowait(None)

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


def _row_index(event: dict[str, Any]) -> int:
    """An event's position in its type's output, from its row_key."""
    return int(event["row_key"].rsplit(":", 1)[1])


def _index_of(event: dict[str, Any], slot: int, step: int) -> int:
    """An event's position in its type's output, from its row_id."""
    return (event["row_id"] - slot - 1) // step
