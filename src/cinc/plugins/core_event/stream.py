"""core_event in live mode: rows normalized batch by batch.

The result is what ``CoreEventLogType.normalize_events`` gives for every row
so far (``cinc.testing.assert_stream_matches`` checks this): each new row is
normalized against the channel catalog and start time fixed by the rows
before it, and a CLEAR pairs with the oldest open SET on each of its
channels, which is returned as an updated event.
"""

from __future__ import annotations

from collections import deque
from typing import Any

from cinc.core.log_type import NeedsRebuild, row_id

from .config import severities as severity_scale
from .plugin import (
    _build_pair_key,
    _finish_event,
    _normalize_channel_name,
    _normalize_event,
    _order_key,
    _pair_links,
    _resolve_channel_catalog,
    _resolve_fallback_start,
)


class CoreEventStream:
    def __init__(
        self,
        header: dict[str, Any],
        *,
        type_id: str,
        validate,
        row_id_base: int = 0,
        row_id_step: int = 1,
    ):
        self._header = {k: v for k, v in (header or {}).items() if k != "events"}
        self._type_id = type_id
        self._validate = validate
        self._base = row_id_base
        self._step = row_id_step
        self._scale = severity_scale(self._header)
        self._channels: list[str] | None = None  # set by the first rows
        self._fallback_start = None
        # Every event so far, by position; open SETs are queued by position.
        self._events: list[dict[str, Any]] = []
        self._open_sets: dict[str, deque[int]] = {}
        self._last_key: tuple[float, int] | None = None

    def append(self, rows: list[dict[str, Any]]):
        """``(new events, updated earlier events)`` for ``rows``."""
        rows = list(rows)
        self._validate({**self._header, "events": rows}, self._type_id)
        if not rows:
            return [], []
        count = len(self._events)

        # Work out everything first: a ValueError or NeedsRebuild must leave
        # the stream as it was.
        if count == 0:
            channels = _resolve_channel_catalog(self._header, rows)
            fallback_start = _resolve_fallback_start(rows)
        else:
            channels = self._channels
            for row in rows:
                for item in row.get("channels") or []:
                    channel = _normalize_channel_name(item)
                    if channel and channel not in channels:
                        raise NeedsRebuild(
                            f"new channel {channel!r} and the header declares none"
                        )
            fallback_start = self._fallback_start
            if fallback_start is None and _resolve_fallback_start(rows) is not None:
                raise NeedsRebuild(
                    "the first row with a utctime came after rows without one"
                )

        new = []
        for offset, raw in enumerate(rows):
            index = count + offset
            event = _normalize_event(
                raw, index, fallback_start, channels, self._scale
            )
            # The row's own id orders ties, as in the batch path; the page id
            # replaces it (normalize_events remaps it the same way).
            key = _order_key(event)
            event["row_id"] = row_id(index, self._base, self._step)
            new.append((key, index, event))
        new.sort(key=lambda item: (item[0], item[1]))
        if self._last_key is not None and new[0][0] < self._last_key:
            raise NeedsRebuild("a row is earlier than the last row processed")

        # Apply.
        self._channels = channels
        self._fallback_start = fallback_start
        self._events.extend(event for _, _, event in sorted(new, key=lambda n: n[1]))
        self._last_key = new[-1][0]
        updated: set[int] = set()
        for _, index, event in new:
            for channel in event.get("channels", []):
                queue = self._open_sets.setdefault(
                    _build_pair_key(event, channel), deque()
                )
                if event.get("set_clear") == "set":
                    queue.append(index)
                    continue
                if not queue:
                    continue
                set_index = queue.popleft()
                set_event = self._events[set_index]
                forward, back = _pair_links(set_event, event, channel)
                if set_index < count:
                    # Copy, so events already handed out stay as they were.
                    set_event = self._events[set_index] = {
                        **set_event,
                        "pairedChannels": {
                            **set_event["pairedChannels"],
                            channel: forward,
                        },
                    }
                    updated.add(set_index)
                else:
                    set_event["pairedChannels"][channel] = forward
                event["pairedChannels"][channel] = back

        for index in [*sorted(updated), *range(count, len(self._events))]:
            _finish_event(self._events[index])
        return (
            self._events[count:],
            [self._events[index] for index in sorted(updated)],
        )
