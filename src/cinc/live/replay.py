from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class SampleReplaySource:
    def __init__(
        self,
        sample_path: Path,
        *,
        events_key="events",
        time_key="time",
        speed=10.0,
        poll_interval=1.0,
    ):
        self.sample_path = Path(sample_path)
        self.events_key = events_key
        self.time_key = time_key
        self.speed = float(speed)
        self.poll_interval = float(poll_interval)
        self._events = []
        self._index = 0

    def start(self, session_id: str) -> dict[str, Any]:
        del session_id
        payload = json.loads(self.sample_path.read_text(encoding="utf-8"))
        self._events = list(payload.get(self.events_key, []))
        self._index = 0
        return {key: value for key, value in payload.items() if key != self.events_key}

    def poll(self) -> list[dict[str, Any]]:
        if self._index >= len(self._events):
            return []
        count = max(1, int(self.speed))
        rows = self._events[self._index : self._index + count]
        self._index += len(rows)
        return rows

    def finished(self) -> bool:
        return self._index >= len(self._events)

    def stop(self) -> None:
        return None
