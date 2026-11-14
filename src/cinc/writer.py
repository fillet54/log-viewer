"""Writing logs from Python: log folders on disk and posts to a live server.

A log folder is what ``cinc serve`` stores and ``cinc build`` reads::

    from cinc import LogWriter
    from cinc.plugins.core_event import event

    with LogWriter("cinc-data", name="Run 42") as log:
        log.header("core_event", channels=["A", "B"])
        log.write("core_event", event(t0, "Pump hot", system="Thermal", channels=["A"]))
        log.write("track", [{"time": t0, "track": "UAV-1", "lat": 34.1, "lon": -117.2}])
    print(log.path)   # pass to `cinc build`, or keep under `cinc serve --data`

``LiveClient`` sends the same rows to a running ``cinc serve`` instead.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from .logdir import LogDir
from .core.times import iso_time


def _rows(rows: dict[str, Any] | Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """A row or rows, with datetime values turned into ISO times."""
    items = [rows] if isinstance(rows, dict) else list(rows)
    result = []
    for row in items:
        if not isinstance(row, dict):
            raise ValueError("rows must be JSON objects (dicts)")
        result.append(
            {
                key: iso_time(value) if isinstance(value, datetime) else value
                for key, value in row.items()
            }
        )
    return result


class LogWriter:
    """Creates a log folder under ``root`` and appends rows to it.

    Rows are appended as they are written, so ``cinc serve`` shows the log in
    its history while it grows. ``log_id`` defaults to a timestamped id."""

    def __init__(
        self,
        root: Path | str,
        name: str | None = None,
        *,
        log_id: str | None = None,
        started: datetime | None = None,
    ):
        self._log = LogDir.create(Path(root), name, log_id=log_id, started=started)

    @property
    def path(self) -> Path:
        return self._log.path

    @property
    def id(self) -> str:
        return self._log.id

    def header(self, type_id: str, header: dict[str, Any] | None = None, **fields):
        """Set (merge into) a type's header, e.g. ``channels`` or ``tracks``."""
        merged = {**self._log.header(type_id), **(header or {}), **fields}
        self._log.set_header(type_id, merged)
        return self

    def write(self, type_id: str, rows: dict[str, Any] | Iterable[dict[str, Any]]):
        """Append one row or several rows of a log type."""
        self._log.append(type_id, _rows(rows))
        return self

    def __enter__(self) -> "LogWriter":
        return self

    def __exit__(self, *exc_info) -> None:
        return None


class LiveClient:
    """Posts rows to a running ``cinc serve`` (its live API)."""

    def __init__(self, base_url: str = "http://127.0.0.1:8080", timeout: float = 10):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def start(self, name: str | None = None) -> dict[str, Any]:
        """Start a new log and make it the live one; returns {"id", "url"}."""
        return self._post("/api/logs", {"name": name} if name else {})

    def post(
        self,
        type_id: str,
        rows: dict[str, Any] | Iterable[dict[str, Any]] = (),
        *,
        header: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Append rows (and optionally set the type's header) on the live log."""
        body: dict[str, Any] = {"type": type_id, "events": _rows(rows)}
        if header is not None:
            body["header"] = header
        return self._post("/api/events", body)

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        request = urllib.request.Request(
            self.base_url + path,
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")
            raise ValueError(f"{path}: {exc.code} {detail}") from None
