"""On-disk log format.

A log is a plain directory that any tool can write::

    <root>/<log-id>/
        log.json        {"name": "...", "started": "<ISO time>"}   (optional)
        <type>.jsonl    raw rows for log type <type>, one JSON object per line
        <type>.json     header for <type>, e.g. {"channels": [...]}  (optional)

The payload handed to a log type is ``{**header, "events": rows}``.
"""

from __future__ import annotations

import json
import re
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .core.times import parse_time

_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def valid_id(value: str) -> bool:
    return bool(_ID.fullmatch(value or ""))


def new_log_id(now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    return f"{now:%Y%m%d-%H%M%S}-{secrets.token_hex(2)}"


class LogDir:
    def __init__(self, path: Path):
        self.path = Path(path)

    @property
    def id(self) -> str:
        return self.path.name

    @classmethod
    def create(
        cls,
        root: Path,
        name: str | None = None,
        *,
        log_id: str | None = None,
        started: datetime | None = None,
    ) -> "LogDir":
        now = datetime.now(timezone.utc).replace(microsecond=0)
        if log_id is not None and not valid_id(log_id):
            raise ValueError(f"invalid log id {log_id!r}")
        log = cls(Path(root) / (log_id or new_log_id(now)))
        log.path.mkdir(parents=True)
        started = started or now
        meta = {"name": name or f"Log {started:%Y-%m-%d %H:%M:%S}", "started": started}
        log._write_json("log.json", meta)
        return log

    def meta(self) -> dict[str, Any]:
        """``log.json`` with ``name`` and ``started`` filled in (folder name,
        folder mtime). An unreadable ``log.json`` gives just those, plus
        ``"error"`` saying why, so one bad folder does not break the rest."""
        try:
            meta = self._read_json("log.json")
        except (ValueError, OSError) as exc:
            meta = {"error": f"unreadable log.json: {exc}"}
        meta.setdefault("name", self.id)
        if not meta.get("started"):
            try:
                mtime = self.path.stat().st_mtime
            except OSError:
                mtime = 0
            meta["started"] = datetime.fromtimestamp(mtime, timezone.utc).isoformat()
        return meta

    def types(self) -> list[str]:
        return sorted(path.stem for path in self.path.glob("*.jsonl"))

    def header(self, type_id: str) -> dict[str, Any]:
        return self._read_json(f"{type_id}.json")

    def set_header(self, type_id: str, header: dict[str, Any]) -> None:
        self._write_json(f"{type_id}.json", header)

    def rows(self, type_id: str) -> list[dict[str, Any]]:
        path = self.path / f"{type_id}.jsonl"
        if not path.exists():
            return []
        text = path.read_text(encoding="utf-8")
        lines = text.split("\n")
        if not text.endswith("\n"):
            lines.pop()  # a writer may be mid-line; skip the unfinished row
        rows = []
        for number, line in enumerate(lines, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{number}: invalid JSON ({exc.msg})")
            if not isinstance(row, dict):
                raise ValueError(f"{path}:{number}: row must be a JSON object")
            rows.append(row)
        return rows

    def append(self, type_id: str, rows: list[dict[str, Any]]) -> None:
        with (self.path / f"{type_id}.jsonl").open("a", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, separators=(",", ":")) + "\n")

    def row_count(self) -> int:
        return sum(len(self.rows(type_id)) for type_id in self.types())

    def payloads(self) -> list[tuple[str, dict[str, Any]]]:
        return [
            (type_id, {**self.header(type_id), "events": self.rows(type_id)})
            for type_id in self.types()
        ]

    def _read_json(self, name: str) -> dict[str, Any]:
        path = self.path / name
        if not path.exists():
            return {}
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError(f"{path} must contain a JSON object")
        return value

    def _write_json(self, name: str, value: dict[str, Any]) -> None:
        text = json.dumps(value, indent=2, default=_iso)
        (self.path / name).write_text(text + "\n", encoding="utf-8")


def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Not JSON serializable: {value!r}")


def is_log_dir(path: Path) -> bool:
    path = Path(path)
    return path.is_dir() and ((path / "log.json").exists() or any(path.glob("*.jsonl")))


def list_logs(root: Path) -> list[LogDir]:
    """All logs under ``root``, newest first."""
    return [log for log, _ in list_logs_with_meta(root)]


def list_logs_with_meta(root: Path) -> list[tuple[LogDir, dict[str, Any]]]:
    """``(log, log.meta())`` for every log under ``root``, newest first; each
    meta is read once."""
    root = Path(root)
    if not root.is_dir():
        return []
    logs = [
        (log, log.meta())
        for log in (
            LogDir(path)
            for path in root.iterdir()
            if valid_id(path.name) and is_log_dir(path)
        )
    ]
    return sorted(logs, key=lambda item: _started(item[1]), reverse=True)


_NEVER = datetime.min.replace(tzinfo=timezone.utc)


def _started(meta: dict[str, Any]) -> datetime:
    try:
        return parse_time(meta["started"])
    except (KeyError, ValueError):
        return _NEVER
