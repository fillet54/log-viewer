from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import json


@dataclass
class SessionMeta:
    id: str
    started_at: datetime
    ended_at: datetime | None
    channels: list[str]
    event_count: int
    status: str  # "active" | "completed" | "error"
    plugin_id: str

    @property
    def duration_seconds(self) -> float | None:
        if self.ended_at is None:
            return None
        return (self.ended_at - self.started_at).total_seconds()

    @property
    def duration_label(self) -> str:
        s = self.duration_seconds
        if s is None:
            return "ongoing"
        if s < 60:
            return f"{s:.0f}s"
        m, sec = divmod(int(s), 60)
        return f"{m}m {sec}s"


def _dt_from_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def read_meta(path: Path) -> SessionMeta | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        started_at = _dt_from_iso(data.get("started_at")) or datetime.fromtimestamp(0, tz=timezone.utc)
        return SessionMeta(
            id=str(data["id"]),
            started_at=started_at,
            ended_at=_dt_from_iso(data.get("ended_at")),
            channels=list(data.get("channels") or []),
            event_count=int(data.get("event_count") or 0),
            status=str(data.get("status") or "completed"),
            plugin_id=str(data.get("plugin_id") or ""),
        )
    except Exception:
        return None


def write_meta(path: Path, meta: SessionMeta) -> None:
    path.write_text(
        json.dumps(
            {
                "id": meta.id,
                "started_at": meta.started_at.isoformat(),
                "ended_at": meta.ended_at.isoformat() if meta.ended_at else None,
                "channels": meta.channels,
                "event_count": meta.event_count,
                "status": meta.status,
                "plugin_id": meta.plugin_id,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def read_events(path: Path) -> list[dict]:
    if not path.exists():
        return []
    events = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return events


class SessionStore:
    def __init__(self, sessions_dir: Path):
        self._dir = sessions_dir

    def list_sessions(self) -> list[SessionMeta]:
        if not self._dir.exists():
            return []
        sessions = []
        for meta_path in self._dir.glob("*/meta.json"):
            meta = read_meta(meta_path)
            if meta:
                sessions.append(meta)
        return sorted(sessions, key=lambda s: s.started_at, reverse=True)

    def get_meta(self, session_id: str) -> SessionMeta | None:
        return read_meta(self._dir / session_id / "meta.json")

    def get_events(self, session_id: str) -> list[dict]:
        return read_events(self._dir / session_id / "events.jsonl")

    def create_session(self, session_id: str, meta: SessionMeta) -> None:
        session_dir = self._dir / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        write_meta(session_dir / "meta.json", meta)

    def update_meta(self, meta: SessionMeta) -> None:
        write_meta(self._dir / meta.id / "meta.json", meta)

    def append_events(self, session_id: str, events: list[dict]) -> None:
        events_file = self._dir / session_id / "events.jsonl"
        with events_file.open("a", encoding="utf-8") as f:
            for event in events:
                f.write(json.dumps(event) + "\n")
