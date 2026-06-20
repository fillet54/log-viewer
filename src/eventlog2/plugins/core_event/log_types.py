from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from ...live.storage import SessionStore
from ...logs.types import LogRecord, LogTypeDefinition

_LIVE_PREFIX = "live:"


class CoreEventBootLogType(LogTypeDefinition):
    id = "boot-log"
    name = "Boot Log"
    description = "Core event log captured from a system boot sequence"

    def __init__(
        self,
        plugin_id: str,
        build_page_data: Callable[[dict[str, Any]], dict[str, Any]],
        session_store: SessionStore | None = None,
    ) -> None:
        super().__init__(plugin_id)
        self._build_page_data = build_page_data
        self._session_store = session_store

    def parse_import(
        self,
        *,
        file: Any = None,
        json_data: dict[str, Any] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        if file and getattr(file, "filename", None):
            try:
                raw = file.read()
                data = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise ValueError(f"Invalid JSON file: {exc}") from exc
            name = Path(file.filename).stem
        elif json_data is not None:
            data = json_data
            name = "Imported Boot Log"
        else:
            raise ValueError("Provide a JSON file or a JSON request body.")

        if not isinstance(data.get("events"), list):
            raise ValueError("Invalid format: expected an object with an 'events' array.")

        return name, data

    def extract_metadata(self, payload: dict[str, Any]) -> dict[str, Any]:
        events = payload.get("events") or []
        return {
            "event_count": len(events),
            "hours": float(payload.get("hours") or 0),
            "channels": list(payload.get("channels") or []),
        }

    def get_list_columns(self) -> list[dict[str, str]]:
        return [
            {"key": "event_count", "label": "Events"},
            {"key": "duration", "label": "Duration"},
            {"key": "channels", "label": "Channels"},
        ]

    def format_list_row(self, record: LogRecord) -> dict[str, str]:
        m = record.metadata
        hours = float(m.get("hours") or 0)
        if hours == 0:
            dur = "—"
        elif hours < 1 / 60:
            dur = f"{int(hours * 3600)}s"
        elif hours < 1:
            dur = f"{int(hours * 60)}m"
        else:
            dur = f"{hours:.1f}h"
        channels = m.get("channels") or []
        return {
            "event_count": f"{m.get('event_count', 0):,}",
            "duration": dur,
            "channels": ", ".join(channels) if channels else "—",
        }

    def build_view_page_data(
        self, record: LogRecord, payload: dict[str, Any]
    ) -> dict[str, Any]:
        return self._build_page_data(payload)

    def extra_records(self, search: str = "") -> list[LogRecord]:
        if not self._session_store:
            return []
        sessions = self._session_store.list_sessions()
        records = []
        lo = search.lower()
        for s in sessions:
            # Skip active sessions — they aren't complete captures yet
            if s.status == "active":
                continue
            name = f"Live capture {s.started_at.strftime('%Y-%m-%d %H:%M')}"
            if lo and lo not in name.lower():
                continue
            hours = 0.0
            if s.duration_seconds is not None:
                hours = s.duration_seconds / 3600
            records.append(LogRecord(
                id=f"{_LIVE_PREFIX}{s.id}",
                log_type_id=self.full_id,
                plugin_id=self.plugin_id,
                name=name,
                imported_at=s.started_at,
                metadata={
                    "event_count": s.event_count,
                    "hours": hours,
                    "channels": s.channels,
                    "source": "live",
                },
            ))
        return records

    def get_extra_payload(self, record_id: str) -> dict[str, Any] | None:
        if not self._session_store or not record_id.startswith(_LIVE_PREFIX):
            return None
        session_id = record_id[len(_LIVE_PREFIX):]
        meta = self._session_store.get_meta(session_id)
        if not meta:
            return None
        events = self._session_store.get_events(session_id)
        return {
            "events": events,
            "started_at": meta.started_at.isoformat(),
            "ended_at": meta.ended_at.isoformat() if meta.ended_at else None,
            "channels": meta.channels,
            "hours": meta.duration_seconds / 3600 if meta.duration_seconds else 0,
        }
