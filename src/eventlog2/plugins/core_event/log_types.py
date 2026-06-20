from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from ...logs.types import LogRecord, LogTypeDefinition


class CoreEventBootLogType(LogTypeDefinition):
    id = "core_event"
    name = "Core Event"
    description = "Core event log captured from a system boot sequence"

    def __init__(
        self,
        plugin_id: str,
        build_page_data: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> None:
        super().__init__(plugin_id)
        self._build_page_data = build_page_data

    @property
    def full_id(self) -> str:
        return self.id

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
        header = {key: value for key, value in payload.items() if key != "events"}
        return {
            "event_count": len(events),
            "hours": float(payload.get("hours") or 0),
            "channels": list(payload.get("channels") or []),
            "payload_header": header,
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

    def normalize_events(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        page_data = self._build_page_data(payload)
        events = page_data.get("logData", {}).get("events") or []
        normalized = []
        for event in events:
            if not isinstance(event, dict):
                continue
            entry = dict(event)
            entry.setdefault("time", entry.get("utctime"))
            normalized.append(entry)
        return normalized

    def build_payload_from_events(self, record: LogRecord, events: list[dict[str, Any]]) -> dict[str, Any]:
        payload = dict(record.metadata.get("payload_header") or {})
        payload["events"] = events
        channels = record.metadata.get("channels")
        if channels and "channels" not in payload:
            payload["channels"] = channels
        if record.started_at and "start" not in payload:
            payload["start"] = record.started_at.isoformat()
        if record.ended_at and "end" not in payload:
            payload["end"] = record.ended_at.isoformat()
        return payload

    def build_view_page_data(
        self, record: LogRecord, payload: dict[str, Any]
    ) -> dict[str, Any]:
        page_data = self._build_page_data(payload)
        page_data["logType"] = {"id": self.full_id, "name": self.name}
        page_data["apiVersion"] = 1
        page_data["search"] = {
            "fields": ["time", "utctime", "name", "system", "subsystem", "unit", "code", "color", "set_clear"],
            "examples": ["color:Red", "system:Power", "set_clear:set", "data.$.*~voltage"],
        }
        page_data["view"].setdefault("charts", self.get_supported_charts()["charts"])
        page_data["view"].setdefault("timelineViews", self.get_supported_charts()["timelineViews"])
        return page_data

    def get_supported_charts(self) -> dict[str, list[str]]:
        return {
            "charts": ["systems"],
            "timelineViews": ["severity", "bus-load"],
        }
