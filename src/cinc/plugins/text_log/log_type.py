from datetime import datetime, timezone
from cinc.logs.types import LogType


class TextLogType(LogType):
    id = "text_log"
    name = "Text Log"
    description = "Plain timestamped text lines."
    sample_paths = ("samples/sample.json",)

    def parse_import(self, *, file=None, json_data=None):
        if json_data is not None:
            return ("import", json_data)
        raw = file.read()
        text = raw.decode() if isinstance(raw, bytes) else raw
        rows = []
        for i, line in enumerate(text.splitlines(), 1):
            rows.append(
                {
                    "time": datetime.now(timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z"),
                    "message": line,
                    "level": "INFO",
                    "row_id": i,
                }
            )
        return ("text log", {"events": rows})

    def normalize_events(self, payload, *, row_id_base=0):
        result = []
        source = payload.get("events", payload.get("lines", []))
        for i, raw in enumerate(source, row_id_base + 1):
            event = dict(raw)
            event["row_id"] = i
            event.setdefault("time", "1970-01-01T00:00:00Z")
            result.append(event)
        return result

    def detect(self, payload):
        lines = payload.get("lines") if isinstance(payload, dict) else None
        if not isinstance(lines, list) or not lines or not isinstance(lines[0], dict):
            return 0.0
        return 0.9 if {"ts", "level", "message"} <= set(lines[0]) else 0.0

    def search_config(self, payload):
        return {
            "labelField": "message",
            "fields": ["time", "level", "message"],
        }

    def view_config(self, payload):
        return {"rowSettings": {}, "charts": [], "timelineViews": []}

    def log_summary(self, payload):
        return {"Events": str(len(payload.get("events", [])))}
