from __future__ import annotations

from cinc.core.log_type import LogType, row_id


class TextLogType(LogType):
    id = "text_log"
    name = "Text Log"
    description = "Plain timestamped application log lines"
    script_paths = ("static/row.js", "static/charts.js")
    style_paths = ("static/row.css",)
    row_height = 36
    rows_independent = True

    def normalize_events(self, payload, *, row_id_base=0, row_id_step=1):
        source = payload.get("lines", payload.get("events", []))
        result = []
        for index, raw in enumerate(source):
            event = dict(raw)
            event["row_id"] = row_id(index, row_id_base, row_id_step)
            event["time"] = event.pop("ts", event.get("time", ""))
            if "fields" in event:
                event["data"] = event.pop("fields")
            result.append(event)
        return result

    def detect(self, payload):
        lines = payload.get("lines") if isinstance(payload, dict) else None
        if not isinstance(lines, list) or not lines or not isinstance(lines[0], dict):
            return 0.0
        return 0.9 if {"ts", "level", "message"} <= set(lines[0]) else 0.0

    def search_config(self, payload):
        return {
            "excludeFromBareTerms": ["data"],
            "fields": ["time", "level", "logger", "message"],
        }

    def view_config(self, payload):
        return {"levels": ["DEBUG", "INFO", "WARN", "ERROR"]}

    def log_summary(self, payload):
        lines = payload.get("lines", payload.get("events", []))
        return {
            "Source": str(payload.get("source") or "—"),
            "Host": str(payload.get("host") or "—"),
            "Lines": str(len(lines)),
            "Errors": str(sum(1 for line in lines if line.get("level") == "ERROR")),
        }
