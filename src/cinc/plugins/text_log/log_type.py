from __future__ import annotations

from cinc.core.log_type import LogType


class TextLogType(LogType):
    id = "text_log"
    name = "Text Log"
    description = "Plain timestamped application log lines"
    script_paths = ("static/row.js", "static/charts.js")
    style_paths = ("static/row.css",)

    def normalize_events(self, payload, *, row_id_base=0):
        source = payload.get("lines", payload.get("events", []))
        result = []
        for row_id, raw in enumerate(source, row_id_base + 1):
            event = dict(raw)
            event["row_id"] = row_id
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
            "labelField": "message",
            "excludeFromBareTerms": ["data"],
            "aliasSuffix": None,
            "fields": ["time", "level", "logger", "message"],
            "examples": [
                "level:ERROR",
                "logger~db",
                "message~refused",
                "NOT level:DEBUG",
            ],
        }

    def view_config(self, payload):
        return {
            "rowSettings": {"levels": ["DEBUG", "INFO", "WARN", "ERROR"]},
            "rowHeightHint": 36,
            "charts": [],
            "timelineViews": ["levels"],
        }

    def chart_config(self, payload):
        return {
            "severity": {
                "field": "level",
                "mapping": {
                    "DEBUG": "green",
                    "INFO": "green",
                    "WARN": "yellow",
                    "ERROR": "red",
                    "FATAL": "flashing-red",
                    "CRITICAL": "flashing-red",
                },
            }
        }

    def log_summary(self, payload):
        lines = payload.get("lines", payload.get("events", []))
        return {
            "Source": str(payload.get("source") or "—"),
            "Host": str(payload.get("host") or "—"),
            "Lines": str(len(lines)),
            "Errors": str(sum(1 for line in lines if line.get("level") == "ERROR")),
        }
