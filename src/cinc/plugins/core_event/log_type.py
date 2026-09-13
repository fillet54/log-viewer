from cinc.logs.types import LogType
from .plugin import CoreEventPlugin


class CoreEventLogType(LogType):
    id = "core_event"
    name = "Core Event"
    description = "Timestamped system events and fault transitions."
    script_paths = ("static/row.js", "static/charts.js")
    sample_paths = ("samples/sample-3ch.json", "samples/sample-4ch.json")

    def __init__(self):
        self._impl = CoreEventPlugin()

    def parse_import(self, *, file=None, json_data=None):
        payload = json_data if json_data is not None else __import__("json").load(file)
        return ("import", payload)

    def normalize_events(self, payload, *, row_id_base=0):
        data = self._impl._build_log_data(payload)[0]
        events = data["events"]
        for i, event in enumerate(events, row_id_base + 1):
            event["row_id"] = i
        return events

    def detect(self, payload):
        events = payload.get("events") if isinstance(payload, dict) else None
        if (
            not isinstance(events, list)
            or not events
            or not isinstance(events[0], dict)
        ):
            return 0.0
        keys = set(events[0])
        if {"set_clear", "channels"} & keys:
            return 0.9
        return 0.7 if {"norm_time", "utctime"} <= keys else 0.0

    def view_config(self, payload):
        channels = self._impl._build_log_data(payload)[1]
        return {
            "rowSettings": {"channels": channels},
            "charts": ["systems"],
            "timelineViews": ["severity", "bus-load"],
        }

    def search_config(self, payload):
        return {
            "labelField": "name",
            "excludeFromBareTerms": ["data"],
            "aliasSuffix": "_search",
            "fields": ["time", "name", "system", "color"],
        }

    def log_summary(self, payload):
        return {}

    def inline_scripts(self):
        return self._impl.get_inline_scripts()

    def inline_styles(self):
        return []
