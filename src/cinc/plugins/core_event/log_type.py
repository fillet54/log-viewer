from cinc.logs.types import LogType
from cinc.live.replay import SampleReplaySource
from importlib.resources import files
from .plugin import CoreEventPlugin


def _validate_raw_payload(payload):
    if not isinstance(payload, dict) or not isinstance(payload.get("events"), list):
        raise ValueError("core_event: raw payload must contain an events list")
    declared = set(payload.get("channels") or [])
    for index, event in enumerate(payload["events"]):
        if not isinstance(event, dict):
            raise ValueError(f"core_event: raw event {index} is not an object")
        channels = event.get("channels")
        if not isinstance(channels, list) or not channels:
            raise ValueError(f"core_event: raw event {index} requires channels")
        if declared and not set(channels) <= declared:
            raise ValueError(f"core_event: raw event {index} has an unknown channel")


class CoreEventLogType(LogType):
    id = "core_event"
    name = "Core Event"
    description = "Timestamped system events and fault transitions."
    script_paths = ("static/row.js", "static/charts.js")
    sample_paths = ("samples/sample-3ch.json", "samples/sample-4ch.json")
    style_paths = ("static/row.css",)

    def __init__(self):
        self._impl = CoreEventPlugin()

    def parse_import(self, *, file=None, json_data=None):
        payload = json_data if json_data is not None else __import__("json").load(file)
        return ("import", payload)

    def normalize_events(self, payload, *, row_id_base=0):
        _validate_raw_payload(payload)
        data = self._impl._build_log_data(payload)[0]
        events = data["events"]
        row_id_map = {}
        for i, event in enumerate(events, row_id_base + 1):
            row_id_map[event["row_id"]] = i
            event["row_id"] = i
        for event in events:
            for match in event.get("pairedChannels", {}).values():
                match["linkedRowId"] = row_id_map.get(
                    match.get("linkedRowId"), match.get("linkedRowId")
                )
            for match in event.get("matchSummary", {}).get("items", []):
                match["linkedRowId"] = row_id_map.get(
                    match.get("linkedRowId"), match.get("linkedRowId")
                )
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

    def create_live_source(self):
        return SampleReplaySource(
            files(__package__).joinpath("samples/sample-4ch.json"), speed=20.0
        )

    def view_config(self, payload):
        channels = self._impl._build_log_data(payload)[1]
        return {
            "rowSettings": {"channels": channels},
            "rowHeightHint": 36,
            "charts": ["systems"],
            "timelineViews": ["severity", "bus-load"],
        }

    def chart_config(self, payload):
        return {
            "severity": {
                "field": "color",
                "mapping": {
                    "Green": "green",
                    "Yellow": "yellow",
                    "Red": "red",
                    "Flashing Red": "flashing-red",
                },
            },
            "series": [
                {
                    "id": "bus-load",
                    "label": "Bus Load",
                    "kind": "line",
                    "field": "data.bus.load_pct",
                    "unit": "%",
                }
            ],
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
