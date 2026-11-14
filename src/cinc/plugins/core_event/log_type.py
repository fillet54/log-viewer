from cinc.core.log_type import LogType, row_id

from . import config
from .plugin import build_log_data


def _validate_raw_payload(payload, type_id="core_event"):
    if not isinstance(payload, dict) or not isinstance(payload.get("events"), list):
        raise ValueError(f"{type_id}: raw payload must contain an events list")
    declared = set(payload.get("channels") or [])
    for index, event in enumerate(payload["events"]):
        if not isinstance(event, dict):
            raise ValueError(f"{type_id}: raw event {index} is not an object")
        channels = event.get("channels")
        if not isinstance(channels, list) or not channels:
            raise ValueError(f"{type_id}: raw event {index} requires channels")
        if declared and not set(channels) <= declared:
            raise ValueError(f"{type_id}: raw event {index} has an unknown channel")


class CoreEventLogType(LogType):
    """Timestamped system events with SET/CLEAR pairing across channels.

    The header can set ``channels``, ``severities`` and ``series`` (see
    ``config``). To reuse the rows and charts under another id, subclass it
    and set ``id``, ``name`` and ``defaults``, header values used when a log's
    header leaves them out::

        class PumpEvents(CoreEventLogType):
            id = "pump_event"
            name = "Pump Events"
            defaults = {"severities": [{"name": "OK", "tone": "green"},
                                       {"name": "Alarm", "tone": "red"}]}
    """

    defaults: dict = {}

    id = "core_event"
    name = "Core Event"
    description = "Timestamped system events and fault transitions."
    script_paths = ("static/row.js", "static/charts.js")
    style_paths = ("static/row.css",)
    row_height = 36

    def with_defaults(self, payload):
        """The payload with ``defaults`` filled in under its own header."""
        return {**self.defaults, **(payload or {})}

    def normalize_events(self, payload, *, row_id_base=0, row_id_step=1):
        payload = self.with_defaults(payload)
        _validate_raw_payload(payload, self.id)
        data = build_log_data(payload)[0]
        events = data["events"]
        row_id_map = {}
        for index, event in enumerate(events):
            new_id = row_id(index, row_id_base, row_id_step)
            row_id_map[event["row_id"]] = new_id
            event["row_id"] = new_id
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

    def view_config(self, payload):
        payload = self.with_defaults(payload)
        return {
            "channels": build_log_data(payload)[1],
            "severities": config.severities(payload),
            "series": config.series(payload),
        }

    def search_config(self, payload):
        return {
            "excludeFromBareTerms": ["data"],
            "fields": ["time", "name", "system", "color"],
        }
