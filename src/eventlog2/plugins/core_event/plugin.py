from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from ..base import EventLogSourcePlugin

DEFAULT_CORE_EVENT_CHANNELS = ["A", "B", "C", "D"]
CORE_EVENT_SEVERITY = ["Green", "Yellow", "Red", "Flashing Red"]
COLOR_PREFIX = {
    "green": "G",
    "yellow": "Y",
    "red": "R",
    "dark-red": "R",
    "flashing-red": "F",
}


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _isoformat_seconds(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_action(value: Any) -> str:
    action = str(value or "").strip().lower()
    return "clear" if action == "clear" else "set"


def _normalize_channel_name(value: Any) -> str:
    return str(value or "").strip().upper()


def _normalize_channel_count(value: Any) -> int | None:
    try:
        count = int(value)
    except (TypeError, ValueError):
        return None
    return count if count > 0 else None


def _build_default_channel_labels(count: int) -> list[str]:
    if count <= 0:
        return []
    if count <= 26:
        return [chr(ord("A") + index) for index in range(count)]
    return [str(index + 1) for index in range(count)]


def _coerce_string(value: Any) -> str:
    return str(value or "").strip()


def _unique_non_empty(values: list[Any]) -> list[str]:
    result: list[str] = []
    for value in values:
        text = _coerce_string(value)
        if text and text not in result:
            result.append(text)
    return result


def _normalize_entity_fields(raw_event: dict[str, Any], key: str) -> dict[str, Any]:
    raw_value = raw_event.get(key)
    raw_id = raw_event.get(f"{key}_id")
    raw_name = raw_event.get(f"{key}_name")

    value_id = ""
    value_name = ""

    if isinstance(raw_value, dict):
        value_id = _coerce_string(raw_value.get("id"))
        value_name = _coerce_string(raw_value.get("name") or raw_value.get("label"))
    elif raw_value is not None:
        if raw_name or raw_id:
            candidate = _coerce_string(raw_value)
            if candidate and candidate != _coerce_string(raw_id) and candidate != _coerce_string(raw_name):
                value_name = candidate
        else:
            value_name = _coerce_string(raw_value)

    value_id = _coerce_string(raw_id) or value_id
    value_name = _coerce_string(raw_name) or value_name
    display = value_name or value_id
    search_values = _unique_non_empty([display, value_id, value_name])

    return {
        key: display,
        f"{key}_id": value_id,
        f"{key}_name": value_name,
        f"{key}_search": search_values,
    }


def _infer_event_channels(event: dict[str, Any]) -> list[str]:
    inferred: list[str] = []
    for key, value in event.items():
        if value is None or key == "norm_time" or not isinstance(key, str) or not key.endswith("_time"):
            continue
        channel = _normalize_channel_name(key[:-5])
        if channel and channel not in inferred:
            inferred.append(channel)
    return inferred


def _resolve_channel_catalog(payload: dict[str, Any], raw_events: list[dict[str, Any]]) -> list[str]:
    configured = payload.get("channels")
    configured_count = _normalize_channel_count(payload.get("channelCount"))
    discovered: list[str] = []

    if isinstance(configured, list):
        for item in configured:
            channel = _normalize_channel_name(item)
            if channel and channel not in discovered:
                discovered.append(channel)

    if not discovered and configured_count is not None:
        discovered.extend(_build_default_channel_labels(configured_count))

    for event in raw_events:
        explicit = event.get("channels")
        source = explicit if isinstance(explicit, list) else _infer_event_channels(event)
        for item in source:
            channel = _normalize_channel_name(item)
            if channel and channel not in discovered:
                discovered.append(channel)
    if not discovered:
        return list(DEFAULT_CORE_EVENT_CHANNELS)
    return discovered


def _normalize_channel_list(event: dict[str, Any], available_channels: list[str]) -> list[str]:
    available = [_normalize_channel_name(channel) for channel in available_channels if _normalize_channel_name(channel)]
    available_set = set(available)
    explicit = event.get("channels")
    if isinstance(explicit, list):
        valid = []
        for item in explicit:
            channel = _normalize_channel_name(item)
            if channel in available_set and channel not in valid:
                valid.append(channel)
        if valid:
            return [channel for channel in available if channel in valid]

    inferred = _infer_event_channels(event)
    if not inferred:
        return []
    if not available:
        return inferred
    return [channel for channel in available if channel in inferred]


def _normalize_norm_time(value: Any, fallback: int | float = 0) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return float(fallback)
    return numeric


def _resolve_fallback_start(raw_events: list[dict[str, Any]]) -> datetime | None:
    for event in raw_events:
        parsed = _parse_datetime(event.get("utctime"))
        if parsed is None:
            continue
        seconds = _normalize_norm_time(event.get("norm_time"), 0)
        return parsed - timedelta(seconds=seconds)
    return None


def _build_pair_key(event: dict[str, Any], channel: str) -> str:
    return "|".join(
        [
            str(event.get("system") or "").strip(),
            str(event.get("subsystem") or "").strip(),
            str(event.get("unit") or "").strip(),
            str(event.get("code") or "").strip(),
            channel,
        ]
    )


def _format_duration_label(seconds: float) -> str:
    if seconds < 0:
        return ""
    return f"{round(seconds)}s"


def _has_event_data(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, list):
        return len(value) > 0
    if isinstance(value, dict):
        return len(value) > 0
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _build_fault_prefix(event: dict[str, Any]) -> str:
    raw_code = str(event.get("code") or "").strip()
    if not raw_code:
        return ""

    color_class = str(event.get("color") or "Green").lower().replace(" ", "-")
    severity = COLOR_PREFIX.get(color_class, "G")
    parts = [part for part in raw_code.split("-") if part]

    if len(parts) >= 2:
        return f"{parts[0]}-{severity}-{'-'.join(parts[1:])}"

    system_prefix = "".join(ch for ch in str(event.get("system") or "").strip().upper() if ch.isalnum())[:3]
    return f"{system_prefix}-{severity}-{raw_code}" if system_prefix else f"{severity}-{raw_code}"


def _build_row_display(event: dict[str, Any]) -> dict[str, Any]:
    norm_time = float(event.get("norm_time") or 0)
    location = "/".join(str(event.get(key) or "") for key in ("system", "subsystem", "unit")).strip("/")
    prefix = _build_fault_prefix(event)
    has_data = _has_event_data(event.get("data"))

    return {
        "utctime": str(event.get("utctime") or ""),
        "actionLabel": str(event.get("set_clear") or ""),
        "name": str(event.get("name") or ""),
        "prefix": f"[{prefix}]" if prefix else "",
        "offset": f"{norm_time:.3f}s",
        "description": str(event.get("description") or ""),
        "location": f"({location})" if location else "",
        "hasData": has_data,
        "dataLabel": "Event has data" if has_data else "No event data",
    }


def _summarize_matches(event: dict[str, Any], channel_matches: dict[str, Any]) -> dict[str, Any]:
    channels = event.get("channels") or []
    if not channels:
        return {"items": [], "collapsed": False}

    matches = [
        channel_matches[channel]
        for channel in channels
        if channel_matches.get(channel) and channel_matches[channel].get("linkedRowId") is not None
        and channel_matches[channel].get("durationSeconds") is not None
    ]
    if not matches:
        return {"items": [], "collapsed": False}

    reference = matches[0]
    all_channels_matched = len(matches) == len(channels) and all(
        item.get("linkedRowId") == reference.get("linkedRowId")
        and item.get("direction") == reference.get("direction")
        and item.get("label") == reference.get("label")
        for item in matches
    )

    if all_channels_matched and len(channels) > 1:
        return {
            "collapsed": True,
            "items": [
                {
                    "channelLabel": "ALL",
                    "label": reference.get("label"),
                    "linkedRowId": reference.get("linkedRowId"),
                    "direction": reference.get("direction"),
                    "title": reference.get("title"),
                    "durationSeconds": reference.get("durationSeconds"),
                }
            ],
        }

    items = []
    for channel in channels:
        item = channel_matches.get(channel)
        if not item or item.get("linkedRowId") is None or item.get("durationSeconds") is None:
            continue
        items.append(
            {
                "channelLabel": channel,
                "label": item.get("label"),
                "linkedRowId": item.get("linkedRowId"),
                "direction": item.get("direction"),
                "title": item.get("title"),
                "durationSeconds": item.get("durationSeconds"),
            }
        )
    return {"collapsed": False, "items": items}


def _normalize_event(
    raw_event: dict[str, Any], index: int, fallback_start: datetime | None, available_channels: list[str]
) -> dict[str, Any]:
    norm_time = _normalize_norm_time(raw_event.get("norm_time"), index)
    parsed_utc = _parse_datetime(raw_event.get("utctime"))
    if parsed_utc is not None:
        utctime = _isoformat_seconds(parsed_utc)
    elif fallback_start is not None:
        utctime = _isoformat_seconds(fallback_start + timedelta(seconds=norm_time))
    else:
        utctime = _isoformat_seconds(datetime.fromtimestamp(0, tz=timezone.utc))

    try:
        row_id = int(raw_event.get("row_id"))
    except (TypeError, ValueError):
        row_id = index + 1

    color = raw_event.get("color")
    normalized_color = color if color in CORE_EVENT_SEVERITY else "Green"
    raw_id = raw_event.get("id") or raw_event.get("name") or f"event-{index + 1}"
    raw_name = raw_event.get("name") or raw_event.get("id") or f"Event {index + 1}"
    system_fields = _normalize_entity_fields(raw_event, "system")
    subsystem_fields = _normalize_entity_fields(raw_event, "subsystem")
    unit_fields = _normalize_entity_fields(raw_event, "unit")

    return {
        **raw_event,
        "row_id": row_id,
        "time": utctime,
        "norm_time": norm_time,
        "utctime": utctime,
        "id": str(raw_id),
        "name": str(raw_name),
        "description": str(raw_event.get("description") or ""),
        "color": normalized_color,
        **system_fields,
        **subsystem_fields,
        **unit_fields,
        "code": str(raw_event.get("code") or ""),
        "set_clear": _normalize_action(raw_event.get("set_clear")),
        "channels": _normalize_channel_list(raw_event, available_channels),
        "data": raw_event.get("data"),
        "pairedChannels": {},
        "matchSummary": {"items": [], "collapsed": False},
    }


def _derive_core_events(raw_events: list[dict[str, Any]], available_channels: list[str]) -> list[dict[str, Any]]:
    fallback_start = _resolve_fallback_start(raw_events)
    events = [
        _normalize_event(event, index, fallback_start, available_channels) for index, event in enumerate(raw_events)
    ]
    ordered = sorted(events, key=lambda event: (float(event.get("norm_time") or 0), int(event.get("row_id") or 0)))
    open_sets: dict[str, list[dict[str, Any]]] = {}

    for event in ordered:
        for channel in event.get("channels", []):
            key = _build_pair_key(event, channel)
            queue = open_sets.setdefault(key, [])

            if event.get("set_clear") == "set":
                queue.append(event)
                continue

            if not queue:
                continue

            set_event = queue.pop(0)
            duration_seconds = max(0.0, float(event.get("norm_time") or 0) - float(set_event.get("norm_time") or 0))
            label = _format_duration_label(duration_seconds)

            set_event["pairedChannels"][channel] = {
                "channel": channel,
                "linkedRowId": event.get("row_id"),
                "linkedSeconds": event.get("norm_time"),
                "durationSeconds": duration_seconds,
                "direction": "forward",
                "label": label,
                "title": f"Jump to clear event for channel {channel}",
            }
            event["pairedChannels"][channel] = {
                "channel": channel,
                "linkedRowId": set_event.get("row_id"),
                "linkedSeconds": set_event.get("norm_time"),
                "durationSeconds": duration_seconds,
                "direction": "back",
                "label": label,
                "title": f"Jump to set event for channel {channel}",
            }

    for event in events:
        event["matchSummary"] = _summarize_matches(event, event.get("pairedChannels", {}))
        event["rowDisplay"] = _build_row_display(event)

    return events


def _resolve_bounds(payload: dict[str, Any], events: list[dict[str, Any]]) -> dict[str, Any]:
    start = _parse_datetime(payload.get("start"))
    end = _parse_datetime(payload.get("end"))

    # Use provided values when available; fall back per-field rather than
    # requiring both.  This preserves the explicit session start time even
    # when the session is still active (ended_at is None / "end" is absent).
    if start is None or end is None:
        ordered = sorted(events, key=lambda event: (float(event.get("norm_time") or 0), int(event.get("row_id") or 0)))
        first = ordered[0] if ordered else None
        last = ordered[-1] if ordered else None
        if start is None:
            start = _parse_datetime(first.get("utctime") if first else None) or datetime.fromtimestamp(0, tz=timezone.utc)
        if end is None:
            end = _parse_datetime(last.get("utctime") if last else None) or start

    return {
        "start": _isoformat_seconds(start),
        "end": _isoformat_seconds(end),
        "hours": max(0.0, (end - start).total_seconds() / 3600),
    }


class CoreEventPlugin(EventLogSourcePlugin):
    plugin_id = "core-event"
    plugin_name = "Core Event"
    script_paths = ("static/row.js", "static/charts.js")
    row_settings = {
        "channels": DEFAULT_CORE_EVENT_CHANNELS,
    }

    def get_log_types(self, session_store=None) -> list:
        from .log_types import CoreEventBootLogType
        return [CoreEventBootLogType(self.plugin_id, self.build_page_data)]

    def _build_view_config(self, channels: list[str]) -> dict[str, Any]:
        return {
            "scripts": self.get_inline_scripts(),
            "rowSettings": {
                "channels": list(channels),
            },
        }

    def _build_log_data(self, payload: Any) -> tuple[dict[str, Any], list[str]]:
        source = payload if isinstance(payload, dict) else {}
        raw_events = source.get("events")
        event_list = raw_events if isinstance(raw_events, list) else []
        valid_events = [event for event in event_list if isinstance(event, dict)]
        channels = _resolve_channel_catalog(source, valid_events)
        events = _derive_core_events(valid_events, channels)
        bounds = _resolve_bounds(source, events)
        modes = source.get("modes")

        return (
            {
                "pluginId": self.plugin_id,
                "logTypeId": "core_event",
                "pluginName": self.plugin_name,
                "channels": list(channels),
                "channelCount": len(channels),
                "start": bounds["start"],
                "end": bounds["end"],
                "hours": source.get("hours", bounds["hours"]),
                "seed": str(source.get("seed") or ""),
                "modes": modes if isinstance(modes, list) else [],
                "events": events,
            },
            channels,
        )

    def normalize_stream_events(self, raw_events: list[dict[str, Any]], channels: list[str]) -> list[dict[str, Any]]:
        """Normalize a batch of raw events for live streaming (no set/clear pairing)."""
        fallback_start = _resolve_fallback_start(raw_events)
        result = []
        for index, event in enumerate(raw_events):
            normalized = _normalize_event(event, index, fallback_start, channels)
            normalized["pairedChannels"] = {}
            normalized["matchSummary"] = {"items": [], "collapsed": False}
            normalized["rowDisplay"] = _build_row_display(normalized)
            result.append(normalized)
        return result

    def parse_payload(self, payload: Any) -> dict[str, Any]:
        log_data, _ = self._build_log_data(payload)
        return log_data

    def build_page_data(self, payload: Any) -> dict[str, Any]:
        log_data, channels = self._build_log_data(payload)
        return {
            "apiVersion": 1,
            "plugin": {
                "id": self.plugin_id,
                "name": self.plugin_name,
            },
            "logType": {
                "id": "core_event",
                "name": "Core Event",
            },
            "logData": log_data,
            "view": self._build_view_config(channels),
        }
