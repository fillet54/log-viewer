from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from ..base import EventLogPlugin

CORE_EVENT_CHANNELS = ["A", "B", "C", "D"]
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


def _normalize_channel_list(event: dict[str, Any]) -> list[str]:
    explicit = event.get("channels")
    if isinstance(explicit, list):
        valid = []
        for item in explicit:
            channel = str(item or "").strip().upper()
            if channel in CORE_EVENT_CHANNELS and channel not in valid:
                valid.append(channel)
        if valid:
            return [channel for channel in CORE_EVENT_CHANNELS if channel in valid]

    inferred = []
    for channel in CORE_EVENT_CHANNELS:
        if event.get(f"{channel.lower()}_time") is not None:
            inferred.append(channel)
    return inferred


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


def _normalize_event(raw_event: dict[str, Any], index: int, fallback_start: datetime | None) -> dict[str, Any]:
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

    return {
        **raw_event,
        "row_id": row_id,
        "norm_time": norm_time,
        "utctime": utctime,
        "id": str(raw_id),
        "name": str(raw_name),
        "description": str(raw_event.get("description") or ""),
        "color": normalized_color,
        "system": str(raw_event.get("system") or ""),
        "subsystem": str(raw_event.get("subsystem") or ""),
        "unit": str(raw_event.get("unit") or ""),
        "code": str(raw_event.get("code") or ""),
        "set_clear": _normalize_action(raw_event.get("set_clear")),
        "channels": _normalize_channel_list(raw_event),
        "data": raw_event.get("data"),
        "pairedChannels": {},
        "matchSummary": {"items": [], "collapsed": False},
    }


def _derive_core_events(raw_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fallback_start = _resolve_fallback_start(raw_events)
    events = [_normalize_event(event, index, fallback_start) for index, event in enumerate(raw_events)]
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
    if start is not None and end is not None:
        return {
            "start": _isoformat_seconds(start),
            "end": _isoformat_seconds(end),
            "hours": max(0.0, (end - start).total_seconds() / 3600),
        }

    ordered = sorted(events, key=lambda event: (float(event.get("norm_time") or 0), int(event.get("row_id") or 0)))
    first = ordered[0] if ordered else None
    last = ordered[-1] if ordered else None
    first_time = _parse_datetime(first.get("utctime") if first else None) or datetime.fromtimestamp(0, tz=timezone.utc)
    last_time = _parse_datetime(last.get("utctime") if last else None) or first_time

    return {
        "start": _isoformat_seconds(first_time),
        "end": _isoformat_seconds(last_time),
        "hours": max(0.0, (last_time - first_time).total_seconds() / 3600),
    }


class CoreEventPlugin(EventLogPlugin):
    plugin_id = "core-event"
    plugin_name = "Core Event"
    row_template_path = "templates/log_row.html"
    script_paths = ("static/row.js", "static/charts.js")
    row_settings = {
        "channels": CORE_EVENT_CHANNELS,
    }

    def parse_payload(self, payload: Any) -> dict[str, Any]:
        source = payload if isinstance(payload, dict) else {}
        raw_events = source.get("events")
        event_list = raw_events if isinstance(raw_events, list) else []
        events = _derive_core_events([event for event in event_list if isinstance(event, dict)])
        bounds = _resolve_bounds(source, events)
        modes = source.get("modes")

        return {
            "pluginId": self.plugin_id,
            "pluginName": self.plugin_name,
            "start": bounds["start"],
            "end": bounds["end"],
            "hours": source.get("hours", bounds["hours"]),
            "seed": str(source.get("seed") or ""),
            "modes": modes if isinstance(modes, list) else [],
            "events": events,
        }
