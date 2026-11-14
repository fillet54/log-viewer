"""core_event settings that a log's header (``core_event.json``) can change.

``severities``: the colour names events use, lowest first, each mapped to one
of the viewer's tones::

    {"severities": [{"name": "Nominal", "tone": "green"},
                    {"name": "Caution", "tone": "yellow"},
                    {"name": "Warning", "tone": "red"}]}

``series``: line views on the timeline, one per numeric field of the events::

    {"series": [{"id": "bus-load", "label": "Bus Load", "field": "data.bus.load_pct",
                 "unit": "%", "where": {"id": "pwr_bus"}, "min": 0, "max": 100}]}

``where`` (optional) keeps only events whose fields equal the given values.
"""

from __future__ import annotations

import re
from typing import Any

TONES = ("green", "yellow", "red", "flashing-red")
DEFAULT_SEVERITIES = [
    {"name": "Green", "tone": "green"},
    {"name": "Yellow", "tone": "yellow"},
    {"name": "Red", "tone": "red"},
    {"name": "Flashing Red", "tone": "flashing-red"},
]
_PATH = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")


def severities(payload: dict[str, Any]) -> list[dict[str, str]]:
    """The payload's severity scale (lowest first), or the default one."""
    raw = payload.get("severities") if isinstance(payload, dict) else None
    if raw is None:
        return [dict(item) for item in DEFAULT_SEVERITIES]
    if not isinstance(raw, list) or not raw:
        raise ValueError("core_event: severities must be a non-empty list")
    result, seen = [], set()
    for index, item in enumerate(raw):
        if not isinstance(item, dict) or not str(item.get("name") or "").strip():
            raise ValueError(f"core_event: severity {index} needs a name")
        name, tone = str(item["name"]).strip(), item.get("tone")
        if tone not in TONES:
            raise ValueError(
                f"core_event: severity {name!r} tone must be one of {', '.join(TONES)}"
            )
        if name in seen:
            raise ValueError(f"core_event: duplicate severity {name!r}")
        seen.add(name)
        result.append({"name": name, "tone": tone})
    return result


def series(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """The payload's timeline line series (none by default)."""
    raw = payload.get("series") if isinstance(payload, dict) else None
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValueError("core_event: series must be a list")
    result, seen = [], set()
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(f"core_event: series {index} must be an object")
        series_id = str(item.get("id") or "").strip()
        if not series_id or series_id in seen:
            raise ValueError(f"core_event: series {index} needs a unique id")
        seen.add(series_id)
        field = item.get("field")
        if not isinstance(field, str) or not _PATH.fullmatch(field):
            raise ValueError(f"core_event: series {series_id!r} needs a field path")
        where = item.get("where") or {}
        if not isinstance(where, dict) or not all(_PATH.fullmatch(k) for k in where):
            raise ValueError(f"core_event: series {series_id!r} where must map fields")
        entry = {
            "id": series_id,
            "label": str(item.get("label") or series_id),
            "field": field,
            "unit": str(item.get("unit") or ""),
            "where": where,
        }
        for bound in ("min", "max"):
            if item.get(bound) is not None:
                if isinstance(item[bound], bool) or not isinstance(
                    item[bound], (int, float)
                ):
                    raise ValueError(f"core_event: series {series_id!r} {bound}")
                entry[bound] = item[bound]
        result.append(entry)
    return result
