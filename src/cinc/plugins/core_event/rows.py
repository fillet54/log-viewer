"""Building core_event rows from Python, instead of assembling dicts by hand."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from cinc.core.times import iso_time


def event(
    time: datetime | str,
    name: str,
    *,
    system: str,
    channels: list[str],
    set_clear: str = "set",
    color: str | None = None,
    subsystem: str | None = None,
    unit: str | None = None,
    code: str | None = None,
    description: str | None = None,
    data: dict[str, Any] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """One raw core_event row.

    ``set_clear`` is ``"set"`` or ``"clear"``; a CLEAR pairs with the earlier
    SET of the same system/subsystem/unit/code on each channel. ``color`` names
    a severity of the log's scale (see ``config``). ``extra`` fields are kept
    on the row and are searchable."""
    if set_clear not in ("set", "clear"):
        raise ValueError("set_clear must be 'set' or 'clear'")
    if not channels:
        raise ValueError("an event needs at least one channel")
    row: dict[str, Any] = {
        "utctime": iso_time(time),
        "name": name,
        "system": system,
        "set_clear": set_clear,
        "channels": list(channels),
    }
    optional = {
        "color": color,
        "subsystem": subsystem,
        "unit": unit,
        "code": code,
        "description": description,
        "data": data,
    }
    row.update({key: value for key, value in optional.items() if value is not None})
    row.update(extra)
    return row
