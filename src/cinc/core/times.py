from __future__ import annotations

from datetime import datetime, timezone


def iso_time(value: datetime | str) -> str:
    """An ISO 8601 UTC time ("...Z"); a datetime without a zone is taken as UTC."""
    if isinstance(value, str):
        return value
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
