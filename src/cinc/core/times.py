from __future__ import annotations

import re
from datetime import datetime, timezone

_FRACTION = re.compile(r"(\d{2}:\d{2}:\d{2})[.,](\d+)")


class MissingZoneError(ValueError):
    """A time without a timezone where one is required."""


def parse_time(value: datetime | str, *, require_zone: bool = False) -> datetime:
    """An aware datetime for an ISO 8601 time. Every time cinc reads goes
    through this.

    Accepts a trailing ``Z`` and any number of fraction digits (cut or padded
    to microseconds), so Python 3.10 reads what 3.11+ ``fromisoformat`` does.
    A time without a zone is UTC, or with ``require_zone`` raises
    ``MissingZoneError``. Anything else raises ``ValueError``."""
    if isinstance(value, datetime):
        parsed = value
    else:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"invalid time {value!r}")
        text = value.strip()
        if text[-1] in "Zz":
            text = text[:-1] + "+00:00"
        text = _FRACTION.sub(
            lambda m: f"{m.group(1)}.{m.group(2)[:6].ljust(6, '0')}", text, count=1
        )
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            raise ValueError(f"invalid time {value!r}") from None
    if parsed.tzinfo is None:
        if require_zone:
            raise MissingZoneError(f"time {value!r} must include a timezone")
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def iso_time(value: datetime | str) -> str:
    """An ISO 8601 UTC time ("...Z"); a datetime without a zone is taken as UTC."""
    if isinstance(value, str):
        return value
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
