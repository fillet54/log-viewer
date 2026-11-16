from datetime import datetime
from typing import Any

from .times import MissingZoneError, parse_time


def validate_events(events: list[dict[str, Any]], log_type_id: str) -> list[datetime]:
    """Validate the small, domain-neutral normalized-event contract. Returns
    each event's time as an aware datetime, in order."""
    seen: set[int] = set()
    instants: list[datetime] = []
    for index, event in enumerate(events):
        if not isinstance(event, dict):
            raise ValueError(f"{log_type_id}: event {index} is not an object")
        for key in ("row_id", "time", "log_type"):
            if key not in event:
                raise ValueError(f"{log_type_id}: event {index} missing {key}")
        row_id = event["row_id"]
        if isinstance(row_id, bool) or not isinstance(row_id, int):
            raise ValueError(f"{log_type_id}: event {index} row_id must be an integer")
        if row_id in seen:
            raise ValueError(f"{log_type_id}: duplicate row_id {row_id}")
        seen.add(row_id)
        try:
            instants.append(parse_time(event["time"], require_zone=True))
        except MissingZoneError:
            raise ValueError(
                f"{log_type_id}: event {index} time must include a timezone"
            ) from None
        except ValueError:
            raise ValueError(
                f"{log_type_id}: event {index} has an invalid time"
            ) from None
        if event["log_type"] != log_type_id:
            raise ValueError(f"{log_type_id}: event {index} has the wrong log_type")
    return instants
