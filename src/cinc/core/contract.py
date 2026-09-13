from datetime import datetime
from typing import Any


def validate_events(events: list[dict[str, Any]], log_type_id: str) -> None:
    """Validate the small, domain-neutral normalized-event contract."""
    seen: set[int] = set()
    for index, event in enumerate(events):
        if not isinstance(event, dict):
            raise ValueError(f"{log_type_id}: event {index} is not an object")
        for key in ("row_id", "time", "log_type"):
            if key not in event:
                raise ValueError(f"{log_type_id}: event {index} missing {key}")
        row_id = event["row_id"]
        if isinstance(row_id, bool) or not isinstance(row_id, int):
            raise ValueError(
                f"{log_type_id}: event {index} row_id must be an integer"
            )
        if row_id in seen:
            raise ValueError(f"{log_type_id}: duplicate row_id {row_id}")
        seen.add(row_id)
        try:
            value = str(event["time"]).replace("Z", "+00:00")
            parsed = datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError(
                f"{log_type_id}: event {index} has an invalid time"
            ) from None
        if parsed.tzinfo is None:
            raise ValueError(
                f"{log_type_id}: event {index} time must include a timezone"
            )
        if event["log_type"] != log_type_id:
            raise ValueError(
                f"{log_type_id}: event {index} has the wrong log_type"
            )
