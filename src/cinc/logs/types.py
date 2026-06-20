from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class LogRecord:
    id: str
    log_type_id: str
    plugin_id: str
    name: str
    imported_at: datetime
    source: str = "import"
    status: str = "completed"
    started_at: datetime | None = None
    ended_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def imported_at_label(self) -> str:
        return self.imported_at.strftime("%Y-%m-%d %H:%M")

    @property
    def duration_seconds(self) -> float | None:
        if self.started_at is None or self.ended_at is None:
            return None
        return (self.ended_at - self.started_at).total_seconds()

    @property
    def duration_label(self) -> str:
        seconds = self.duration_seconds
        if seconds is None:
            return "ongoing" if self.status == "active" else "—"
        if seconds < 60:
            return f"{seconds:.0f}s"
        minutes, remainder = divmod(int(seconds), 60)
        return f"{minutes}m {remainder}s"


class LogTypeDefinition(ABC):
    """Base class for a plugin-registered log type.

    Subclasses set class-level ``id``, ``name``, and optionally ``description``,
    then implement ``parse_import`` and optionally override the display helpers.
    """

    id: str
    name: str
    description: str = ""

    def __init__(self, plugin_id: str) -> None:
        self.plugin_id = plugin_id

    @property
    def full_id(self) -> str:
        """Globally-unique identifier: ``'{plugin_id}.{id}'``."""
        return f"{self.plugin_id}.{self.id}"

    @abstractmethod
    def parse_import(
        self,
        *,
        file: Any = None,
        json_data: dict[str, Any] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Parse an import request.

        Accepts either a file-like object (from a multipart upload) or a
        parsed JSON dict (from a REST request body).
        Returns ``(display_name, payload)`` or raises ``ValueError``.
        """

    def extract_metadata(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Return lightweight metadata to store alongside the log for list display."""
        return {}

    def get_list_columns(self) -> list[dict[str, str]]:
        """Column definitions for the list table: ``[{"key": …, "label": …}]``."""
        return []

    def format_list_row(self, record: LogRecord) -> dict[str, str]:
        """Return display strings keyed by the columns from ``get_list_columns``."""
        return {}

    def build_view_page_data(
        self, record: LogRecord, payload: dict[str, Any]
    ) -> dict[str, Any]:
        """Build the viewer ``page_data`` for viewing this log."""
        return payload

    def normalize_events(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        """Return normalized event maps for storage/search.

        The default expects payloads to already expose an ``events`` array.
        Log types can override when they need richer normalization before
        persisting events.
        """
        events = payload.get("events") if isinstance(payload, dict) else None
        return [event for event in events if isinstance(event, dict)] if isinstance(events, list) else []

    def build_payload_from_events(self, record: LogRecord, events: list[dict[str, Any]]) -> dict[str, Any]:
        """Rebuild a viewer payload from persisted normalized events."""
        payload = dict(record.metadata.get("payload_header") or {})
        payload["events"] = events
        return payload

    def get_import_template(self) -> str | None:
        """Return a custom import template name, if this log type has one."""
        return None

    def get_supported_charts(self) -> dict[str, list[str]]:
        """Return supported chart and timeline IDs for the viewer."""
        return {"charts": [], "timelineViews": []}

    def extra_records(self, search: str = "") -> list[LogRecord]:
        """Additional LogRecords not held in the main LogStore.

        IDs must be prefixed so they can be distinguished from store IDs.
        Override in log types that expose records from secondary sources
        (e.g., persisted live-capture sessions).
        """
        return []

    def get_extra_payload(self, record_id: str) -> dict[str, Any] | None:
        """Return payload for a record returned by extra_records.

        Called when ``record_id`` is not found in the main LogStore.
        """
        return None
