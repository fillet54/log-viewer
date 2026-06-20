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
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def imported_at_label(self) -> str:
        return self.imported_at.strftime("%Y-%m-%d %H:%M")


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
