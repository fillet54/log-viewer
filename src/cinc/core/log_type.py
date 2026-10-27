from __future__ import annotations

from importlib.resources import files
from typing import Any


class LogType:
    """Base class for a registered log type.

    A log type turns a raw payload (``{"events": [...], **header}``) into
    normalized events plus the viewer configuration for those events.
    Subclasses set ``id`` and ``name`` and override what they need.
    """

    id: str
    name: str
    description: str = ""

    asset_package: str | None = None
    script_paths: tuple[str, ...] = ()
    style_paths: tuple[str, ...] = ()

    def __init__(self):
        self._event_augmenters: list[tuple[str, Any]] = []

    def detect(self, payload: Any) -> float:
        """Return 0..1 confidence that ``payload`` belongs to this type."""
        return 0.0

    def normalize_events(
        self, payload: dict[str, Any], *, row_id_base: int = 0
    ) -> list[dict[str, Any]]:
        """Return normalized events; each needs ``row_id`` and ``time``."""
        events = payload.get("events") if isinstance(payload, dict) else None
        if not isinstance(events, list):
            return []
        return [
            {**event, "row_id": row_id}
            for row_id, event in enumerate(
                (event for event in events if isinstance(event, dict)),
                row_id_base + 1,
            )
        ]

    def view_config(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {}

    def search_config(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {}

    def chart_config(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {}

    def log_summary(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {}

    def inline_scripts(self) -> list[str]:
        return [self.read_asset_text(path) for path in self.script_paths]

    def inline_styles(self) -> list[str]:
        return [self.read_asset_text(path) for path in self.style_paths]

    def read_asset_text(self, relative_path: str) -> str:
        package = self.asset_package or self.__class__.__module__.rsplit(".", 1)[0]
        return files(package).joinpath(relative_path).read_text(encoding="utf-8")

    def register_event_augmenter(self, name: str, callback: Any) -> None:
        name = str(name or "").strip()
        if not name or not callable(callback):
            raise ValueError(f"{self.id}: augmenter requires a name and callback")
        if any(existing == name for existing, _ in self._event_augmenters):
            raise ValueError(f"{self.id}: duplicate augmenter {name!r}")
        self._event_augmenters.append((name, callback))

    def augment_events(
        self,
        events: list[dict[str, Any]],
        *,
        payload: dict[str, Any],
        mode: str = "batch",
    ) -> list[dict[str, Any]]:
        current = [dict(event) for event in events]
        protected = {
            key: [event.get(key) for event in current]
            for key in ("row_id", "time", "log_type")
        }
        context = {"log_type": self.id, "payload": dict(payload or {}), "mode": mode}
        for name, callback in getattr(self, "_event_augmenters", []):
            try:
                result = callback([dict(event) for event in current], context)
            except Exception as exc:
                raise ValueError(
                    f"{self.id}: augmenter {name!r} failed: {exc}"
                ) from exc
            if not isinstance(result, list) or len(result) != len(current):
                raise ValueError(f"{self.id}: augmenter {name!r} changed event count")
            if any(not isinstance(event, dict) for event in result):
                raise ValueError(f"{self.id}: augmenter {name!r} returned a non-object")
            for key, values in protected.items():
                if [event.get(key) for event in result] != values:
                    raise ValueError(f"{self.id}: augmenter {name!r} changed {key}")
            current = [dict(event) for event in result]
        return current
