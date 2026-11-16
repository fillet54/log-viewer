from __future__ import annotations

import sys
from typing import Any

from .plugin import Asset


def row_id(index: int, base: int = 0, step: int = 1) -> int:
    """Row ids interleave log types: type slot ``base`` of ``step`` types gets
    base+1, base+1+step, ... A row's id depends only on its type's slot and its
    position, so a live log and a reloaded page agree on every id."""
    return base + 1 + index * step


class LogType:
    """Base class for a registered log type.

    A log type turns a raw payload (``{"events": [...], **header}``) into
    normalized events plus the viewer configuration for those events.
    Subclasses set ``id`` and ``name`` and override what they need.

    ``script_paths`` and ``style_paths`` name browser modules and stylesheets
    relative to the package of the class that lists them (or its
    ``asset_package``). They add up along the class hierarchy, so a subclass
    keeps its base's scripts and lists only its own. Each script's default
    export ``setup(ctx)`` is called with this type's id and ``view_config``
    (see docs/plugins.md), so a subclass registered under a new id gets the
    same rows and charts.
    """

    id: str
    name: str
    description: str = ""

    asset_package: str | None = None
    script_paths: tuple[str, ...] = ()
    style_paths: tuple[str, ...] = ()
    row_height: int | None = None  # pixels, when rows are not the default height

    def __init__(self):
        # (name, callback, whole_log)
        self._event_augmenters: list[tuple[str, Any, bool]] = []

    def detect(self, payload: Any) -> float:
        """Return 0..1 confidence that ``payload`` belongs to this type."""
        return 0.0

    def normalize_events(
        self, payload: dict[str, Any], *, row_id_base: int = 0, row_id_step: int = 1
    ) -> list[dict[str, Any]]:
        """Return normalized events; each needs ``row_id`` and ``time``.

        The n-th row (from 0, in payload order) gets ``row_id(n, base, step)``,
        so a row keeps its id however many rows come after it."""
        events = payload.get("events") if isinstance(payload, dict) else None
        if not isinstance(events, list):
            return []
        return [
            {**event, "row_id": row_id(index, row_id_base, row_id_step)}
            for index, event in enumerate(
                event for event in events if isinstance(event, dict)
            )
        ]

    def view_config(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Settings for this type's browser scripts (``ctx.config``)."""
        return {}

    def search_config(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Search settings: ``fields`` (names a bare search term can match as
        a field) and ``excludeFromBareTerms`` (fields bare terms skip)."""
        return {}

    def log_summary(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {}

    def tracks(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        """Position tracks for the Map chart (see ``cinc.core.track``).

        Return ``[{"id", "label", "color", "samples": [[ms, lat, lon, alt]]}]``;
        ``cinc.core.track.build_tracks`` builds this from sample rows. Called
        with the whole payload on a page load and with only the new rows in
        live mode, so each sample must depend on its own row only."""
        return []

    def scripts(self) -> list[Asset]:
        return self._assets("script_paths")

    def styles(self) -> list[Asset]:
        return self._assets("style_paths")

    def _assets(self, attribute: str) -> list[Asset]:
        """``attribute`` paths from every class in the hierarchy, base first,
        each resolved against the package of the class that lists it."""
        result: list[Asset] = []
        for cls in reversed(type(self).__mro__):
            paths = cls.__dict__.get(attribute)
            if not paths:
                continue
            package = cls.__dict__.get("asset_package") or _package_of_class(cls)
            for path in paths:
                asset = Asset(package, path)
                if asset not in result:
                    result.append(asset)
        return result

    def register_event_augmenter(
        self, name: str, callback: Any, whole_log: bool = False
    ) -> None:
        """Add ``callback(events, context) -> events``, run after
        ``normalize_events`` in registration order.

        An augmenter computes each event from that event and the header only:
        in live mode it is given just the new and changed events. One that
        needs every event (``whole_log=True``) puts the type on the slow path,
        where every post re-derives the type's whole log."""
        name = str(name or "").strip()
        if not name or not callable(callback):
            raise ValueError(f"{self.id}: augmenter requires a name and callback")
        if any(existing == name for existing, *_ in self._event_augmenters):
            raise ValueError(f"{self.id}: duplicate augmenter {name!r}")
        self._event_augmenters.append((name, callback, bool(whole_log)))

    @property
    def has_whole_log_augmenters(self) -> bool:
        return any(whole for *_, whole in getattr(self, "_event_augmenters", []))

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
            for key in ("row_id", "row_key", "time", "log_type")
        }
        context = {"log_type": self.id, "payload": dict(payload or {}), "mode": mode}
        for name, callback, _ in getattr(self, "_event_augmenters", []):
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


def _package_of_class(cls: type) -> str:
    module = sys.modules.get(cls.__module__)
    if module is not None and hasattr(module, "__path__"):
        return module.__name__
    if module is not None and module.__package__:
        return module.__package__
    return cls.__module__.rsplit(".", 1)[0]
