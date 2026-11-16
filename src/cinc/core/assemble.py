from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .contract import validate_events
from .track import merge_tracks

if TYPE_CHECKING:  # pragma: no cover
    from .registry import Registry

# Version of the page data and browser plugin API (docs/plugins.md).
API_VERSION = 4


def normalize_section(
    log_type: Any,
    payload: dict[str, Any],
    *,
    row_id_base: int = 0,
    row_id_step: int = 1,
    mode="batch",
) -> list[dict[str, Any]]:
    """Normalize, augment and validate one type's payload into viewer events."""
    events, _ = normalize_with_instants(
        log_type,
        payload,
        row_id_base=row_id_base,
        row_id_step=row_id_step,
        mode=mode,
    )
    return events


def normalize_with_instants(
    log_type: Any,
    payload: dict[str, Any],
    *,
    row_id_base: int,
    row_id_step: int,
    mode: str,
) -> tuple[list[dict[str, Any]], list[Any]]:
    """``normalize_section``, plus each event's parsed time."""
    produced = log_type.normalize_events(
        payload, row_id_base=row_id_base, row_id_step=row_id_step
    )
    for key in ("log_type", "row_key"):
        if any(key in event for event in produced):
            raise ValueError(f"{log_type.id}: normalized events must not supply {key}")
    # row_key: the event's position in its type's output. Unlike row_id it does
    # not depend on how many types the log has, so it is what to store.
    produced = [
        dict(event, row_key=f"{log_type.id}:{index}")
        for index, event in enumerate(produced)
    ]
    produced = log_type.augment_events(produced, payload=payload, mode=mode)
    events = [dict(event, log_type=log_type.id) for event in produced]
    instants = validate_events(events, log_type.id)
    return events, instants


def type_config(
    log_type: Any, payload: dict[str, Any], registry: "Registry | None" = None
) -> dict[str, Any]:
    """What the viewer gets for one log type: its settings, and the browser
    modules to set up for it (its own, then any extensions')."""
    extensions = registry.extensions_for(log_type.id) if registry else []
    return {
        "name": log_type.name,
        "rowHeight": log_type.row_height,
        "search": log_type.search_config(payload),
        "config": log_type.view_config(payload),
        "modules": [asset.module_id for asset in log_type.scripts()],
        "extensions": [
            {
                "plugin": extension.plugin_id,
                "modules": [asset.module_id for asset in extension.scripts],
                "config": extension.config,
            }
            for extension in extensions
        ],
    }


def assemble_page_data(
    sections: list[tuple[Any, dict[str, Any]]],
    registry: "Registry | None" = None,
    *,
    normalized: list[tuple[list[dict[str, Any]], list[Any]]] | None = None,
) -> dict[str, Any]:
    """Page data for one log from its ``(log type, payload)`` sections.

    ``registry`` adds what plugins contributed: extensions to these types and
    global scripts and styles. ``normalized`` gives each section's events and
    instants (``normalize_with_instants`` with this slot and step) when the
    caller already has them, as the live hub does."""
    keyed: list[tuple[Any, int, dict[str, Any]]] = []
    configs: dict[str, dict[str, Any]] = {}
    summary: dict[str, Any] = {}
    styles: list[Any] = list(registry.styles) if registry else []
    track_groups: list[list[dict[str, Any]]] = []
    # Section i's rows get ids i+1, i+1+n, ... (n sections), so ids are stable
    # as a live log grows (see LiveHub.post).
    for slot, (log_type, payload) in enumerate(sections):
        if normalized is not None:
            section, instants = normalized[slot]
        else:
            section, instants = normalize_with_instants(
                log_type,
                payload,
                row_id_base=slot,
                row_id_step=len(sections),
                mode="batch",
            )
        keyed.extend(
            (instant, event["row_id"], event)
            for instant, event in zip(instants, section)
        )
        configs[log_type.id] = type_config(log_type, payload, registry)
        summary.update(log_type.log_summary(payload))
        track_groups.append(log_type.tracks(payload))
        styles.extend(log_type.styles())
        if registry:
            for extension in registry.extensions_for(log_type.id):
                styles.extend(extension.styles)
    # By instant, not by the time string: "05.5Z" is after "05Z", and
    # "01:00:04+01:00" before "00:00:05Z".
    keyed.sort(key=lambda item: (item[0], item[1]))
    events = [event for _, _, event in keyed]
    return {
        "apiVersion": API_VERSION,
        "logTypes": configs,
        "log": {"summary": summary},
        "logData": {"events": events, "tracks": merge_tracks(track_groups)},
        "view": {
            "modules": [a.module_id for a in registry.scripts] if registry else [],
            "styles": [asset.read_text() for asset in dict.fromkeys(styles)],
        },
    }
