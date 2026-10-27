from typing import Any

from .chart_config import validate_chart_config
from .contract import validate_events


def normalize_section(
    log_type: Any, payload: dict[str, Any], *, row_id_base: int = 0, mode="batch"
) -> list[dict[str, Any]]:
    """Normalize, augment and validate one type's payload into viewer events."""
    produced = log_type.normalize_events(payload, row_id_base=row_id_base)
    if any("log_type" in event for event in produced):
        raise ValueError(f"{log_type.id}: normalized events must not supply log_type")
    produced = log_type.augment_events(produced, payload=payload, mode=mode)
    events = [dict(event, log_type=log_type.id) for event in produced]
    validate_events(events, log_type.id)
    return events


def type_config(log_type: Any, payload: dict[str, Any]) -> dict[str, Any]:
    view = log_type.view_config(payload)
    return {
        "name": log_type.name,
        "rowSettings": view.get("rowSettings", {}),
        "charts": view.get("charts", []),
        "timelineViews": view.get("timelineViews", []),
        "search": log_type.search_config(payload),
        "chartConfig": validate_chart_config(
            log_type.chart_config(payload), log_type.id
        ),
    }


def assemble_page_data(
    sections: list[tuple[Any, dict[str, Any]]],
) -> dict[str, Any]:
    events: list[dict[str, Any]] = []
    configs: dict[str, dict[str, Any]] = {}
    summary: dict[str, Any] = {}
    scripts: list[str] = []
    styles: list[str] = []
    for log_type, payload in sections:
        events.extend(normalize_section(log_type, payload, row_id_base=len(events)))
        configs[log_type.id] = type_config(log_type, payload)
        summary.update(log_type.log_summary(payload))
        scripts.extend(log_type.inline_scripts())
        styles.extend(log_type.inline_styles())
    events.sort(key=lambda event: (event["time"], event["row_id"]))
    return {
        "apiVersion": 2,
        "logTypes": configs,
        "log": {"summary": summary},
        "logData": {"events": events},
        "view": {
            "scripts": list(dict.fromkeys(scripts)),
            "styles": list(dict.fromkeys(styles)),
        },
    }
