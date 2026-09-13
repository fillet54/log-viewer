from typing import Any
from .contract import validate_events


def assemble_page_data(
    sections: list[tuple[Any, dict[str, Any]]],
) -> dict[str, Any]:
    events: list[dict[str, Any]] = []
    configs: dict[str, dict[str, Any]] = {}
    payloads: dict[str, dict[str, Any]] = {}
    base = 0
    for log_type, payload in sections:
        produced = log_type.normalize_events(payload, row_id_base=base)
        for event in produced:
            event = dict(event)
            event["log_type"] = log_type.id
            events.append(event)
        validate_events(
            [dict(event, log_type=log_type.id) for event in produced],
            log_type.id,
        )
        base += len(produced)
        view = log_type.view_config(payload)
        configs[log_type.id] = {
            "name": log_type.name,
            "rowSettings": view.get("rowSettings", {}),
            "charts": view.get("charts", []),
            "timelineViews": view.get("timelineViews", []),
            "search": log_type.search_config(payload),
        }
        payloads[log_type.id] = payload
    events.sort(key=lambda event: (event["time"], event["row_id"]))
    summary: dict[str, Any] = {}
    for log_type, payload in sections:
        summary.update(log_type.log_summary(payload))
    scripts, styles = [], []
    for log_type, _ in sections:
        for item in log_type.inline_scripts() + log_type.inline_styles():
            (scripts if item in log_type.inline_scripts() else styles).append(item)
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
