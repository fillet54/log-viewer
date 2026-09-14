from __future__ import annotations

import re
from typing import Any

_SEVERITIES = {"green", "yellow", "red", "flashing-red"}
_PATH = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")


def validate_chart_config(config: Any, log_type_id: str) -> dict[str, Any]:
    if config is None:
        return {}
    if not isinstance(config, dict):
        raise ValueError(f"{log_type_id}: chart_config must be an object")
    result = dict(config)
    severity = result.get("severity")
    if severity is not None:
        if not isinstance(severity, dict) or not isinstance(severity.get("field"), str):
            raise ValueError(f"{log_type_id}: severity requires a field")
        _validate_path(severity["field"], log_type_id, "severity field")
        mapping = severity.get("mapping")
        if not isinstance(mapping, dict) or not mapping:
            raise ValueError(f"{log_type_id}: severity requires a mapping")
        if any(str(value) not in _SEVERITIES for value in mapping.values()):
            raise ValueError(f"{log_type_id}: invalid severity mapping")
    series = result.get("series", [])
    if not isinstance(series, list):
        raise ValueError(f"{log_type_id}: chart series must be an array")
    seen: set[str] = set()
    for item in series:
        if not isinstance(item, dict):
            raise ValueError(f"{log_type_id}: chart series entries must be objects")
        series_id = str(item.get("id") or "").strip()
        if not series_id or series_id in seen:
            raise ValueError(f"{log_type_id}: duplicate or missing chart series id")
        seen.add(series_id)
        if item.get("kind", "line") != "line":
            raise ValueError(f"{log_type_id}: unsupported chart series kind")
        field = item.get("field")
        if not isinstance(field, str):
            raise ValueError(f"{log_type_id}: chart series requires a field")
        _validate_path(field, log_type_id, "chart series field")
    result["series"] = series
    return result


def _validate_path(path: str, log_type_id: str, label: str) -> None:
    if not _PATH.fullmatch(path):
        raise ValueError(f"{log_type_id}: invalid {label}")
