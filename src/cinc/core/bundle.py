import json
from pathlib import Path
from typing import Any

from .assemble import assemble_page_data
from .registry import LogTypeRegistry


def _registered(registry):
    return ", ".join(item.id for item in registry.all())


def resolve_sections(
    payload: Any,
    registry: LogTypeRegistry,
    *,
    path: str = "<input>",
    log_type_id: str | None = None,
):
    if isinstance(payload, dict) and "cinc" in payload:
        sections = payload.get("sections")
        if not isinstance(sections, list):
            raise ValueError(f"Bundle {path} must contain a sections array.")
        result = []
        for section in sections:
            declared = section.get("logType") if isinstance(section, dict) else None
            log_type = registry.get(str(declared))
            if log_type is None:
                raise ValueError(
                    f"Unknown log type {declared!r} for {path}. "
                    f"Registered types: {_registered(registry)}"
                )
            result.append((log_type, section.get("payload", {})))
        return result
    declared = payload.get("logType") if isinstance(payload, dict) else None
    chosen = str(declared or log_type_id or "")
    if chosen:
        log_type = registry.get(chosen)
        if log_type is None:
            raise ValueError(
                f"Unknown log type {chosen!r} for {path}. "
                f"Registered types: {_registered(registry)}"
            )
        clean = dict(payload)
        clean.pop("logType", None)
        return [(log_type, clean)]
    scores = [(t.detect(payload), t) for t in registry.all()]
    best = max((score for score, _ in scores), default=0.0)
    matches = [t for score, t in scores if score == best and score > 0.5]
    if not matches:
        raise ValueError(
            f"Cannot determine log type for {path}. Registered types: "
            f'{_registered(registry)}. Pass --log-type, or add a "logType" key '
            "to the file."
        )
    if len(matches) > 1:
        raise ValueError(
            f"Ambiguous log type for {path}: "
            f"{', '.join(t.id for t in matches)} both match. "
            "Pass --log-type to disambiguate."
        )
    return [(matches[0], payload)]


def build_from_paths(
    paths: list[Path], registry: LogTypeRegistry, *, log_type_id: str | None = None
):
    sections = []
    for path in paths:
        with path.open(encoding="utf-8") as handle:
            sections.extend(
                resolve_sections(
                    json.load(handle), registry, path=str(path), log_type_id=log_type_id
                )
            )
    return assemble_page_data(sections)
