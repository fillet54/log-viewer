import json
from pathlib import Path
from typing import Any

from ..logdir import LogDir, is_log_dir
from .assemble import assemble_page_data
from .registry import Registry


def _registered(registry):
    return ", ".join(item.id for item in registry.all())


def resolve_sections(
    payload: Any,
    registry: Registry,
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


def _lookup(registry: Registry, type_id: str, path: str):
    log_type = registry.get(type_id)
    if log_type is None:
        raise ValueError(
            f"Unknown log type {type_id!r} for {path}. "
            f"Registered types: {_registered(registry)}"
        )
    return log_type


def log_dir_sections(log: LogDir, registry: Registry):
    return [
        (_lookup(registry, type_id, str(log.path)), payload)
        for type_id, payload in log.payloads()
    ]


def read_sections(path: Path, registry: Registry, *, log_type_id: str | None = None):
    """Sections for one CLI input: a log directory, a ``<type>.jsonl`` file,
    or a JSON payload/bundle file."""
    path = Path(path)
    if is_log_dir(path):
        return log_dir_sections(LogDir(path), registry)
    if path.suffix == ".jsonl":
        log = LogDir(path.parent)
        type_id = log_type_id or path.stem
        payload = {**log.header(path.stem), "events": log.rows(path.stem)}
        return [(_lookup(registry, type_id, str(path)), payload)]
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    return resolve_sections(payload, registry, path=str(path), log_type_id=log_type_id)


def build_from_paths(
    paths: list[Path], registry: Registry, *, log_type_id: str | None = None
):
    sections = []
    for path in paths:
        sections.extend(read_sections(path, registry, log_type_id=log_type_id))
    return assemble_page_data(sections, registry)
