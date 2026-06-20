from __future__ import annotations

import json
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .types import LogRecord


class LogStore:
    def __init__(self, data_dir: Path) -> None:
        self._base = Path(data_dir)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _safe(self, full_id: str) -> str:
        return full_id.replace(".", "_").replace("/", "_").replace(":", "_")

    def _type_dir(self, log_type_id: str) -> Path:
        return self._base / self._safe(log_type_id)

    def _log_dir(self, log_type_id: str, log_id: str) -> Path:
        return self._type_dir(log_type_id) / log_id

    def _parse_meta(self, data: dict[str, Any]) -> LogRecord:
        return LogRecord(
            id=data["id"],
            log_type_id=data["log_type_id"],
            plugin_id=data["plugin_id"],
            name=data["name"],
            imported_at=datetime.fromisoformat(data["imported_at"]),
            metadata=data.get("metadata", {}),
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def store(
        self,
        log_type_id: str,
        plugin_id: str,
        name: str,
        metadata: dict[str, Any],
        payload: dict[str, Any],
    ) -> LogRecord:
        log_id = str(uuid.uuid4())
        d = self._log_dir(log_type_id, log_id)
        d.mkdir(parents=True, exist_ok=True)

        imported_at = datetime.now(timezone.utc)
        meta: dict[str, Any] = {
            "id": log_id,
            "log_type_id": log_type_id,
            "plugin_id": plugin_id,
            "name": name,
            "imported_at": imported_at.isoformat(),
            "metadata": metadata,
        }
        (d / "meta.json").write_text(json.dumps(meta, indent=2))
        (d / "data.json").write_text(json.dumps(payload))

        return LogRecord(
            id=log_id,
            log_type_id=log_type_id,
            plugin_id=plugin_id,
            name=name,
            imported_at=imported_at,
            metadata=metadata,
        )

    def get_record(self, log_type_id: str, log_id: str) -> LogRecord | None:
        path = self._log_dir(log_type_id, log_id) / "meta.json"
        if not path.exists():
            return None
        try:
            return self._parse_meta(json.loads(path.read_text()))
        except Exception:
            return None

    def get_payload(self, log_type_id: str, log_id: str) -> dict[str, Any] | None:
        path = self._log_dir(log_type_id, log_id) / "data.json"
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text())
        except Exception:
            return None

    def list_records(
        self,
        log_type_id: str,
        page: int = 1,
        per_page: int = 20,
        search: str = "",
    ) -> tuple[list[LogRecord], int]:
        type_dir = self._type_dir(log_type_id)
        if not type_dir.exists():
            return [], 0

        records: list[LogRecord] = []
        for subdir in type_dir.iterdir():
            if not subdir.is_dir():
                continue
            meta_path = subdir / "meta.json"
            if not meta_path.exists():
                continue
            try:
                records.append(self._parse_meta(json.loads(meta_path.read_text())))
            except Exception:
                continue

        records.sort(key=lambda r: r.imported_at, reverse=True)

        if search:
            lo = search.lower()
            records = [r for r in records if lo in r.name.lower()]

        total = len(records)
        start = (page - 1) * per_page
        return records[start : start + per_page], total

    def count(self, log_type_id: str) -> int:
        _, total = self.list_records(log_type_id, per_page=1)
        return total

    def delete(self, log_type_id: str, log_id: str) -> bool:
        d = self._log_dir(log_type_id, log_id)
        if not d.exists():
            return False
        shutil.rmtree(d)
        return True
