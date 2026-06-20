from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .types import LogRecord


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _format_dt(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _json_loads(value: str | None, fallback: Any) -> Any:
    if not value:
        return fallback
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return fallback


def _event_time(event: dict[str, Any]) -> str:
    for key in ("time", "utctime"):
        value = event.get(key)
        if value:
            return str(value)
    try:
        seconds = float(event.get("norm_time") or 0)
    except (TypeError, ValueError):
        seconds = 0
    return datetime.fromtimestamp(seconds, tz=timezone.utc).isoformat()


class LogStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = Path(db_path)
        if self._db_path.suffix != ".sqlite":
            self._db_path = self._db_path / "cinc.sqlite"
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self._db_path)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA foreign_keys = ON")
        return con

    def _init_db(self) -> None:
        with self._connect() as con:
            con.executescript(
                """
                CREATE TABLE IF NOT EXISTS log_records (
                    id TEXT PRIMARY KEY,
                    log_type TEXT NOT NULL,
                    plugin_id TEXT NOT NULL DEFAULT '',
                    name TEXT NOT NULL,
                    source TEXT NOT NULL,
                    status TEXT NOT NULL,
                    imported_at TEXT NOT NULL,
                    started_at TEXT,
                    ended_at TEXT,
                    metadata_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS log_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    log_id TEXT NOT NULL REFERENCES log_records(id) ON DELETE CASCADE,
                    log_type TEXT NOT NULL,
                    time TEXT NOT NULL,
                    row_id INTEGER,
                    source TEXT NOT NULL,
                    tags_json TEXT NOT NULL DEFAULT '[]',
                    event_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_log_records_type_imported
                    ON log_records(log_type, imported_at);
                CREATE INDEX IF NOT EXISTS idx_log_records_source_status
                    ON log_records(source, status);
                CREATE INDEX IF NOT EXISTS idx_log_events_log_row
                    ON log_events(log_id, row_id);
                CREATE INDEX IF NOT EXISTS idx_log_events_log_time
                    ON log_events(log_id, time);
                """
            )

    def _record_from_row(self, row: sqlite3.Row) -> LogRecord:
        return LogRecord(
            id=str(row["id"]),
            log_type_id=str(row["log_type"]),
            plugin_id=str(row["plugin_id"] or ""),
            name=str(row["name"]),
            imported_at=_parse_dt(row["imported_at"]) or datetime.fromtimestamp(0, tz=timezone.utc),
            source=str(row["source"] or "import"),
            status=str(row["status"] or "completed"),
            started_at=_parse_dt(row["started_at"]),
            ended_at=_parse_dt(row["ended_at"]),
            metadata=_json_loads(row["metadata_json"], {}),
        )

    def create_log(
        self,
        log_type_id: str,
        plugin_id: str,
        name: str,
        metadata: dict[str, Any] | None = None,
        *,
        source: str = "import",
        status: str = "completed",
        started_at: datetime | None = None,
        ended_at: datetime | None = None,
        log_id: str | None = None,
    ) -> LogRecord:
        record_id = log_id or str(uuid.uuid4())
        imported_at = _utc_now()
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO log_records (
                    id, log_type, plugin_id, name, source, status, imported_at,
                    started_at, ended_at, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record_id,
                    log_type_id,
                    plugin_id,
                    name,
                    source,
                    status,
                    _format_dt(imported_at),
                    _format_dt(started_at),
                    _format_dt(ended_at),
                    json.dumps(metadata or {}),
                ),
            )
        return LogRecord(
            id=record_id,
            log_type_id=log_type_id,
            plugin_id=plugin_id,
            name=name,
            imported_at=imported_at,
            source=source,
            status=status,
            started_at=started_at,
            ended_at=ended_at,
            metadata=metadata or {},
        )

    def append_events(
        self,
        log_id: str,
        events: list[dict[str, Any]],
        *,
        source: str = "import",
        tags: tuple[str, ...] | list[str] = (),
    ) -> None:
        if not events:
            return
        record = self.get_record(log_id)
        if not record:
            raise ValueError(f"Unknown log record: {log_id}")

        tag_list = [str(tag) for tag in tags if str(tag)]
        rows = []
        for event in events:
            normalized = dict(event)
            normalized.setdefault("time", _event_time(normalized))
            rows.append(
                (
                    log_id,
                    record.log_type_id,
                    normalized["time"],
                    normalized.get("row_id"),
                    source,
                    json.dumps(tag_list),
                    json.dumps(normalized),
                )
            )

        metadata = dict(record.metadata or {})
        metadata["event_count"] = int(metadata.get("event_count") or 0) + len(rows)
        with self._connect() as con:
            con.executemany(
                """
                INSERT INTO log_events (log_id, log_type, time, row_id, source, tags_json, event_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            con.execute(
                "UPDATE log_records SET metadata_json = ? WHERE id = ?",
                (json.dumps(metadata), log_id),
            )

    def complete_log(
        self,
        log_id: str,
        *,
        ended_at: datetime | None = None,
        status: str = "completed",
    ) -> None:
        with self._connect() as con:
            con.execute(
                "UPDATE log_records SET status = ?, ended_at = ? WHERE id = ?",
                (status, _format_dt(ended_at or _utc_now()), log_id),
            )

    def update_record_metadata(self, log_id: str, metadata: dict[str, Any]) -> None:
        with self._connect() as con:
            con.execute(
                "UPDATE log_records SET metadata_json = ? WHERE id = ?",
                (json.dumps(metadata), log_id),
            )

    def get_record(self, log_type_id_or_log_id: str, log_id: str | None = None) -> LogRecord | None:
        if log_id is None:
            sql = "SELECT * FROM log_records WHERE id = ?"
            params: tuple[Any, ...] = (log_type_id_or_log_id,)
        else:
            sql = "SELECT * FROM log_records WHERE log_type = ? AND id = ?"
            params = (log_type_id_or_log_id, log_id)
        with self._connect() as con:
            row = con.execute(sql, params).fetchone()
        return self._record_from_row(row) if row else None

    def get_events(self, log_id: str) -> list[dict[str, Any]]:
        with self._connect() as con:
            rows = con.execute(
                """
                SELECT event_json FROM log_events
                WHERE log_id = ?
                ORDER BY COALESCE(row_id, id), id
                """,
                (log_id,),
            ).fetchall()
        return [_json_loads(row["event_json"], {}) for row in rows]

    def get_payload(self, log_type_id: str, log_id: str) -> dict[str, Any] | None:
        record = self.get_record(log_type_id, log_id)
        if not record:
            return None
        payload = dict(record.metadata.get("payload_header") or {})
        payload["events"] = self.get_events(log_id)
        if record.started_at:
            payload.setdefault("start", _format_dt(record.started_at))
        if record.ended_at:
            payload.setdefault("end", _format_dt(record.ended_at))
        return payload

    def list_records(
        self,
        log_type_id: str | None = None,
        page: int = 1,
        per_page: int = 20,
        search: str = "",
        *,
        source: str | None = None,
        status: str | None = None,
    ) -> tuple[list[LogRecord], int]:
        where = []
        params: list[Any] = []
        if log_type_id:
            where.append("log_type = ?")
            params.append(log_type_id)
        if source:
            where.append("source = ?")
            params.append(source)
        if status:
            where.append("status = ?")
            params.append(status)
        if search:
            where.append("(lower(name) LIKE ? OR lower(source) LIKE ? OR lower(status) LIKE ? OR lower(metadata_json) LIKE ?)")
            needle = f"%{search.lower()}%"
            params.extend([needle, needle, needle, needle])

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        page = max(1, int(page or 1))
        per_page = max(1, int(per_page or 20))
        offset = (page - 1) * per_page

        with self._connect() as con:
            total = int(con.execute(f"SELECT COUNT(*) AS count FROM log_records {where_sql}", params).fetchone()["count"])
            rows = con.execute(
                f"""
                SELECT * FROM log_records
                {where_sql}
                ORDER BY imported_at DESC
                LIMIT ? OFFSET ?
                """,
                [*params, per_page, offset],
            ).fetchall()
        return [self._record_from_row(row) for row in rows], total

    def count(self, log_type_id: str | None = None) -> int:
        _, total = self.list_records(log_type_id, per_page=1)
        return total

    def get_active_live_record(self, log_type_id: str | None = None) -> LogRecord | None:
        records, _ = self.list_records(log_type_id, source="live", status="active", per_page=1)
        return records[0] if records else None

    def delete(self, log_type_id_or_log_id: str, log_id: str | None = None) -> bool:
        record_id = log_id or log_type_id_or_log_id
        with self._connect() as con:
            cur = con.execute("DELETE FROM log_records WHERE id = ?", (record_id,))
        return cur.rowcount > 0

    def store(
        self,
        log_type_id: str,
        plugin_id: str,
        name: str,
        metadata: dict[str, Any],
        payload: dict[str, Any],
    ) -> LogRecord:
        header = {key: value for key, value in payload.items() if key != "events"} if isinstance(payload, dict) else {}
        record_metadata = dict(metadata or {})
        record_metadata.setdefault("payload_header", header)
        record_metadata["event_count"] = 0
        record = self.create_log(log_type_id, plugin_id, name, record_metadata)
        events = payload.get("events") if isinstance(payload, dict) else []
        if isinstance(events, list):
            self.append_events(record.id, [event for event in events if isinstance(event, dict)])
        return self.get_record(record.id) or record
