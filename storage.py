from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from flask import current_app, g


def get_db() -> sqlite3.Connection:
    """Application DB: user/auth tables only."""
    if "db" not in g:
        db_path = Path(current_app.config["DATABASE"])
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        g.db = conn
    return g.db


def close_db(_: Optional[BaseException] = None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def _table_exists(db: sqlite3.Connection, table: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return bool(row)


def _table_columns(db: sqlite3.Connection, table: str) -> List[str]:
    return [row["name"] for row in db.execute(f"PRAGMA table_info({table})")]


def _slugify(value: str) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in value.strip().lower())
    safe = "_".join(filter(None, safe.split("_")))
    return safe or "dataset"


def ensure_dataset_dir() -> Path:
    path = Path(current_app.config["DATASET_ROOT"])
    path.mkdir(parents=True, exist_ok=True)
    return path


def _migrate_logs_schema(conn: sqlite3.Connection) -> None:
    info = conn.execute("PRAGMA table_info(logs)").fetchall()
    col_names = [row["name"] for row in info]
    needs_recreate = not info or "boot_id" not in col_names or "id" not in col_names

    if needs_recreate:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS logs_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                boot_id TEXT NOT NULL,
                row_id INTEGER NOT NULL,
                name TEXT,
                description TEXT,
                color TEXT,
                system TEXT,
                subsystem TEXT,
                unit TEXT,
                code TEXT,
                set_clear TEXT,
                utctime TEXT,
                norm_time INTEGER,
                a_time INTEGER,
                b_time INTEGER,
                c_time INTEGER,
                d_time INTEGER,
                channels TEXT,
                data TEXT,
                event_id TEXT,
                tags TEXT,
                UNIQUE(boot_id, row_id)
            )
        """
        )
        if info:
            legacy_boot = "legacy"
            select_cols = []
            for col in [
                "row_id",
                "name",
                "description",
                "color",
                "system",
                "subsystem",
                "unit",
                "code",
                "set_clear",
                "utctime",
                "norm_time",
                "a_time",
                "b_time",
                "c_time",
                "d_time",
                "channels",
                "data",
                "event_id",
                "tags",
            ]:
                select_cols.append(col if col in col_names else f"NULL AS {col}")
            conn.execute(
                f"""
                INSERT INTO logs_new (
                    boot_id, row_id, name, description, color, system, subsystem, unit, code,
                    set_clear, utctime, norm_time, a_time, b_time, c_time, d_time,
                    channels, data, event_id, tags
                )
                SELECT ?, {", ".join(select_cols)}
                FROM logs
            """,
                (legacy_boot,),
            )
            conn.execute("DROP TABLE logs")
        conn.execute("ALTER TABLE logs_new RENAME TO logs")

    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_logs_boot_row ON logs(boot_id, row_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_boot ON logs(boot_id)")


def _init_dataset_db_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dataset_info (
            singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
            dataset_id INTEGER NOT NULL UNIQUE,
            name TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            owner_user_id INTEGER,
            log_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """
    )
    _migrate_logs_schema(conn)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS boots (
            boot_id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            event_count INTEGER NOT NULL
        )
    """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS log_index (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            boot_id TEXT NOT NULL,
            row_id INTEGER NOT NULL,
            system TEXT,
            event_id TEXT,
            tags TEXT
        )
    """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_log_index_boot ON log_index(boot_id)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bookmarks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            boot_id TEXT NOT NULL,
            row_id INTEGER NOT NULL,
            color_index INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(user_id, boot_id, row_id)
        )
    """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            boot_id TEXT NOT NULL,
            row_id INTEGER NOT NULL,
            parent_id INTEGER,
            body TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_comments_boot ON comments(boot_id)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS migration_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """
    )
    conn.commit()


def _dataset_files() -> List[Path]:
    return sorted(ensure_dataset_dir().glob("*.db"))


def _read_dataset_info(path: Path) -> Optional[Dict[str, Any]]:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    _init_dataset_db_schema(conn)
    row = conn.execute("SELECT * FROM dataset_info WHERE singleton_id = 1").fetchone()
    conn.close()
    if not row:
        return None
    return {
        "id": int(row["dataset_id"]),
        "name": row["name"],
        "description": row["description"] or "",
        "owner_user_id": row["owner_user_id"],
        "db_path": str(path),
        "log_count": int(row["log_count"] or 0),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _scan_datasets() -> List[Dict[str, Any]]:
    datasets = []
    for path in _dataset_files():
        meta = _read_dataset_info(path)
        if meta:
            datasets.append(meta)
    datasets.sort(key=lambda d: (d["name"].lower(), d["id"]))
    return datasets


def _next_dataset_id() -> int:
    existing = {d["id"] for d in _scan_datasets()}
    next_id = 1
    while next_id in existing:
        next_id += 1
    return next_id


def _set_dataset_info(conn: sqlite3.Connection, dataset: Dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO dataset_info (
            singleton_id, dataset_id, name, description, owner_user_id, log_count, created_at, updated_at
        ) VALUES (1, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(singleton_id)
        DO UPDATE SET
            dataset_id = excluded.dataset_id,
            name = excluded.name,
            description = excluded.description,
            owner_user_id = excluded.owner_user_id,
            log_count = excluded.log_count,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at
    """,
        (
            dataset["id"],
            dataset["name"],
            dataset.get("description", ""),
            dataset.get("owner_user_id"),
            dataset.get("log_count", 0),
            dataset["created_at"],
            dataset["updated_at"],
        ),
    )


def get_dataset_db(dataset: Dict[str, Any], attach_app_db: bool = True) -> sqlite3.Connection:
    """Open dataset DB and optionally ATTACH app DB for cross-db joins (e.g. users)."""
    path = Path(dataset["db_path"])
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    _init_dataset_db_schema(conn)
    if attach_app_db:
        app_db = str(Path(current_app.config["DATABASE"]))
        conn.execute("ATTACH DATABASE ? AS app_db", (app_db,))
    return conn


def _migrate_legacy_app_db(db: sqlite3.Connection) -> None:
    """One-way migration from old app-db dataset tables into per-dataset DB files."""
    if not _table_exists(db, "datasets"):
        return

    dataset_cols = _table_columns(db, "datasets")
    datasets = db.execute("SELECT * FROM datasets ORDER BY id").fetchall()
    root = ensure_dataset_dir()

    has_boots = _table_exists(db, "boots") and "dataset_id" in _table_columns(db, "boots")
    has_index = _table_exists(db, "log_index") and "dataset_id" in _table_columns(db, "log_index")
    has_bookmarks = _table_exists(db, "bookmarks") and "dataset_id" in _table_columns(db, "bookmarks")
    has_comments = _table_exists(db, "comments") and "dataset_id" in _table_columns(db, "comments")

    for row in datasets:
        dataset_id = int(row["id"])
        name = row["name"] if "name" in dataset_cols else f"Dataset {dataset_id}"
        description = row["description"] if "description" in dataset_cols else ""
        owner_user_id = row["owner_user_id"] if "owner_user_id" in dataset_cols else None
        created_at = row["created_at"] if "created_at" in dataset_cols else datetime.utcnow().isoformat()
        updated_at = row["updated_at"] if "updated_at" in dataset_cols else created_at
        log_count = int(row["log_count"] or 0) if "log_count" in dataset_cols else 0

        db_path_raw = row["db_path"] if "db_path" in dataset_cols else None
        if db_path_raw:
            dataset_path = Path(db_path_raw)
            if not dataset_path.is_absolute():
                dataset_path = root / dataset_path
        else:
            dataset_path = root / f"dataset{dataset_id}_{_slugify(name)}.db"

        conn = sqlite3.connect(dataset_path)
        conn.row_factory = sqlite3.Row
        _init_dataset_db_schema(conn)
        marker = conn.execute(
            "SELECT value FROM migration_state WHERE key = 'legacy_app_db_import_v1'"
        ).fetchone()
        if marker:
            conn.close()
            continue

        _set_dataset_info(
            conn,
            {
                "id": dataset_id,
                "name": name,
                "description": description or "",
                "owner_user_id": owner_user_id,
                "log_count": log_count,
                "created_at": created_at,
                "updated_at": updated_at,
            },
        )

        if has_boots:
            boots_rows = db.execute(
                "SELECT boot_id, created_at, event_count FROM boots WHERE dataset_id = ?",
                (dataset_id,),
            ).fetchall()
            conn.executemany(
                """
                INSERT OR REPLACE INTO boots (boot_id, created_at, event_count)
                VALUES (?, ?, ?)
            """,
                [(r["boot_id"], r["created_at"], r["event_count"]) for r in boots_rows],
            )

        if has_index:
            index_rows = db.execute(
                "SELECT boot_id, row_id, system, event_id, tags FROM log_index WHERE dataset_id = ?",
                (dataset_id,),
            ).fetchall()
            conn.executemany(
                """
                INSERT INTO log_index (boot_id, row_id, system, event_id, tags)
                VALUES (?, ?, ?, ?, ?)
            """,
                [
                    (r["boot_id"], r["row_id"], r["system"], r["event_id"], r["tags"])
                    for r in index_rows
                ],
            )

        if has_bookmarks:
            bookmark_rows = db.execute(
                """
                SELECT user_id, boot_id, row_id, color_index, created_at, updated_at
                FROM bookmarks
                WHERE dataset_id = ?
            """,
                (dataset_id,),
            ).fetchall()
            conn.executemany(
                """
                INSERT OR REPLACE INTO bookmarks
                    (user_id, boot_id, row_id, color_index, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                [
                    (
                        r["user_id"],
                        r["boot_id"],
                        r["row_id"],
                        r["color_index"],
                        r["created_at"],
                        r["updated_at"],
                    )
                    for r in bookmark_rows
                ],
            )

        if has_comments:
            comment_rows = db.execute(
                """
                SELECT id, user_id, boot_id, row_id, parent_id, body, created_at
                FROM comments
                WHERE dataset_id = ?
            """,
                (dataset_id,),
            ).fetchall()
            conn.executemany(
                """
                INSERT OR IGNORE INTO comments
                    (id, user_id, boot_id, row_id, parent_id, body, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    (
                        r["id"],
                        r["user_id"],
                        r["boot_id"],
                        r["row_id"],
                        r["parent_id"],
                        r["body"],
                        r["created_at"],
                    )
                    for r in comment_rows
                ],
            )

        log_total = conn.execute("SELECT COUNT(*) AS c FROM logs").fetchone()["c"]
        conn.execute(
            "UPDATE dataset_info SET log_count = ?, updated_at = ? WHERE singleton_id = 1",
            (int(log_total), datetime.utcnow().isoformat()),
        )
        conn.execute(
            "INSERT OR REPLACE INTO migration_state (key, value) VALUES ('legacy_app_db_import_v1', ?)",
            (datetime.utcnow().isoformat(),),
        )
        conn.commit()
        conn.close()


def init_db() -> None:
    db = get_db()
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            name TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_seen TEXT
        )
    """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS login_tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            token TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            used_at TEXT,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    """
    )
    db.commit()

    # Upgrade path from the old architecture. No new app-level dataset tables are created.
    _migrate_legacy_app_db(db)


def _row_to_user(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "id": row["id"],
        "email": row["email"],
        "name": row["name"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "last_seen": row["last_seen"],
    }


def get_user(user_id: int) -> Optional[Dict[str, Any]]:
    db = get_db()
    row = db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    return _row_to_user(row) if row else None


def get_or_create_user(email: str) -> Dict[str, Any]:
    db = get_db()
    row = db.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    now = datetime.utcnow().isoformat()
    if row:
        return _row_to_user(row)
    cursor = db.execute(
        "INSERT INTO users (email, name, created_at, updated_at, last_seen) VALUES (?, ?, ?, ?, ?)",
        (email, None, now, now, now),
    )
    db.commit()
    return get_user(cursor.lastrowid)


def update_user_name(user_id: int, name: str) -> None:
    db = get_db()
    now = datetime.utcnow().isoformat()
    db.execute("UPDATE users SET name = ?, updated_at = ? WHERE id = ?", (name, now, user_id))
    db.commit()


def update_user_last_seen(user_id: int, when: datetime) -> None:
    db = get_db()
    now = when.isoformat()
    db.execute("UPDATE users SET last_seen = ?, updated_at = ? WHERE id = ?", (now, now, user_id))
    db.commit()


def issue_login_token(user_id: int, ttl_minutes: int = 15) -> str:
    import secrets

    db = get_db()
    token = secrets.token_urlsafe(32)
    now = datetime.utcnow()
    expires_at = now + timedelta(minutes=ttl_minutes)
    db.execute(
        """
        INSERT INTO login_tokens (user_id, token, created_at, expires_at)
        VALUES (?, ?, ?, ?)
    """,
        (user_id, token, now.isoformat(), expires_at.isoformat()),
    )
    db.commit()
    return token


def consume_login_token(token: str) -> Optional[int]:
    db = get_db()
    now_iso = datetime.utcnow().isoformat()
    row = db.execute(
        """
        SELECT * FROM login_tokens
        WHERE token = ? AND used_at IS NULL AND expires_at >= ?
    """,
        (token, now_iso),
    ).fetchone()
    if not row:
        return None
    db.execute("UPDATE login_tokens SET used_at = ? WHERE id = ?", (now_iso, row["id"]))
    db.commit()
    return int(row["user_id"])


def get_dataset(dataset_id: int) -> Optional[Dict[str, Any]]:
    for dataset in _scan_datasets():
        if dataset["id"] == dataset_id:
            return dataset
    return None


def get_dataset_by_name(name: str, owner_user_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
    target = name.strip().lower()
    for dataset in _scan_datasets():
        if dataset["name"].strip().lower() != target:
            continue
        if owner_user_id is None and dataset.get("owner_user_id") is None:
            return dataset
        if owner_user_id is not None and dataset.get("owner_user_id") == owner_user_id:
            return dataset
    return None


def list_datasets(user_id: Optional[int] = None) -> List[Dict[str, Any]]:
    datasets = _scan_datasets()
    if user_id is None:
        return [d for d in datasets if d.get("owner_user_id") is None]
    return [d for d in datasets if d.get("owner_user_id") is None or d.get("owner_user_id") == user_id]


def get_first_dataset(user_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
    datasets = list_datasets(user_id)
    return datasets[0] if datasets else None


def create_dataset(name: str, description: str = "", owner_user_id: Optional[int] = None) -> Dict[str, Any]:
    now = datetime.utcnow().isoformat()
    dataset_id = _next_dataset_id()
    root = ensure_dataset_dir()
    path = root / f"dataset{dataset_id}_{_slugify(name)}.db"
    dataset = {
        "id": dataset_id,
        "name": name,
        "description": description,
        "owner_user_id": owner_user_id,
        "db_path": str(path),
        "log_count": 0,
        "created_at": now,
        "updated_at": now,
    }
    conn = get_dataset_db(dataset, attach_app_db=False)
    _set_dataset_info(conn, dataset)
    conn.commit()
    conn.close()
    return dataset


def delete_dataset(dataset_id: int) -> None:
    dataset = get_dataset(dataset_id)
    if not dataset:
        return
    try:
        path = Path(dataset["db_path"])
        if path.exists():
            path.unlink()
    except OSError:
        pass


def parse_events_from_upload(file_storage) -> List[Dict[str, Any]]:
    raw = file_storage.read()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return []
    events = []
    if isinstance(payload, dict) and isinstance(payload.get("events"), list):
        events = payload["events"]
    elif isinstance(payload, list):
        events = payload
    cleaned = []
    now = datetime.utcnow()
    for idx, item in enumerate(events):
        if not isinstance(item, dict):
            continue
        row_id = item.get("row_id") or idx + 1
        utctime = item.get("utctime") or (now + timedelta(seconds=idx)).isoformat() + "Z"
        norm_time = item.get("norm_time") or idx
        channels = item.get("channels") or []
        tags = item.get("tags") or item.get("labels") or []
        event_id = item.get("event_id") or item.get("eventid")
        cleaned.append(
            {
                "row_id": int(row_id),
                "name": item.get("name", f"Event {row_id}"),
                "description": item.get("description", ""),
                "color": item.get("color", "Green"),
                "system": item.get("system", "Unknown"),
                "subsystem": item.get("subsystem", ""),
                "unit": item.get("unit", ""),
                "code": item.get("code", ""),
                "set_clear": item.get("set_clear", "set"),
                "utctime": utctime,
                "norm_time": int(norm_time),
                "a_time": item.get("a_time"),
                "b_time": item.get("b_time"),
                "c_time": item.get("c_time"),
                "d_time": item.get("d_time"),
                "channels": channels,
                "data": item.get("data"),
                "event_id": event_id or "",
                "tags": tags,
            }
        )
    return cleaned


def insert_events_into_dataset(dataset: Dict[str, Any], events: List[Dict[str, Any]]) -> str:
    import secrets

    if not events:
        return ""
    boot_id = secrets.token_urlsafe(8)
    now_iso = datetime.utcnow().isoformat()
    conn = get_dataset_db(dataset, attach_app_db=False)
    conn.executemany(
        """
        INSERT OR REPLACE INTO logs (
            boot_id, row_id, name, description, color, system, subsystem, unit, code, set_clear,
            utctime, norm_time, a_time, b_time, c_time, d_time, channels, data, event_id, tags
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        [
            (
                boot_id,
                e["row_id"],
                e["name"],
                e["description"],
                e["color"],
                e["system"],
                e["subsystem"],
                e["unit"],
                e["code"],
                e["set_clear"],
                e["utctime"],
                e["norm_time"],
                e["a_time"],
                e["b_time"],
                e["c_time"],
                e["d_time"],
                json.dumps(e["channels"]),
                json.dumps(e["data"]),
                e["event_id"],
                ",".join(e["tags"]) if isinstance(e["tags"], list) else str(e["tags"] or ""),
            )
            for e in events
        ],
    )

    conn.execute(
        "INSERT OR REPLACE INTO boots (boot_id, created_at, event_count) VALUES (?, ?, ?)",
        (boot_id, now_iso, len(events)),
    )
    conn.executemany(
        """
        INSERT INTO log_index (boot_id, row_id, system, event_id, tags)
        VALUES (?, ?, ?, ?, ?)
    """,
        [
            (
                boot_id,
                e["row_id"],
                e["system"],
                e["event_id"],
                ",".join(e["tags"]) if isinstance(e["tags"], list) else str(e["tags"] or ""),
            )
            for e in events
        ],
    )
    total_logs = int(conn.execute("SELECT COUNT(*) AS c FROM logs").fetchone()["c"])
    conn.execute(
        "UPDATE dataset_info SET log_count = ?, updated_at = ? WHERE singleton_id = 1",
        (total_logs, now_iso),
    )
    conn.commit()
    conn.close()
    return boot_id


def _latest_boot_id(conn: sqlite3.Connection) -> Optional[str]:
    row = conn.execute(
        "SELECT boot_id FROM boots ORDER BY datetime(created_at) DESC LIMIT 1"
    ).fetchone()
    return row["boot_id"] if row else None


def list_boots_for_dataset(dataset_id: int) -> List[Dict[str, Any]]:
    dataset = get_dataset(dataset_id)
    if not dataset:
        return []
    conn = get_dataset_db(dataset, attach_app_db=False)
    rows = conn.execute(
        "SELECT boot_id, created_at, event_count FROM boots ORDER BY datetime(created_at) DESC"
    ).fetchall()
    conn.close()
    return [
        {
            "boot_id": r["boot_id"],
            "created_at": r["created_at"],
            "event_count": int(r["event_count"]),
        }
        for r in rows
    ]


def load_log_data_from_dataset(dataset: Dict[str, Any], boot_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    path = Path(dataset["db_path"])
    if not path.exists():
        return None

    conn = get_dataset_db(dataset, attach_app_db=False)
    target_boot = boot_id or _latest_boot_id(conn)
    if not target_boot:
        row = conn.execute("SELECT boot_id FROM logs ORDER BY id DESC LIMIT 1").fetchone()
        target_boot = row["boot_id"] if row else None
    if not target_boot:
        conn.close()
        return None

    rows = conn.execute("SELECT * FROM logs WHERE boot_id = ? ORDER BY row_id", (target_boot,)).fetchall()
    conn.close()

    events = []
    start_ts = None
    end_ts = None
    for row in rows:
        event_time = row["utctime"]
        try:
            parsed = datetime.fromisoformat(event_time.replace("Z", ""))
        except Exception:
            parsed = None
        if parsed:
            start_ts = parsed if start_ts is None else min(start_ts, parsed)
            end_ts = parsed if end_ts is None else max(end_ts, parsed)

        events.append(
            {
                "row_id": row["row_id"],
                "name": row["name"],
                "description": row["description"],
                "color": row["color"],
                "system": row["system"],
                "subsystem": row["subsystem"],
                "unit": row["unit"],
                "code": row["code"],
                "set_clear": row["set_clear"],
                "utctime": row["utctime"],
                "norm_time": row["norm_time"],
                "a_time": row["a_time"],
                "b_time": row["b_time"],
                "c_time": row["c_time"],
                "d_time": row["d_time"],
                "channels": json.loads(row["channels"] or "[]"),
                "data": json.loads(row["data"] or "null"),
                "event_id": row["event_id"],
                "tags": (row["tags"] or "").split(",") if row["tags"] else [],
            }
        )

    if not events:
        return None

    start_value = start_ts or datetime.utcnow()
    end_value = end_ts or start_value
    return {
        "start": start_value.isoformat(timespec="seconds") + "Z",
        "end": end_value.isoformat(timespec="seconds") + "Z",
        "hours": (end_value - start_value).total_seconds() / 3600,
        "seed": None,
        "events": events,
        "modes": [],
        "boot_id": target_boot,
        "dataset_id": dataset["id"],
    }


def get_boot_meta(dataset_id: int, boot_id: str) -> Optional[Dict[str, Any]]:
    dataset = get_dataset(dataset_id)
    if not dataset:
        return None
    conn = get_dataset_db(dataset, attach_app_db=False)
    row = conn.execute(
        "SELECT boot_id, created_at, event_count FROM boots WHERE boot_id = ?",
        (boot_id,),
    ).fetchone()
    conn.close()
    if not row:
        return None
    return {
        "dataset_id": dataset_id,
        "boot_id": row["boot_id"],
        "created_at": row["created_at"],
        "event_count": int(row["event_count"]),
    }


def get_boot_details(dataset: Dict[str, Any], boot_id: str) -> Dict[str, str]:
    conn = get_dataset_db(dataset, attach_app_db=False)
    row = conn.execute(
        "SELECT system, event_id, tags FROM logs WHERE boot_id = ? LIMIT 1",
        (boot_id,),
    ).fetchone()
    conn.close()
    return {
        "system": row["system"] if row else "",
        "event_id": row["event_id"] if row else "",
        "tags": row["tags"] if row else "",
    }


def update_boot_metadata(dataset: Dict[str, Any], boot_id: str, system: str, event_id: str, tags: List[str]) -> None:
    tags_str = ",".join(tags)
    conn = get_dataset_db(dataset, attach_app_db=False)
    conn.execute(
        "UPDATE logs SET system = ?, event_id = ?, tags = ? WHERE boot_id = ?",
        (system, event_id, tags_str, boot_id),
    )
    rows = conn.execute(
        "SELECT row_id, system, event_id, tags FROM logs WHERE boot_id = ?",
        (boot_id,),
    ).fetchall()
    conn.execute("DELETE FROM log_index WHERE boot_id = ?", (boot_id,))
    conn.executemany(
        """
        INSERT INTO log_index (boot_id, row_id, system, event_id, tags)
        VALUES (?, ?, ?, ?, ?)
    """,
        [(boot_id, r["row_id"], r["system"], r["event_id"], r["tags"]) for r in rows],
    )
    conn.commit()
    conn.close()


def list_bookmarks_for_user(user_id: int, dataset_id: int, boot_id: str) -> Dict[str, int]:
    dataset = get_dataset(dataset_id)
    if not dataset:
        return {}
    conn = get_dataset_db(dataset, attach_app_db=False)
    rows = conn.execute(
        """
        SELECT row_id, color_index
        FROM bookmarks
        WHERE user_id = ? AND boot_id = ?
        ORDER BY row_id
    """,
        (user_id, boot_id),
    ).fetchall()
    conn.close()
    return {str(r["row_id"]): int(r["color_index"]) for r in rows}


def set_bookmark(user_id: int, dataset_id: int, boot_id: str, row_id: int, color_index: int) -> None:
    dataset = get_dataset(dataset_id)
    if not dataset:
        return
    now = datetime.utcnow().isoformat()
    conn = get_dataset_db(dataset, attach_app_db=False)
    if color_index <= 0:
        conn.execute(
            "DELETE FROM bookmarks WHERE user_id = ? AND boot_id = ? AND row_id = ?",
            (user_id, boot_id, row_id),
        )
        conn.commit()
        conn.close()
        return

    conn.execute(
        """
        INSERT INTO bookmarks (user_id, boot_id, row_id, color_index, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id, boot_id, row_id)
        DO UPDATE SET color_index = excluded.color_index, updated_at = excluded.updated_at
    """,
        (user_id, boot_id, row_id, color_index, now, now),
    )
    conn.commit()
    conn.close()


def list_comments_for_boot(dataset_id: int, boot_id: str) -> List[Dict[str, Any]]:
    dataset = get_dataset(dataset_id)
    if not dataset:
        return []
    conn = get_dataset_db(dataset, attach_app_db=True)
    rows = conn.execute(
        """
        SELECT c.id, c.row_id, c.parent_id, c.body, c.created_at,
               u.id AS user_id, u.name AS user_name, u.email AS user_email
        FROM comments c
        LEFT JOIN app_db.users u ON u.id = c.user_id
        WHERE c.boot_id = ?
        ORDER BY datetime(c.created_at) ASC, c.id ASC
    """,
        (boot_id,),
    ).fetchall()
    conn.close()
    return [
        {
            "id": r["id"],
            "row_id": r["row_id"],
            "parent_id": r["parent_id"],
            "body": r["body"],
            "created_at": r["created_at"],
            "user_id": r["user_id"],
            "user_name": r["user_name"],
            "user_email": r["user_email"],
        }
        for r in rows
    ]


def create_comment(
    user_id: int,
    dataset_id: int,
    boot_id: str,
    row_id: int,
    body: str,
    parent_id: Optional[int] = None,
) -> Dict[str, Any]:
    dataset = get_dataset(dataset_id)
    if not dataset:
        raise ValueError("dataset_not_found")

    now = datetime.utcnow().isoformat()
    conn = get_dataset_db(dataset, attach_app_db=True)

    parent_valid = None
    if parent_id is not None:
        parent = conn.execute(
            "SELECT id, row_id FROM comments WHERE id = ? AND boot_id = ?",
            (parent_id, boot_id),
        ).fetchone()
        if not parent:
            conn.close()
            raise ValueError("invalid_parent")
        if int(parent["row_id"]) != int(row_id):
            conn.close()
            raise ValueError("parent_row_mismatch")
        parent_valid = parent["id"]

    cursor = conn.execute(
        """
        INSERT INTO comments (user_id, boot_id, row_id, parent_id, body, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        (user_id, boot_id, row_id, parent_valid, body, now),
    )
    row = conn.execute(
        """
        SELECT c.id, c.row_id, c.parent_id, c.body, c.created_at,
               u.id AS user_id, u.name AS user_name, u.email AS user_email
        FROM comments c
        LEFT JOIN app_db.users u ON u.id = c.user_id
        WHERE c.id = ?
    """,
        (cursor.lastrowid,),
    ).fetchone()
    conn.commit()
    conn.close()

    return {
        "id": row["id"],
        "row_id": row["row_id"],
        "parent_id": row["parent_id"],
        "body": row["body"],
        "created_at": row["created_at"],
        "user_id": row["user_id"],
        "user_name": row["user_name"],
        "user_email": row["user_email"],
    }
