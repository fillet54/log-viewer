from __future__ import annotations

import json
import secrets
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from flask import current_app, g


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        db_path = current_app.config["DATABASE"]
        db_dir = Path(db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        g.db = conn
    return g.db


def close_db(_: Optional[BaseException] = None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


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
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS datasets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL
        )
    """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS shards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dataset_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            db_path TEXT NOT NULL,
            log_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            UNIQUE(dataset_id, name),
            FOREIGN KEY (dataset_id) REFERENCES datasets (id)
        )
    """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS log_index (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shard_id INTEGER NOT NULL,
            boot_id TEXT NOT NULL,
            row_id INTEGER NOT NULL,
            system TEXT,
            event_id TEXT,
            tags TEXT,
            FOREIGN KEY (shard_id) REFERENCES shards (id)
        )
    """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS boots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shard_id INTEGER NOT NULL,
            boot_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            event_count INTEGER NOT NULL,
            UNIQUE(boot_id),
            FOREIGN KEY (shard_id) REFERENCES shards (id)
        )
    """
    )
    cols = [row["name"] for row in db.execute("PRAGMA table_info(log_index)")]
    if "boot_id" not in cols:
        db.execute("ALTER TABLE log_index ADD COLUMN boot_id TEXT")
    db.commit()


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
    db.execute(
        "UPDATE users SET last_seen = ?, updated_at = ? WHERE id = ?",
        (when.isoformat(), when.isoformat(), user_id),
    )
    db.commit()


def issue_login_token(user_id: int, ttl_minutes: int = 15) -> str:
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


def _slugify(value: str) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in value.strip().lower())
    safe = "_".join(filter(None, safe.split("_")))
    return safe or "shard"


def ensure_shard_dir() -> Path:
    path = Path(current_app.config["SHARD_ROOT"])
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_dataset_by_name(name: str) -> Optional[Dict[str, Any]]:
    db = get_db()
    row = db.execute("SELECT * FROM datasets WHERE name = ?", (name,)).fetchone()
    if not row:
        return None
    return {"id": row["id"], "name": row["name"], "created_at": row["created_at"]}


def create_dataset(name: str) -> Dict[str, Any]:
    db = get_db()
    now = datetime.utcnow().isoformat()
    cursor = db.execute("INSERT INTO datasets (name, created_at) VALUES (?, ?)", (name, now))
    db.commit()
    return {"id": cursor.lastrowid, "name": name, "created_at": now}


def get_dataset(dataset_id: int) -> Optional[Dict[str, Any]]:
    db = get_db()
    row = db.execute("SELECT * FROM datasets WHERE id = ?", (dataset_id,)).fetchone()
    if not row:
        return None
    return {"id": row["id"], "name": row["name"], "created_at": row["created_at"]}


def list_datasets() -> List[Dict[str, Any]]:
    db = get_db()
    rows = db.execute("SELECT * FROM datasets ORDER BY name").fetchall()
    return [{"id": r["id"], "name": r["name"], "created_at": r["created_at"]} for r in rows]


def get_shards_for_dataset(dataset_id: int) -> List[Dict[str, Any]]:
    db = get_db()
    rows = db.execute(
        "SELECT * FROM shards WHERE dataset_id = ? ORDER BY name", (dataset_id,)
    ).fetchall()
    return [
        {
            "id": r["id"],
            "dataset_id": r["dataset_id"],
            "name": r["name"],
            "db_path": r["db_path"],
            "log_count": r["log_count"],
            "created_at": r["created_at"],
        }
        for r in rows
    ]


def get_shard(shard_id: int) -> Optional[Dict[str, Any]]:
    db = get_db()
    row = db.execute("SELECT * FROM shards WHERE id = ?", (shard_id,)).fetchone()
    if not row:
        return None
    return {
        "id": row["id"],
        "dataset_id": row["dataset_id"],
        "name": row["name"],
        "db_path": row["db_path"],
        "log_count": row["log_count"],
        "created_at": row["created_at"],
    }


def create_shard(dataset_id: int, name: str) -> Dict[str, Any]:
    ensure_shard_dir()
    db = get_db()
    slug = _slugify(name)
    now = datetime.utcnow().isoformat()
    shard_path = Path(current_app.config["SHARD_ROOT"]) / f"dataset{dataset_id}_{slug}.db"
    cursor = db.execute(
        "INSERT INTO shards (dataset_id, name, db_path, log_count, created_at) VALUES (?, ?, ?, ?, ?)",
        (dataset_id, name, str(shard_path), 0, now),
    )
    db.commit()
    return get_shard(cursor.lastrowid)


def delete_shard(shard_id: int) -> None:
    shard = get_shard(shard_id)
    if not shard:
        return
    db = get_db()
    db.execute("DELETE FROM boots WHERE shard_id = ?", (shard_id,))
    db.execute("DELETE FROM log_index WHERE shard_id = ?", (shard_id,))
    db.execute("DELETE FROM shards WHERE id = ?", (shard_id,))
    db.commit()
    try:
        path = Path(shard["db_path"])
        if path.exists():
            path.unlink()
    except OSError:
        pass


def get_first_shard() -> Optional[Dict[str, Any]]:
    db = get_db()
    row = db.execute("SELECT * FROM shards ORDER BY created_at LIMIT 1").fetchone()
    if not row:
        return None
    return get_shard(row["id"])


def migrate_shard_schema(conn: sqlite3.Connection) -> None:
    info = conn.execute("PRAGMA table_info(logs)").fetchall()
    col_names = [row["name"] for row in info]
    needs_recreate = False
    if not info:
        needs_recreate = True
    else:
        needs_recreate = "boot_id" not in col_names or "id" not in col_names

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
            existing_cols = ", ".join([c for c in col_names if c != "rowid"])
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
                if col in col_names:
                    select_cols.append(col)
                else:
                    select_cols.append(f"NULL AS {col}")
            conn.execute(
                f"""
                INSERT INTO logs_new (
                    boot_id, row_id, name, description, color, system, subsystem, unit, code,
                    set_clear, utctime, norm_time, a_time, b_time, c_time, d_time, channels,
                    data, event_id, tags
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
    conn.commit()


def ensure_shard_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    migrate_shard_schema(conn)
    return conn


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


def insert_events_into_shard(shard: Dict[str, Any], events: List[Dict[str, Any]]) -> str:
    if not events:
        return ""
    boot_id = secrets.token_urlsafe(8)
    now_iso = datetime.utcnow().isoformat()
    conn = ensure_shard_db(Path(shard["db_path"]))
    cursor = conn.cursor()
    cursor.executemany(
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
    conn.commit()
    conn.close()

    db = get_db()
    conn_total = ensure_shard_db(Path(shard["db_path"]))
    total_logs = conn_total.execute("SELECT COUNT(*) FROM logs").fetchone()[0]
    conn_total.close()
    db.execute("UPDATE shards SET log_count = ? WHERE id = ?", (total_logs, shard["id"]))
    db.execute(
        """
        INSERT OR REPLACE INTO boots (shard_id, boot_id, created_at, event_count)
        VALUES (?, ?, ?, ?)
    """,
        (shard["id"], boot_id, now_iso, len(events)),
    )
    db.executemany(
        """
        INSERT INTO log_index (shard_id, boot_id, row_id, system, event_id, tags)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        [
            (
                shard["id"],
                boot_id,
                e["row_id"],
                e["system"],
                e["event_id"],
                ",".join(e["tags"]) if isinstance(e["tags"], list) else str(e["tags"] or ""),
            )
            for e in events
        ],
    )
    db.commit()
    return boot_id


def get_latest_boot_id(shard_id: int) -> Optional[str]:
    db = get_db()
    row = db.execute(
        "SELECT boot_id FROM boots WHERE shard_id = ? ORDER BY datetime(created_at) DESC LIMIT 1",
        (shard_id,),
    ).fetchone()
    if row:
        return row["boot_id"]
    return None


def list_boots_for_shard(shard_id: int) -> List[Dict[str, Any]]:
    db = get_db()
    rows = db.execute(
        "SELECT * FROM boots WHERE shard_id = ? ORDER BY datetime(created_at) DESC", (shard_id,)
    ).fetchall()
    return [
        {
            "boot_id": r["boot_id"],
            "created_at": r["created_at"],
            "event_count": r["event_count"],
        }
        for r in rows
    ]


def load_log_data_from_shard(shard: Dict[str, Any], boot_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    path = Path(shard["db_path"])
    if not path.exists():
        return None
    conn = ensure_shard_db(path)
    target_boot = boot_id or get_latest_boot_id(shard["id"])
    if not target_boot:
        row = conn.execute("SELECT boot_id FROM logs ORDER BY id DESC LIMIT 1").fetchone()
        target_boot = row["boot_id"] if row else None
    if not target_boot:
        conn.close()
        return None
    rows = conn.execute(
        "SELECT * FROM logs WHERE boot_id = ? ORDER BY row_id", (target_boot,)
    ).fetchall()
    conn.close()
    events = []
    start_ts = None
    end_ts = None
    for row in rows:
        event_time = row["utctime"]
        try:
            parsed_time = datetime.fromisoformat(event_time.replace("Z", ""))
        except Exception:
            parsed_time = None
        if parsed_time:
            start_ts = parsed_time if start_ts is None else min(start_ts, parsed_time)
            end_ts = parsed_time if end_ts is None else max(end_ts, parsed_time)
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
        "hours": (end_value - start_value).total_seconds() / 3600 if start_value and end_value else 0,
        "seed": None,
        "events": events,
        "modes": [],
        "boot_id": target_boot,
    }


def get_boot_meta(shard_id: int, boot_id: str) -> Optional[Dict[str, Any]]:
    db = get_db()
    row = db.execute(
        "SELECT * FROM boots WHERE shard_id = ? AND boot_id = ?", (shard_id, boot_id)
    ).fetchone()
    if not row:
        return None
    return {
        "shard_id": row["shard_id"],
        "boot_id": row["boot_id"],
        "created_at": row["created_at"],
        "event_count": row["event_count"],
    }


def get_boot_details(shard: Dict[str, Any], boot_id: str) -> Dict[str, str]:
    conn = ensure_shard_db(Path(shard["db_path"]))
    sample = conn.execute(
        "SELECT system, event_id, tags FROM logs WHERE boot_id = ? LIMIT 1", (boot_id,)
    ).fetchone()
    conn.close()
    return {
        "system": sample["system"] if sample else "",
        "event_id": sample["event_id"] if sample else "",
        "tags": sample["tags"] if sample else "",
    }


def update_boot_metadata(shard: Dict[str, Any], boot_id: str, system: str, event_id: str, tags: List[str]) -> None:
    conn = ensure_shard_db(Path(shard["db_path"]))
    tags_str = ",".join(tags)
    conn.execute(
        "UPDATE logs SET system = ?, event_id = ?, tags = ? WHERE boot_id = ?",
        (system, event_id, tags_str, boot_id),
    )
    conn.commit()
    rows = conn.execute(
        "SELECT row_id, system, event_id, tags FROM logs WHERE boot_id = ?", (boot_id,)
    ).fetchall()
    conn.close()

    db = get_db()
    db.execute("DELETE FROM log_index WHERE shard_id = ? AND boot_id = ?", (shard["id"], boot_id))
    db.executemany(
        """
        INSERT INTO log_index (shard_id, boot_id, row_id, system, event_id, tags)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        [
            (
                shard["id"],
                boot_id,
                r["row_id"],
                r["system"],
                r["event_id"],
                r["tags"],
            )
            for r in rows
        ],
    )
    db.commit()
