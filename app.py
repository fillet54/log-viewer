from datetime import datetime, timedelta
import os
import random
import secrets
import sqlite3
import json
from pathlib import Path
from typing import Any, Dict, Optional, List
from flask import (
    Flask,
    Response,
    flash,
    g,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

app = Flask(__name__)
app.secret_key = os.environ.get("LOG_VIEWER_SECRET", "dev-secret-key-change-me")
app.permanent_session_lifetime = timedelta(days=30)
app.config["DATABASE"] = os.environ.get(
    "LOG_VIEWER_DB", os.path.join(app.root_path, "log_viewer.db")
)
app.config["SHARD_ROOT"] = os.environ.get(
    "LOG_VIEWER_SHARDS", os.path.join(app.root_path, "data", "shards")
)

FAULT_CATALOG = [
    {
        "id": "pwr_bus",
        "name": "Power Bus Drift",
        "description": "Voltage drift detected on primary bus.",
        "color": "Yellow",
        "system": "Power",
        "subsystem": "Distribution",
        "unit": "PDU-1",
        "code": "PWR-214",
        "data": {
            "voltage": {"min": 26.8, "max": 28.4, "units": "V"},
            "bus": {"id": "A", "load_pct": 62},
        },
    },
    {
        "id": "temp_core",
        "name": "Core Temp Spike",
        "description": "Thermal threshold exceeded on core stack.",
        "color": "Red",
        "system": "Thermal",
        "subsystem": "Cooling",
        "unit": "FAN-3",
        "code": "THM-501",
        "data": {
            "temp_c": {"current": 92.4, "limit": 85.0},
            "sensor": {"id": "core-7", "status": "latched"},
        },
    },
    {
        "id": "link_loss",
        "name": "Link Loss",
        "description": "Packet loss above tolerance.",
        "color": "Yellow",
        "system": "Network",
        "subsystem": "Backplane",
        "unit": "SW-2",
        "code": "NET-118",
    },
    {
        "id": "db_timeout",
        "name": "DB Timeout",
        "description": "Query timeout exceeded 2000ms.",
        "color": "Red",
        "system": "Storage",
        "subsystem": "Database",
        "unit": "DB-1",
        "code": "DB-907",
        "data": {
            "query": {"id": "q-1842", "duration_ms": 2412},
            "host": {"name": "db-primary", "pool": "writer"},
        },
    },
    {
        "id": "sensor_glitch",
        "name": "Sensor Glitch",
        "description": "Transient sensor anomaly detected.",
        "color": "Green",
        "system": "Telemetry",
        "subsystem": "Sensors",
        "unit": "SEN-9",
        "code": "TEL-033",
    },
    {
        "id": "auth_fail",
        "name": "Auth Failure",
        "description": "Repeated authentication failure.",
        "color": "Yellow",
        "system": "Security",
        "subsystem": "Auth",
        "unit": "AUTH-2",
        "code": "SEC-201",
        "data": {
            "user": {"id": "svc-ingest", "attempts": 5},
            "source": {"ip": "10.24.1.18", "zone": "dmz"},
        },
    },
    {
        "id": "queue_lag",
        "name": "Queue Lag",
        "description": "Ingestion queue lag above threshold.",
        "color": "Green",
        "system": "Ingest",
        "subsystem": "Queue",
        "unit": "Q-4",
        "code": "ING-047",
    },
    {
        "id": "ctrl_fault",
        "name": "Control Fault",
        "description": "Control loop instability detected.",
        "color": "Flashing Red",
        "system": "Control",
        "subsystem": "Stability",
        "unit": "CTRL-1",
        "code": "CTL-888",
        "data": {
            "loop": {"axis": "yaw", "gain": 1.8},
            "error": {"rms": 0.42, "limit": 0.25},
        },
    },
    {
        "id": "mem_warn",
        "name": "Memory Pressure",
        "description": "Memory usage above 85%.",
        "color": "Yellow",
        "system": "Compute",
        "subsystem": "Memory",
        "unit": "CPU-2",
        "code": "CMP-312",
    },
    {
        "id": "disk_slow",
        "name": "Disk Slowdown",
        "description": "I/O latency above baseline.",
        "color": "Green",
        "system": "Storage",
        "subsystem": "IO",
        "unit": "DSK-7",
        "code": "STO-119",
    },
]

LEVEL_ORDER = ["Green", "Yellow", "Red", "Flashing Red"]


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        db_path = app.config["DATABASE"]
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
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
    # Ensure new columns exist after previous versions.
    cols = [row["name"] for row in db.execute("PRAGMA table_info(log_index)")]
    if "boot_id" not in cols:
        db.execute("ALTER TABLE log_index ADD COLUMN boot_id TEXT")
    db.commit()
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


def _slugify(value: str) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in value.strip().lower())
    safe = "_".join(filter(None, safe.split("_")))
    return safe or "shard"


def ensure_shard_dir() -> Path:
    path = Path(app.config["SHARD_ROOT"])
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
    cursor = db.execute(
        "INSERT INTO datasets (name, created_at) VALUES (?, ?)", (name, now)
    )
    db.commit()
    return {"id": cursor.lastrowid, "name": name, "created_at": now}


def get_dataset(dataset_id: int) -> Optional[Dict[str, Any]]:
    db = get_db()
    row = db.execute("SELECT * FROM datasets WHERE id = ?", (dataset_id,)).fetchone()
    if not row:
        return None
    return {"id": row["id"], "name": row["name"], "created_at": row["created_at"]}


def list_datasets() -> list[Dict[str, Any]]:
    db = get_db()
    rows = db.execute("SELECT * FROM datasets ORDER BY name").fetchall()
    return [{"id": r["id"], "name": r["name"], "created_at": r["created_at"]} for r in rows]


def get_shards_for_dataset(dataset_id: int) -> list[Dict[str, Any]]:
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
    shard_path = Path(app.config["SHARD_ROOT"]) / f"dataset{dataset_id}_{slug}.db"
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
    # Ensure logs table exists with boot_id support.
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
            # migrate existing rows with legacy boot id
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
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_logs_boot_row ON logs(boot_id, row_id)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_boot ON logs(boot_id)")
    conn.commit()


def ensure_shard_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    migrate_shard_schema(conn)
    return conn


def parse_events_from_upload(file_storage) -> list[Dict[str, Any]]:
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


def insert_events_into_shard(shard: Dict[str, Any], events: list[Dict[str, Any]]) -> str:
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


def get_user(user_id: int) -> Optional[Dict[str, Any]]:
    db = get_db()
    row = db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    return _row_to_user(row) if row else None


def get_or_create_user(email: str) -> Dict[str, Any]:
    db = get_db()
    row = db.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    now = datetime.utcnow().isoformat()
    if row:
        user = _row_to_user(row)
        return user
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
    db.execute(
        "UPDATE login_tokens SET used_at = ? WHERE id = ?", (now_iso, row["id"])
    )
    db.commit()
    return int(row["user_id"])


def login_user(user_id: int) -> None:
    session.clear()
    session.permanent = True
    now = datetime.utcnow().isoformat()
    session["user_id"] = user_id
    session["last_seen"] = now
    update_user_last_seen(user_id, datetime.fromisoformat(now))


@app.teardown_appcontext
def teardown_db(error: Optional[BaseException] = None) -> None:
    close_db(error)


@app.context_processor
def inject_user() -> Dict[str, Any]:
    return {"current_user": getattr(g, "current_user", None)}


@app.before_request
def load_user() -> None:
    init_db()
    g.current_user = None
    user_id = session.get("user_id")
    last_seen_str = session.get("last_seen")
    if not user_id:
        return
    now = datetime.utcnow()
    last_seen = None
    try:
        last_seen = datetime.fromisoformat(last_seen_str) if last_seen_str else None
    except (TypeError, ValueError):
        last_seen = None

    if last_seen and now - last_seen > timedelta(days=30):
        session.clear()
        return

    user = get_user(int(user_id))
    if not user:
        session.clear()
        return

    g.current_user = user
    needs_refresh = last_seen is None or now - last_seen >= timedelta(hours=12)
    if needs_refresh:
        session["last_seen"] = now.isoformat()
        session.modified = True
        update_user_last_seen(user["id"], now)

def _seed_to_int(seed_value: Optional[str]) -> int:
    if seed_value is None or seed_value == "":
        return 1
    try:
        return int(seed_value)
    except ValueError:
        return abs(hash(seed_value)) % (10**9)


def _level_weight(color: str, cluster_weight: float) -> float:
    if color == "Green":
        return 1.2
    if color == "Yellow":
        return 0.9 + cluster_weight * 1.6
    if color == "Red":
        return 0.6 + cluster_weight * 1.8
    return 0.3 + cluster_weight * 2.0


def generate_logs(hours: float, seed_value: Optional[str]):
    seed_int = _seed_to_int(seed_value)
    rng = random.Random(seed_int)
    total_events = max(1, int(hours * 500))
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=hours)
    span_seconds = int(hours * 3600)

    if span_seconds <= 0:
        span_seconds = 3600

    cluster_centers = [rng.uniform(0, span_seconds) for _ in range(max(2, int(hours)))]

    def cluster_weight(at_seconds: float) -> float:
        if not cluster_centers:
            return 0.0
        nearest = min(abs(at_seconds - center) for center in cluster_centers)
        return max(0.0, 1.0 - (nearest / (span_seconds / 6)))

    states = {item["id"]: False for item in FAULT_CATALOG}
    events = []
    time_offsets = sorted(rng.uniform(0, span_seconds) for _ in range(total_events))

    for idx, offset in enumerate(time_offsets):
        weight = cluster_weight(offset)
        weights = [_level_weight(item["color"], weight) for item in FAULT_CATALOG]
        choice = rng.choices(FAULT_CATALOG, weights=weights, k=1)[0]
        is_set = states[choice["id"]]
        set_clear = "clear" if is_set else "set"
        states[choice["id"]] = not is_set

        timestamp = start_time + timedelta(seconds=offset)
        channels = ["A", "B", "C", "D"]
        if rng.random() < 0.85:
            seen_channels = channels
        else:
            seen_channels = rng.sample(channels, rng.randint(1, 3))

        def _channel_time(letter: str) -> Optional[int]:
            if letter not in seen_channels:
                return None
            jitter = rng.uniform(-1.8, 1.8)
            return max(0, int(offset + jitter))

        events.append(
            {
                "row_id": idx + 1,
                "id": choice["id"],
                "name": choice["name"],
                "description": choice["description"],
                "color": choice["color"],
                "system": choice["system"],
                "subsystem": choice["subsystem"],
                "unit": choice["unit"],
                "code": choice["code"],
                "data": choice.get("data"),
                "set_clear": set_clear,
                "utctime": timestamp.isoformat(timespec="seconds") + "Z",
                "norm_time": int(offset),
                "a_time": _channel_time("A"),
                "b_time": _channel_time("B"),
                "c_time": _channel_time("C"),
                "d_time": _channel_time("D"),
                "channels": seen_channels,
            }
        )

    # Create simple system mode segments spanning the log range.
    mode_labels = ["Startup", "Self Test", "Execution", "Pre-Shutdown"]
    mode_count = max(2, min(len(mode_labels), int(hours) + 1))
    mode_span = span_seconds / mode_count
    modes = []
    for idx in range(mode_count):
        start_s = int(idx * mode_span)
        end_s = int((idx + 1) * mode_span) if idx < mode_count - 1 else span_seconds
        start_ts = start_time + timedelta(seconds=start_s)
        end_ts = start_time + timedelta(seconds=end_s)
        modes.append(
            {
                "name": mode_labels[idx],
                "start": start_ts.isoformat(timespec="seconds") + "Z",
                "end": end_ts.isoformat(timespec="seconds") + "Z",
            }
        )

    return {
        "start": start_time.isoformat(timespec="seconds") + "Z",
        "end": end_time.isoformat(timespec="seconds") + "Z",
        "hours": hours,
        "seed": seed_value if seed_value is not None else str(seed_int),
        "events": events,
        "modes": modes,
    }


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


def update_boot_metadata(shard: Dict[str, Any], boot_id: str, system: str, event_id: str, tags: List[str]) -> None:
    conn = ensure_shard_db(Path(shard["db_path"]))
    conn.execute(
        "UPDATE logs SET system = ?, event_id = ?, tags = ? WHERE boot_id = ?",
        (system, event_id, ",".join(tags), boot_id),
    )
    conn.commit()
    conn.close()

    db = get_db()
    db.execute("DELETE FROM log_index WHERE shard_id = ? AND boot_id = ?", (shard["id"], boot_id))
    rows = conn = ensure_shard_db(Path(shard["db_path"]))
    rows = conn.execute(
        "SELECT row_id, system, event_id, tags FROM logs WHERE boot_id = ?", (boot_id,)
    ).fetchall()
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


def load_log_data_from_shard(shard: Dict[str, Any], boot_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    path = Path(shard["db_path"])
    if not path.exists():
        return None
    conn = ensure_shard_db(path)
    target_boot = boot_id or get_latest_boot_id(shard["id"])
    if not target_boot:
        # Fallback to whatever is in the shard DB.
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


def _require_login_redirect():
    if g.current_user:
        return None
    flash("Please log in to continue.")
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if g.current_user:
        return redirect(url_for("index"))
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        if not email:
            flash("Email is required.")
            return redirect(url_for("login"))
        user = get_or_create_user(email)
        token = issue_login_token(user["id"])
        login_link = url_for("consume_token", token=token, _external=True)
        # TODO: send email with login_link; kept commented for now while SMTP is unavailable.
        # send_login_email(email, login_link)
        flash("Check your email for the magic link. Redirecting for development.")
        return redirect(login_link)
    return render_template("login.html")


@app.route("/auth/<token>")
def consume_token(token: str):
    user_id = consume_login_token(token)
    if not user_id:
        flash("That login link is invalid or expired. Request a new one.")
        return redirect(url_for("login"))
    login_user(user_id)
    user = get_user(user_id)
    if user and not user.get("name"):
        return redirect(url_for("profile", first_login=1))
    return redirect(url_for("index"))


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    flash("You have been logged out.")
    return redirect(url_for("index"))


@app.route("/profile", methods=["GET", "POST"])
def profile():
    redirect_response = _require_login_redirect()
    if redirect_response:
        return redirect_response
    user = g.current_user
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        update_user_name(user["id"], name)
        g.current_user["name"] = name
        flash("Profile updated.")
        return redirect(url_for("profile"))
    return render_template("profile.html", user=user, first_login=request.args.get("first_login"))


@app.route("/")
def index():
    shard_id = request.args.get("shard", type=int)
    dataset_id = request.args.get("dataset", type=int)
    shard = None
    if shard_id:
        shard = get_shard(shard_id)
    elif dataset_id:
        shards = get_shards_for_dataset(dataset_id)
        shard = shards[0] if shards else None
    else:
        shard = get_first_shard()

    boot_id = request.args.get("boot")
    log_data = None
    if shard:
        log_data = load_log_data_from_shard(shard, boot_id)

    if not log_data:
        hours = request.args.get("hours", default="4")
        seed = request.args.get("seed")
        try:
            hours_value = max(0.25, min(48.0, float(hours)))
        except ValueError:
            hours_value = 4.0
        log_data = generate_logs(hours_value, seed)
        shard = None

    datasets = list_datasets()
    shards_for_dataset = (
        get_shards_for_dataset(shard["dataset_id"]) if shard else []
    )
    boots_for_shard = list_boots_for_shard(shard["id"]) if shard else []

    return render_template(
        "index.html",
        log_data=log_data,
        current_shard=shard,
        datasets=datasets,
        dataset_shards=shards_for_dataset,
        boots=boots_for_shard,
    )


@app.route("/app.js")
def app_js():
    return Response(render_template("app.js"), mimetype="application/javascript")


@app.route("/upload", methods=["GET", "POST"])
def upload_logs():
    datasets = list_datasets()
    selected_dataset_id = request.form.get("dataset_id", type=int)
    if request.method == "POST":
        dataset_name_new = (request.form.get("dataset_name_new") or "").strip()
        shard_name_new = (request.form.get("shard_name_new") or "").strip()
        shard_id = request.form.get("shard_id", type=int)
        file = request.files.get("log_file")
        gen_hours = request.form.get("hours", type=float)
        gen_seed = request.form.get("seed")

        dataset_obj = None
        if selected_dataset_id:
            dataset_obj = get_dataset(selected_dataset_id)
        if not dataset_obj and dataset_name_new:
            dataset_obj = get_dataset_by_name(dataset_name_new) or create_dataset(dataset_name_new)
        if not dataset_obj:
            flash("Select an existing dataset or provide a new dataset name.")
            return redirect(url_for("upload_logs"))

        shard_obj = None
        if shard_id:
            shard_obj = get_shard(shard_id)
        if (not shard_obj or shard_obj["dataset_id"] != dataset_obj["id"]) and shard_name_new:
            shard_obj = create_shard(dataset_obj["id"], shard_name_new)
        if not shard_obj or shard_obj["dataset_id"] != dataset_obj["id"]:
            flash("Select an existing shard or create a new shard for the chosen dataset.")
            return redirect(url_for("upload_logs"))

        events = []
        if file and file.filename:
            events = parse_events_from_upload(file)
            if not events:
                flash("No events found in upload. Ensure JSON is an array or has an 'events' list.")
                return redirect(url_for("upload_logs"))
        else:
            try:
                hours_value = max(0.25, min(48.0, float(gen_hours))) if gen_hours else 4.0
            except (TypeError, ValueError):
                hours_value = 4.0
            generated = generate_logs(hours_value, gen_seed or None)
            events = generated.get("events", [])
            for ev in events:
                ev.setdefault("event_id", "")
                ev.setdefault("tags", [])

        insert_events_into_shard(shard_obj, events)
        flash(f"Imported {len(events)} events into shard '{shard_obj['name']}'.")
        return redirect(url_for("index", shard=shard_obj["id"]))

    shards_map = {d["id"]: get_shards_for_dataset(d["id"]) for d in datasets}
    return render_template(
        "upload.html",
        datasets=datasets,
        shards_map=shards_map,
        selected_dataset_id=selected_dataset_id,
    )


@app.route("/logs")
def logs_index():
    datasets = list_datasets()
    dataset_id = request.args.get("dataset_id", type=int)
    db = get_db()
    params = []
    where_clause = ""
    if dataset_id:
        where_clause = "WHERE shards.dataset_id = ?"
        params.append(dataset_id)
    rows = db.execute(
        f"""
        SELECT boots.boot_id, boots.event_count, boots.created_at,
               shards.id AS shard_id, shards.name AS shard_name, shards.dataset_id,
               datasets.name AS dataset_name
        FROM boots
        JOIN shards ON shards.id = boots.shard_id
        JOIN datasets ON datasets.id = shards.dataset_id
        {where_clause}
        ORDER BY datasets.name, shards.name, datetime(boots.created_at) DESC
    """,
        tuple(params),
    ).fetchall()

    boots = [
        {
            "boot_id": r["boot_id"],
            "event_count": r["event_count"],
            "created_at": r["created_at"],
            "shard_id": r["shard_id"],
            "shard_name": r["shard_name"],
            "dataset_id": r["dataset_id"],
            "dataset_name": r["dataset_name"],
        }
        for r in rows
    ]

    boot_meta_map = {}
    for boot in boots:
        conn = ensure_shard_db(Path(get_shard(boot["shard_id"])["db_path"]))
        sample = conn.execute(
            "SELECT system, event_id, tags FROM logs WHERE boot_id = ? LIMIT 1", (boot["boot_id"],)
        ).fetchone()
        conn.close()
        boot_meta_map[boot["boot_id"]] = {
            "system": sample["system"] if sample else "",
            "event_id": sample["event_id"] if sample else "",
            "tags": sample["tags"] if sample else "",
        }

    return render_template(
        "logs.html",
        datasets=datasets,
        boots=boots,
        selected_dataset_id=dataset_id,
        boot_meta_map=boot_meta_map,
    )


@app.route("/shards/<int:shard_id>/delete", methods=["POST"])
def delete_shard_route(shard_id: int):
    delete_shard(shard_id)
    flash("Shard deleted.")
    return redirect(url_for("logs_index"))


@app.route("/boots/<int:shard_id>/<boot_id>/edit", methods=["GET", "POST"])
def edit_boot(shard_id: int, boot_id: str):
    shard = get_shard(shard_id)
    if not shard:
        flash("Shard not found.")
        return redirect(url_for("logs_index"))
    boot = get_boot_meta(shard_id, boot_id)
    if not boot:
        flash("Boot not found.")
        return redirect(url_for("logs_index"))

    conn = ensure_shard_db(Path(shard["db_path"]))
    sample = conn.execute(
        "SELECT system, event_id, tags FROM logs WHERE boot_id = ? LIMIT 1", (boot_id,)
    ).fetchone()
    conn.close()
    system_val = sample["system"] if sample else ""
    event_id_val = sample["event_id"] if sample else ""
    tags_val = sample["tags"] if sample else ""

    if request.method == "POST":
        system = (request.form.get("system") or "").strip()
        event_id = (request.form.get("event_id") or "").strip()
        tags_raw = request.form.get("tags") or ""
        tags = [t.strip() for t in tags_raw.split(",") if t.strip()]
        update_boot_metadata(shard, boot_id, system, event_id, tags)
        flash("Boot metadata updated.")
        return redirect(url_for("logs_index"))

    return render_template(
        "boot_edit.html",
        shard=shard,
        boot=boot,
        system=system_val,
        event_id=event_id_val,
        tags=tags_val,
    )


if __name__ == "__main__":
    app.run(debug=True, port=8080)
