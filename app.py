from datetime import datetime, timedelta
import os
import random
import secrets
import sqlite3
from typing import Any, Dict, Optional
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
    hours = request.args.get("hours", default="4")
    seed = request.args.get("seed")
    try:
        hours_value = max(0.25, min(48.0, float(hours)))
    except ValueError:
        hours_value = 4.0

    log_data = generate_logs(hours_value, seed)
    return render_template("index.html", log_data=log_data)


@app.route("/app.js")
def app_js():
    return Response(render_template("app.js"), mimetype="application/javascript")


if __name__ == "__main__":
    app.run(debug=True, port=8080)
