from datetime import datetime, timedelta
import random
from flask import Flask, render_template, request, Response

app = Flask(__name__)

FAULT_CATALOG = [
    {
        "id": "pwr_bus",
        "name": "Power Bus Drift",
        "description": "Voltage drift detected on primary bus.",
        "level": "yellow",
        "system": "Power",
        "subsystem": "Distribution",
        "unit": "PDU-1",
        "code": "PWR-214",
    },
    {
        "id": "temp_core",
        "name": "Core Temp Spike",
        "description": "Thermal threshold exceeded on core stack.",
        "level": "red",
        "system": "Thermal",
        "subsystem": "Cooling",
        "unit": "FAN-3",
        "code": "THM-501",
    },
    {
        "id": "link_loss",
        "name": "Link Loss",
        "description": "Packet loss above tolerance.",
        "level": "yellow",
        "system": "Network",
        "subsystem": "Backplane",
        "unit": "SW-2",
        "code": "NET-118",
    },
    {
        "id": "db_timeout",
        "name": "DB Timeout",
        "description": "Query timeout exceeded 2000ms.",
        "level": "red",
        "system": "Storage",
        "subsystem": "Database",
        "unit": "DB-1",
        "code": "DB-907",
    },
    {
        "id": "sensor_glitch",
        "name": "Sensor Glitch",
        "description": "Transient sensor anomaly detected.",
        "level": "green",
        "system": "Telemetry",
        "subsystem": "Sensors",
        "unit": "SEN-9",
        "code": "TEL-033",
    },
    {
        "id": "auth_fail",
        "name": "Auth Failure",
        "description": "Repeated authentication failure.",
        "level": "yellow",
        "system": "Security",
        "subsystem": "Auth",
        "unit": "AUTH-2",
        "code": "SEC-201",
    },
    {
        "id": "queue_lag",
        "name": "Queue Lag",
        "description": "Ingestion queue lag above threshold.",
        "level": "green",
        "system": "Ingest",
        "subsystem": "Queue",
        "unit": "Q-4",
        "code": "ING-047",
    },
    {
        "id": "ctrl_fault",
        "name": "Control Fault",
        "description": "Control loop instability detected.",
        "level": "dark red",
        "system": "Control",
        "subsystem": "Stability",
        "unit": "CTRL-1",
        "code": "CTL-888",
    },
    {
        "id": "mem_warn",
        "name": "Memory Pressure",
        "description": "Memory usage above 85%.",
        "level": "yellow",
        "system": "Compute",
        "subsystem": "Memory",
        "unit": "CPU-2",
        "code": "CMP-312",
    },
    {
        "id": "disk_slow",
        "name": "Disk Slowdown",
        "description": "I/O latency above baseline.",
        "level": "green",
        "system": "Storage",
        "subsystem": "IO",
        "unit": "DSK-7",
        "code": "STO-119",
    },
]

LEVEL_ORDER = ["green", "yellow", "red", "dark red"]


def _seed_to_int(seed_value: str | None) -> int:
    if seed_value is None or seed_value == "":
        return 1
    try:
        return int(seed_value)
    except ValueError:
        return abs(hash(seed_value)) % (10**9)


def _level_weight(level: str, cluster_weight: float) -> float:
    if level == "green":
        return 1.2
    if level == "yellow":
        return 0.9 + cluster_weight * 1.6
    if level == "red":
        return 0.6 + cluster_weight * 1.8
    return 0.3 + cluster_weight * 2.0


def generate_logs(hours: float, seed_value: str | None):
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
        weights = [_level_weight(item["level"], weight) for item in FAULT_CATALOG]
        choice = rng.choices(FAULT_CATALOG, weights=weights, k=1)[0]
        is_set = states[choice["id"]]
        action = "clear" if is_set else "set"
        states[choice["id"]] = not is_set

        timestamp = start_time + timedelta(seconds=offset)
        events.append(
            {
                "row_id": idx + 1,
                "id": choice["id"],
                "name": choice["name"],
                "description": choice["description"],
                "level": choice["level"],
                "system": choice["system"],
                "subsystem": choice["subsystem"],
                "unit": choice["unit"],
                "code": choice["code"],
                "action": action,
                "utc": timestamp.isoformat(timespec="seconds") + "Z",
                "seconds_from_start": int(offset),
            }
        )

    return {
        "start": start_time.isoformat(timespec="seconds") + "Z",
        "end": end_time.isoformat(timespec="seconds") + "Z",
        "hours": hours,
        "seed": seed_value if seed_value is not None else str(seed_int),
        "events": events,
    }


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
    app.run(debug=True)
