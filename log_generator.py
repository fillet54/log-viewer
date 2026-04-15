from __future__ import annotations

from datetime import datetime, timedelta
import random
from typing import Optional, Dict, Any

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

CONNECTIVITY_FAULTS = [
    {
        "id": "eth1_down",
        "name": "ETH1_DOWN",
        "description": "Connectivity fault for eth1 link.",
        "color": "Red",
        "system": "Network",
        "subsystem": "Connectivity",
        "unit": "ETH1",
        "code": "NET-ETH1-DOWN",
        "link": "eth1",
    },
    {
        "id": "eth2_down",
        "name": "ETH2_DOWN",
        "description": "Connectivity fault for eth2 link.",
        "color": "Red",
        "system": "Network",
        "subsystem": "Connectivity",
        "unit": "ETH2",
        "code": "NET-ETH2-DOWN",
        "link": "eth2",
    },
    {
        "id": "wifi_down",
        "name": "WIFI_DOWN",
        "description": "Connectivity fault for wifi link.",
        "color": "Red",
        "system": "Network",
        "subsystem": "Connectivity",
        "unit": "WIFI",
        "code": "NET-WIFI-DOWN",
        "link": "wifi",
    },
    {
        "id": "cellular_down",
        "name": "CELLULAR_DOWN",
        "description": "Connectivity fault for cellular link.",
        "color": "Red",
        "system": "Network",
        "subsystem": "Connectivity",
        "unit": "CELLULAR",
        "code": "NET-CELLULAR-DOWN",
        "link": "cellular",
    },
]

LEVEL_ORDER = ["Green", "Yellow", "Red", "Flashing Red"]


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


def _build_channels(rng: random.Random) -> list[str]:
    channels = ["A", "B", "C", "D"]
    if rng.random() < 0.85:
        return channels
    return rng.sample(channels, rng.randint(1, 3))


def _build_channel_time(
    offset_seconds: float, channel: str, seen_channels: list[str], rng: random.Random
) -> Optional[int]:
    if channel not in seen_channels:
        return None
    jitter = rng.uniform(-1.8, 1.8)
    return max(0, int(offset_seconds + jitter))


def _build_event_entry(
    choice: Dict[str, Any],
    timestamp: datetime,
    offset_seconds: float,
    set_clear: str,
    seen_channels: list[str],
    rng: random.Random,
) -> Dict[str, Any]:
    return {
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
        "norm_time": int(offset_seconds),
        "a_time": _build_channel_time(offset_seconds, "A", seen_channels, rng),
        "b_time": _build_channel_time(offset_seconds, "B", seen_channels, rng),
        "c_time": _build_channel_time(offset_seconds, "C", seen_channels, rng),
        "d_time": _build_channel_time(offset_seconds, "D", seen_channels, rng),
        "channels": seen_channels,
    }


def _generate_connectivity_events(
    start_time: datetime, span_seconds: int, rng: random.Random
) -> list[Dict[str, Any]]:
    events: list[Dict[str, Any]] = []
    if span_seconds <= 15:
        return events

    for fault in CONNECTIVITY_FAULTS:
        is_down = False
        cursor = rng.uniform(20, min(300, max(25, span_seconds / 6)))

        while cursor < span_seconds - 5:
            is_down = not is_down
            set_clear = "set" if is_down else "clear"
            timestamp = start_time + timedelta(seconds=cursor)
            seen_channels = _build_channels(rng)
            events.append(
                _build_event_entry(
                    {
                        **fault,
                        "data": {
                            "link": {"name": fault["link"]},
                            "fault": {"state": "down" if is_down else "clear"},
                        },
                    },
                    timestamp,
                    cursor,
                    set_clear,
                    seen_channels,
                    rng,
                )
            )

            if rng.random() < 0.28 and cursor < span_seconds - 20:
                cursor += rng.uniform(2, 8)
                is_down = not is_down
                set_clear = "set" if is_down else "clear"
                timestamp = start_time + timedelta(seconds=cursor)
                seen_channels = _build_channels(rng)
                events.append(
                    _build_event_entry(
                        {
                            **fault,
                            "data": {
                                "link": {"name": fault["link"]},
                                "fault": {"state": "down" if is_down else "clear"},
                            },
                        },
                        timestamp,
                        cursor,
                        set_clear,
                        seen_channels,
                        rng,
                    )
                )

            cursor += rng.uniform(45, min(1800, max(90, span_seconds / 3)))

    return events


def generate_logs(hours: float, seed_value: Optional[str]) -> Dict[str, Any]:
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
        seen_channels = _build_channels(rng)
        events.append(
            _build_event_entry(choice, timestamp, offset, set_clear, seen_channels, rng)
        )

    events.extend(_generate_connectivity_events(start_time, span_seconds, rng))
    events.sort(key=lambda item: (item["norm_time"], item["utctime"], item["name"]))
    for idx, event in enumerate(events):
        event["row_id"] = idx + 1

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
