"""Tiny deterministic fake logs for tests and manual demos.

python tests/fake_logs.py write DIR [ROWS]          write a log folder
python tests/fake_logs.py post URL [ROWS] [DELAY]   stream rows to `cinc serve`
cinc build DIR/<log-id>                             view a written folder
"""

from __future__ import annotations

import math
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from cinc import LiveClient, LogWriter
except ImportError:  # run from a checkout without installing: use ../src
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
    from cinc import LiveClient, LogWriter

START = datetime(2026, 1, 1, 8, tzinfo=timezone.utc)
CHANNELS = ["A", "B", "C"]
FAULTS = [
    # system, subsystem, unit, name, code, colour, description
    (
        "Power",
        "PDU",
        "PDU-2",
        "PDU input voltage low",
        "PWR-01",
        "Red",
        "Input bus below 26 V for over 200 ms; loads are shed in priority order.",
    ),
    (
        "Power",
        "Battery",
        "BAT-1",
        "Battery charge complete",
        "PWR-12",
        "Green",
        "Battery 1 reached full charge; the charger dropped to trickle mode.",
    ),
    (
        "Thermal",
        "Coolant",
        "Pump-A",
        "Pump temperature high",
        "THM-04",
        "Yellow",
        "Pump housing is above its 70 C warning limit.",
    ),
    (
        "Comms",
        "Radio",
        "UHF-1",
        "Radio link timeout",
        "COM-02",
        "Flashing Red",
        "No ground-link frames for 5 s; retrying on the backup channel.",
    ),
    (
        "Comms",
        "Radio",
        "UHF-1",
        "Ground link acquired",
        "COM-05",
        "Green",
        "Ground station locked on; telemetry downlink is running at full rate.",
    ),
    (
        "Guidance",
        "Nav",
        "IMU-2",
        "IMU self-test failed",
        "GDN-07",
        "Yellow",
        "Gyro bias check out of tolerance; navigation is using the other IMU.",
    ),
]
# Which channels see each set/clear pair; most see all, some miss one or two.
CHANNEL_PATTERNS = [
    ["A", "B", "C"],
    ["A", "B", "C"],
    ["A", "B"],
    ["A", "B", "C"],
    ["A"],
    ["B", "C"],
    ["A", "B", "C"],
    ["A", "C"],
]
LEVELS = ["INFO", "DEBUG", "INFO", "WARN", "INFO", "ERROR"]
LOGGERS = ["api", "worker", "db"]


def _time(i: int) -> str:
    return (START + timedelta(seconds=5 * i)).isoformat().replace("+00:00", "Z")


def core_event_rows(count: int, start: int = 0) -> list[dict]:
    """Alternating set/clear pairs for a few faults.

    A fault keeps its colour on both the SET and the CLEAR. A pair is seen on the
    same channels, which is usually all of them but not always.
    """
    rows = []
    for i in range(start, start + count):
        system, subsystem, unit, name, code, color, description = FAULTS[
            (i // 2) % len(FAULTS)
        ]
        is_set = i % 2 == 0
        pair = i // 2
        rows.append(
            {
                "utctime": _time(i),
                "name": name,
                "system": system,
                "subsystem": subsystem,
                "unit": unit,
                "description": description,
                "code": code,
                "color": color,
                "set_clear": "set" if is_set else "clear",
                "channels": CHANNEL_PATTERNS[pair % len(CHANNEL_PATTERNS)],
                "data": {"bus": {"load_pct": 40 + (i * 7) % 50}},
            }
        )
    return rows


def text_rows(count: int, start: int = 0) -> list[dict]:
    return [
        {
            "ts": _time(i),
            "level": LEVELS[i % len(LEVELS)],
            "logger": LOGGERS[i % len(LOGGERS)],
            "message": f"handled request {i}",
        }
        for i in range(start, start + count)
    ]


TRACKS = {
    "UAV-1": {"label": "Survey drone", "color": "#d1495b"},
    "AC-7": {"label": "Chase aircraft", "color": "#2e86ab"},
}


AC7_LEG_SECONDS = 60


def track_rows(count: int, start: int = 0) -> list[dict]:
    """Position samples for log rows start..start+count (five seconds per row).

    UAV-1 flies a slow climbing loop, sampled every second. AC-7 zig-zags east
    and is sampled only when it turns (every AC7_LEG_SECONDS), the sparse style
    the Map chart fills in at constant heading and speed.
    """
    rows = []
    for second in range(5 * start, 5 * (start + count)):
        when = (START + timedelta(seconds=second)).isoformat().replace("+00:00", "Z")
        angle = math.radians(second * 1.5)
        rows.append(
            {
                "time": when,
                "track": "UAV-1",
                "lat": round(34.05 + 0.03 * math.sin(angle), 6),
                "lon": round(-117.60 + 0.04 * math.cos(angle), 6),
                "alt": round(300 + 2.5 * second, 1),
            }
        )
        if second % AC7_LEG_SECONDS == 0:
            leg = second // AC7_LEG_SECONDS
            rows.append(
                {
                    "time": when,
                    "track": "AC-7",
                    "lat": round(33.90 + (0.06 if leg % 2 else 0.0), 6),
                    "lon": round(-117.85 + 0.09 * leg, 6),
                    "alt": 3000.0 + (150.0 if leg % 2 else 0.0),
                }
            )
    return rows


# core_event header: the demo's channels, plus a timeline line for the bus load
# every event carries (see cinc/plugins/core_event/config.py).
CORE_EVENT_HEADER = {
    "channels": CHANNELS,
    "series": [
        {
            "id": "bus-load",
            "label": "Bus Load",
            "field": "data.bus.load_pct",
            "unit": "%",
            "min": 0,
            "max": 100,
        }
    ],
}


def write_log(root: Path, rows: int = 40, name: str = "Fake log") -> Path:
    """Write a log folder in the on-disk format, the way any tool could."""
    log = LogWriter(root, name, log_id=f"fake-{START:%Y%m%d}", started=START)
    log.header("core_event", CORE_EVENT_HEADER)
    log.header("track", tracks=TRACKS)
    log.write("core_event", core_event_rows(rows))
    log.write("text_log", text_rows(rows))
    log.write("track", track_rows(rows))
    return log.path


def post_live(base_url: str, rows: int = 200, delay: float = 0.5) -> None:
    """Start a new log on a running server and trickle rows into it."""
    client = LiveClient(base_url)
    client.start("Fake live log")
    client.post("core_event", header=CORE_EVENT_HEADER)
    client.post("track", header={"tracks": TRACKS})
    # One row per post, so a SET and its CLEAR arrive in different requests.
    for i in range(rows):
        client.post("core_event", core_event_rows(1, i))
        client.post("text_log", text_rows(1, i))
        client.post("track", track_rows(1, i))
        time.sleep(delay)


if __name__ == "__main__":
    args = sys.argv[1:] + [None, None, None]
    command, target, rows, delay = args[:4]
    if command == "write" and target:
        print(write_log(Path(target), int(rows or 40)))
    elif command == "post" and target:
        post_live(target, int(rows or 200), float(delay or 0.5))
    else:
        print(__doc__)
