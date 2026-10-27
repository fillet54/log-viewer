"""Tiny deterministic fake logs for tests and manual demos.

python tests/fake_logs.py write DIR [ROWS]          write a log folder
python tests/fake_logs.py post URL [ROWS] [DELAY]   stream rows to `cinc serve`
cinc build DIR/<log-id>                             view a written folder
"""

from __future__ import annotations

import json
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

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


def write_log(root: Path, rows: int = 40, name: str = "Fake log") -> Path:
    """Write a log folder in the on-disk format, the way any tool could."""
    path = Path(root) / f"fake-{START:%Y%m%d}"
    path.mkdir(parents=True, exist_ok=True)
    (path / "log.json").write_text(
        json.dumps({"name": name, "started": START.isoformat()})
    )
    (path / "core_event.json").write_text(json.dumps({"channels": CHANNELS}))
    for type_id, make in (("core_event", core_event_rows), ("text_log", text_rows)):
        lines = [json.dumps(row) for row in make(rows)]
        (path / f"{type_id}.jsonl").write_text("\n".join(lines) + "\n")
    return path


def _post(url: str, body: dict) -> None:
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(request).close()


def post_live(base_url: str, rows: int = 200, delay: float = 0.5) -> None:
    """Start a new log on a running server and trickle rows into it."""
    base_url = base_url.rstrip("/")
    _post(f"{base_url}/api/logs", {"name": "Fake live log"})
    _post(
        f"{base_url}/api/events",
        {"type": "core_event", "header": {"channels": CHANNELS}},
    )
    # One row per post, so a SET and its CLEAR arrive in different requests.
    for i in range(rows):
        _post(
            f"{base_url}/api/events",
            {"type": "core_event", "events": core_event_rows(1, i)},
        )
        _post(f"{base_url}/api/events", {"type": "text_log", "events": text_rows(1, i)})
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
