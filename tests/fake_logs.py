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
    ("Power", "PDU input voltage low", "PWR-01", "Red"),
    ("Thermal", "Pump temperature high", "THM-04", "Yellow"),
    ("Comms", "Radio link timeout", "COM-02", "Flashing Red"),
    ("Guidance", "IMU self-test failed", "GDN-07", "Yellow"),
]
LEVELS = ["INFO", "DEBUG", "INFO", "WARN", "INFO", "ERROR"]
LOGGERS = ["api", "worker", "db"]


def _time(i: int) -> str:
    return (START + timedelta(seconds=5 * i)).isoformat().replace("+00:00", "Z")


def core_event_rows(count: int, start: int = 0) -> list[dict]:
    """Alternating set/clear pairs for a few faults, seen on every channel."""
    rows = []
    for i in range(start, start + count):
        system, name, code, color = FAULTS[(i // 2) % len(FAULTS)]
        is_set = i % 2 == 0
        rows.append(
            {
                "utctime": _time(i),
                "name": name,
                "system": system,
                "code": code,
                "color": color if is_set else "Green",
                "set_clear": "set" if is_set else "clear",
                "channels": CHANNELS,
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
    for i in range(0, rows, 2):
        _post(
            f"{base_url}/api/events",
            {"type": "core_event", "events": core_event_rows(2, i)},
        )
        _post(f"{base_url}/api/events", {"type": "text_log", "events": text_rows(2, i)})
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
