"""Producing logs with Cinc's Python API (what a test rig or simulator would do)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from cinc import LogWriter, track_sample
from cinc.plugins.core_event import event

OWNERS = {
    "Power": "Electrical",
    "Thermal": "Thermal",
    "Comms": "Avionics",
    "Nav": "Avionics",
}
CHANNELS = ["A", "B"]
FAULTS = [
    ("Power", "Bus voltage low", "PWR-01", "Red"),
    ("Thermal", "Pump temperature high", "THM-04", "Yellow"),
    ("Comms", "Downlink lost", "COM-02", "Flashing Red"),
    ("Nav", "GPS degraded", "NAV-03", "Yellow"),
]


def owner_of(system) -> str:
    return OWNERS.get(str(system or ""), "Unassigned")


def write_demo_log(root: Path | str, minutes: int = 10) -> Path:
    """A log folder with faults that set and clear, and an aircraft track."""
    start = datetime(2026, 3, 1, 9, tzinfo=timezone.utc)
    log = LogWriter(root, "Acme demo flight", started=start)
    log.header("core_event", channels=CHANNELS)
    log.header("track", tracks={"ACME-1": {"label": "Acme 1"}})
    for minute in range(minutes):
        system, name, code, color = FAULTS[minute % len(FAULTS)]
        when = start + timedelta(minutes=minute)
        log.write(
            "core_event",
            [
                event(
                    when, name, system=system, code=code, color=color, channels=CHANNELS
                ),
                event(
                    when + timedelta(seconds=40),
                    name,
                    system=system,
                    code=code,
                    color=color,
                    set_clear="clear",
                    channels=CHANNELS,
                ),
            ],
        )
        # One sample per heading change: a box pattern, 2.5 minutes a side.
        if minute % 2 == 0:
            corner = (minute // 2) % 4
            lat = 47.45 + (0.2 if corner in (1, 2) else 0.0)
            lon = -122.3 + (0.3 if corner in (2, 3) else 0.0)
            log.write("track", track_sample(when, lat, lon, 1500, track="ACME-1"))
    return log.path
