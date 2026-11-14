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
FLIGHT_MINUTES = 30

# (system, subsystem, unit, code, name, colour, [(set minute, clear minute or None)])
# None leaves the fault set at the end of the flight.
FAULTS = [
    (
        "Power",
        "PDU",
        "PDU-2",
        "PWR-01",
        "Bus voltage low",
        "Red",
        [(2, 3.5), (14, 14.4), (22, 25)],
    ),
    ("Power", "Battery", "BAT-1", "PWR-07", "Battery temp high", "Yellow", [(5, 12)]),
    ("Power", "Battery", "BAT-1", "PWR-09", "Cell imbalance", "Yellow", [(18, None)]),
    (
        "Thermal",
        "Coolant",
        "Pump-A",
        "THM-04",
        "Pump temperature high",
        "Yellow",
        [(3, 4), (8, 9.5), (16, 17), (24, 26)],
    ),
    ("Thermal", "Coolant", "Pump-A", "THM-05", "Pump flow low", "Red", [(9, 9.6)]),
    (
        "Thermal",
        "Cabin",
        "HX-1",
        "THM-11",
        "Heat exchanger fouled",
        "Yellow",
        [(11, 20)],
    ),
    (
        "Comms",
        "Radio",
        "UHF-1",
        "COM-02",
        "Downlink lost",
        "Flashing Red",
        [(7, 7.8), (19, 19.3)],
    ),
    (
        "Comms",
        "Radio",
        "UHF-1",
        "COM-03",
        "Link margin low",
        "Yellow",
        [(6.5, 8.5), (18.5, 20)],
    ),
    ("Comms", "Antenna", "ANT-2", "COM-09", "Antenna fault", "Red", [(27, None)]),
    ("Nav", "GPS", "GPS-1", "NAV-03", "GPS degraded", "Yellow", [(10, 13), (21, 21.5)]),
    ("Nav", "IMU", "IMU-2", "NAV-07", "IMU self-test failed", "Yellow", [(1, 1.5)]),
]
PHASES = [(0, "Takeoff"), (2, "Climb"), (6, "Cruise"), (23, "Descent"), (29, "Landing")]


def owner_of(system) -> str:
    return OWNERS.get(str(system or ""), "Unassigned")


def fault_rows(start: datetime) -> list[dict]:
    """core_event SET/CLEAR rows for FAULTS, in time order."""
    rows = []
    for system, subsystem, unit, code, name, color, spans in FAULTS:
        fields = dict(
            system=system, subsystem=subsystem, unit=unit, code=code, color=color
        )
        # Most faults are seen on both channels; the comms faults only on A.
        channels = ["A"] if system == "Comms" else CHANNELS
        for set_at, clear_at in spans:
            rows.append(
                event(
                    start + timedelta(minutes=set_at), name, channels=channels, **fields
                )
            )
            if clear_at is not None:
                rows.append(
                    event(
                        start + timedelta(minutes=clear_at),
                        name,
                        channels=channels,
                        set_clear="clear",
                        **fields,
                    )
                )
    return sorted(rows, key=lambda row: row["utctime"])


def write_demo_log(root: Path | str, minutes: int = FLIGHT_MINUTES) -> Path:
    """A log folder: faults that set and clear, phase notes, and an aircraft track."""
    start = datetime(2026, 3, 1, 9, tzinfo=timezone.utc)
    end = start + timedelta(minutes=minutes)
    log = LogWriter(root, "Acme demo flight", started=start)
    log.header("core_event", channels=CHANNELS)
    log.header("track", tracks={"ACME-1": {"label": "Acme 1"}})
    log.write("core_event", [r for r in fault_rows(start) if r["utctime"] <= _iso(end)])
    log.write(
        "text_log",
        [
            {
                "ts": _iso(start + timedelta(minutes=at)),
                "level": "INFO",
                "logger": "flight",
                "message": f"Phase: {phase}",
            }
            for at, phase in PHASES
            if at <= minutes
        ],
    )
    # One sample per heading change: a box pattern, 2.5 minutes a side.
    for leg in range(int(minutes / 2.5) + 1):
        corner = leg % 4
        lat = 47.45 + (0.2 if corner in (1, 2) else 0.0)
        lon = -122.3 + (0.3 if corner in (2, 3) else 0.0)
        when = start + timedelta(minutes=2.5 * leg)
        log.write("track", track_sample(when, lat, lon, 1500, track="ACME-1"))
    return log.path


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")
