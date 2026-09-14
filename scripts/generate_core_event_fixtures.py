#!/usr/bin/env python3
"""Generate deterministic core-event fixtures from a simulated fault catalog."""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

SYSTEMS = {
    "Power": {"Bus": ["PDU", "BAT"], "Conversion": ["INV", "REG"]},
    "Thermal": {"Cooling": ["PMP", "RAD"], "Heating": ["HTR", "LOOP"]},
    "Guidance": {"Navigation": ["IMU", "GPS"], "Control": ["CPU", "ACT"]},
    "Comms": {"Radio": ["TX", "RX"], "Network": ["MUX", "LINK"]},
    "Flight": {"Avionics": ["FMC", "DISP"], "Propulsion": ["ENG", "VALVE"]},
}
FAULT_NAMES = [
    "communication timeout",
    "temperature out of range",
    "input voltage low",
    "watchdog expired",
    "data integrity fault",
    "self-test failed",
    "calibration required",
    "response degraded",
]


def _catalog(rng: random.Random) -> list[dict[str, str]]:
    catalog = []
    for system, subsystems in SYSTEMS.items():
        prefix = system[:3].upper()
        for subsystem, units in subsystems.items():
            for unit in units:
                for number in range(1, rng.randint(20, 50) + 1):
                    catalog.append(
                        {
                            "system": system,
                            "subsystem": subsystem,
                            "unit": unit,
                            "code": f"{prefix}-{number:02d}",
                            "name": f"{unit} {rng.choice(FAULT_NAMES)}",
                        }
                    )
    return catalog


def _channels(rng: random.Random, labels: list[str]) -> list[str]:
    count = len(labels)
    # Most real alarm reports are seen by every channel; partial reports remain.
    desired = rng.choices(
        range(1, count + 1), weights=[0.03, 0.12, 0.25, 0.60][-count:]
    )[0]
    return sorted(rng.sample(labels, desired))


def generate_fixture(channel_count: int, *, seed: int, event_count: int = 2400) -> dict:
    rng = random.Random(seed)
    labels = [chr(ord("A") + index) for index in range(channel_count)]
    catalog = _catalog(rng)
    clear = list(range(len(catalog)))
    active: set[int] = set()
    events = []
    timestamp = datetime(2026, 4, 3, 8, tzinfo=timezone.utc)

    for row_id in range(1, event_count + 1):
        if not clear and not active:
            clear = list(range(len(catalog)))
        if active and (not clear or rng.random() < 0.42):
            index = rng.choice(tuple(active))
            active.remove(index)
            action = "clear"
        else:
            index = rng.choice(clear)
            clear.remove(index)
            active.add(index)
            action = "set"
        fault = catalog[index]
        event = {
            "row_id": row_id,
            "utctime": timestamp.isoformat().replace("+00:00", "Z"),
            "name": fault["name"].title(),
            "color": rng.choices(
                ["Green", "Yellow", "Red", "Flashing Red"],
                weights=[0.40, 0.30, 0.22, 0.08],
            )[0],
            **{key: fault[key] for key in ("system", "subsystem", "unit", "code")},
            "set_clear": action,
            "channels": _channels(rng, labels),
        }
        events.append(event)
        timestamp += timedelta(seconds=rng.uniform(2.0, 24.0))

    return {
        "start": events[0]["utctime"],
        "end": events[-1]["utctime"],
        "hours": 10,
        "channels": labels,
        "channelCount": channel_count,
        "events": events,
    }


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    for count, seed in ((3, 303), (4, 404)):
        path = root / f"src/cinc/plugins/core_event/samples/sample-{count}ch.json"
        path.write_text(json.dumps(generate_fixture(count, seed=seed), indent=2) + "\n")


if __name__ == "__main__":
    main()
