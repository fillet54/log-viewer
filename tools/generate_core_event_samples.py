"""Generate deterministic, channel-complete core-event demo fixtures."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path


def build_sample(channels, variant):
    start = datetime(2026, 4, 3, 8, tzinfo=timezone.utc)
    events = []
    systems = ["Flight", "Power", "Thermal", "Guidance", "Comms"]
    severities = ["Green", "Green", "Green", "Yellow", "Red", "Flashing Red"]
    for index in range(2500):
        timestamp = start + timedelta(seconds=index * 14.4)
        system = systems[(index + variant) % len(systems)]
        channel = channels[index % len(channels)]
        event = {
            "row_id": index + 1,
            "utctime": timestamp.isoformat().replace("+00:00", "Z"),
            "name": f"{system} event {index + 1}",
            "color": severities[index % len(severities)],
            "system": system,
            "subsystem": ["Core", "Bus", "Radiator", "Nav", "Radio"][index % 5],
            "unit": ["CPU", "PWR", "RAD", "IMU", "TX"][index % 5],
            "code": f"{system[:3].upper()}-{index % 97}",
            "set_clear": "set" if index % 2 == 0 else "clear",
            "channels": [channel],
        }
        if index % 10 == 0:
            event.update(
                id="pwr_bus",
                data={"bus": {"load_pct": 60 + (index // 10) % 13}},
            )
        elif index % 17 == 0:
            event["data"] = {"diagnostic": {"level": {"sample": index}}}
        events.append(event)

    events[0]["channels"] = list(channels)
    events[1]["system"] = {"id": "thermal", "name": "Thermal"}
    events[2]["subsystem_id"] = "7"
    events[2]["subsystem_name"] = "Radiator"
    events[3]["name"] = "Long diagnostic description that deliberately exceeds eighty characters for truncation testing"
    events[4]["set_clear"] = "set"
    events[5]["set_clear"] = "clear"
    return {
        "start": start.isoformat().replace("+00:00", "Z"),
        "end": (start + timedelta(hours=10)).isoformat().replace("+00:00", "Z"),
        "hours": 10,
        "channels": channels,
        "channelCount": len(channels),
        "events": events,
    }


root = Path(__file__).resolve().parents[1] / "src/cinc/plugins/core_event/samples"
for filename, channels, variant in (
    ("sample-3ch.json", ["A", "B", "C"], 0),
    ("sample-4ch.json", ["A", "B", "C", "D"], 1),
):
    (root / filename).write_text(
        json.dumps(build_sample(channels, variant), indent=2) + "\n",
        encoding="utf-8",
    )
