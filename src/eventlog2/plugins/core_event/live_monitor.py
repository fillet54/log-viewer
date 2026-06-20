from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Any

from ...live.monitor import LiveMonitorPlugin

_SAMPLE_FAULTS = [
    {"name": "EngineOverheat",      "system": "Engine",      "subsystem": "Cooling",       "unit": "Radiator",    "color": "Red"},
    {"name": "OilPressureLow",      "system": "Engine",      "subsystem": "Lubrication",   "unit": "OilPump",     "color": "Yellow"},
    {"name": "TransmissionFault",   "system": "Drivetrain",  "subsystem": "Transmission",  "unit": "Gearbox",     "color": "Red"},
    {"name": "BatteryChargeLow",    "system": "Power",       "subsystem": "Battery",       "unit": "Cell1",       "color": "Yellow"},
    {"name": "SensorFailure",       "system": "Sensors",     "subsystem": "IMU",           "unit": "Gyro",        "color": "Flashing Red"},
    {"name": "CANBusError",         "system": "Comms",       "subsystem": "CAN",           "unit": "Bus0",        "color": "Yellow"},
    {"name": "BrakeActuatorFault",  "system": "Brakes",      "subsystem": "ABS",           "unit": "FrontLeft",   "color": "Red"},
    {"name": "FuelLevelLow",        "system": "Fuel",        "subsystem": "Tank",          "unit": "MainTank",    "color": "Yellow"},
    {"name": "ThrottleBodyError",   "system": "Engine",      "subsystem": "Throttle",      "unit": "Actuator",    "color": "Red"},
    {"name": "GPSSignalLost",       "system": "Navigation",  "subsystem": "GPS",           "unit": "Receiver",    "color": "Green"},
    {"name": "CoolingFanStall",     "system": "Engine",      "subsystem": "Cooling",       "unit": "Fan",         "color": "Yellow"},
    {"name": "MotorControllerFault","system": "Drivetrain",  "subsystem": "MotorCtrl",     "unit": "MCU",         "color": "Flashing Red"},
    {"name": "HydraulicPressLow",   "system": "Hydraulics",  "subsystem": "Pump",          "unit": "HydPump1",    "color": "Red"},
    {"name": "WheelSpeedSensor",    "system": "Sensors",     "subsystem": "WheelSpeed",    "unit": "RearRight",   "color": "Yellow"},
    {"name": "AirFilterClog",       "system": "Engine",      "subsystem": "AirIntake",     "unit": "Filter",      "color": "Green"},
]


class CoreEventLiveMonitor(LiveMonitorPlugin):
    """Stub implementation that generates random events for development and testing."""

    def __init__(self, channels: list[str] | None = None):
        self._default_channels = channels or ["A", "B", "C", "D"]
        self._channels: list[str] = []
        self._start_time: datetime | None = None
        self._next_row_id = 1
        # open set-events awaiting a clear: key -> template dict
        self._open_sets: dict[str, dict] = {}

    def detect_system(self) -> bool:
        return True

    def get_channels(self) -> list[str]:
        return list(self._default_channels)

    def start_session(self, session_id: str, channels: list[str]) -> None:
        self._channels = list(channels)
        self._start_time = datetime.now(timezone.utc)
        self._next_row_id = 1
        self._open_sets = {}

    def stop_session(self) -> None:
        self._start_time = None
        self._open_sets = {}

    def poll_events(self) -> list[dict[str, Any]]:
        if self._start_time is None:
            return []

        now = datetime.now(timezone.utc)
        elapsed = (now - self._start_time).total_seconds()
        events: list[dict[str, Any]] = []
        count = random.choices([0, 1, 2, 3], weights=[40, 35, 18, 7])[0]

        for _ in range(count):
            # Prefer closing an open set if one exists
            if self._open_sets and random.random() < 0.45:
                key = random.choice(list(self._open_sets))
                tmpl = self._open_sets.pop(key)
                channel = key.split("|")[1]
                events.append(self._make_event(tmpl, [channel], "clear", now, elapsed))
            else:
                tmpl = random.choice(_SAMPLE_FAULTS)
                channel = random.choice(self._channels)
                key = f"{tmpl['name']}|{channel}"
                # Skip if this fault+channel is already open
                if key in self._open_sets:
                    continue
                self._open_sets[key] = tmpl
                events.append(self._make_event(tmpl, [channel], "set", now, elapsed))

        return events

    def _make_event(
        self,
        tmpl: dict,
        channels: list[str],
        set_clear: str,
        utctime: datetime,
        norm_time: float,
    ) -> dict[str, Any]:
        row_id = self._next_row_id
        self._next_row_id += 1
        return {
            "row_id": row_id,
            "name": tmpl["name"],
            "system": tmpl["system"],
            "subsystem": tmpl["subsystem"],
            "unit": tmpl["unit"],
            "color": tmpl["color"],
            "channels": channels,
            "set_clear": set_clear,
            "utctime": utctime.isoformat().replace("+00:00", "Z"),
            "norm_time": round(norm_time, 3),
        }
