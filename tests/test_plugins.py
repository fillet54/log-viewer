from __future__ import annotations

import json
from pathlib import Path

from eventlog2.plugin_manager import build_page_data, get_plugin, list_plugins
from eventlog2.standalone import build_page_data_script


def test_builtin_plugin_is_discoverable() -> None:
    plugin_ids = [plugin.plugin_id for plugin in list_plugins()]
    assert "core-event" in plugin_ids
    assert get_plugin("core-event").plugin_name == "Core Event"


def test_core_event_plugin_builds_normalized_page_data() -> None:
    page_data = build_page_data(
        "core-event",
        {
            "start": "2026-04-03T08:00:00Z",
            "end": "2026-04-03T08:10:00Z",
            "events": [
                {
                    "row_id": 1,
                    "norm_time": 0,
                    "utctime": "2026-04-03T08:00:00Z",
                    "name": "Power Bus Drift",
                    "color": "Yellow",
                    "system": "Power",
                    "subsystem": "Distribution",
                    "unit": "PDU-1",
                    "code": "PWR-214",
                    "set_clear": "set",
                    "channels": ["A", "B"],
                },
                {
                    "row_id": 2,
                    "norm_time": 15,
                    "utctime": "2026-04-03T08:00:15Z",
                    "name": "Power Bus Drift",
                    "color": "Yellow",
                    "system": "Power",
                    "subsystem": "Distribution",
                    "unit": "PDU-1",
                    "code": "PWR-214",
                    "set_clear": "clear",
                    "channels": ["A", "B"],
                },
            ],
        },
    )

    assert page_data["plugin"]["id"] == "core-event"
    assert page_data["logData"]["pluginId"] == "core-event"
    assert page_data["logData"]["events"][0]["matchSummary"]["collapsed"] is True
    assert page_data["logData"]["events"][0]["matchSummary"]["items"][0]["label"] == "15s"
    assert page_data["logData"]["events"][0]["rowDisplay"]["prefix"] == "[PWR-Y-214]"
    assert page_data["logData"]["events"][0]["rowDisplay"]["location"] == "(Power/Distribution/PDU-1)"
    assert page_data["logData"]["events"][0]["rowDisplay"]["hasData"] is False
    assert page_data["logData"]["channels"] == ["A", "B"]
    assert page_data["logData"]["channelCount"] == 2
    assert page_data["view"]["rowSettings"]["channels"] == ["A", "B"]
    assert "<template" in page_data["view"]["rowTemplate"]
    assert len(page_data["view"]["scripts"]) == 2
    assert "registerPluginChartType" in page_data["view"]["scripts"][1]


def test_core_event_plugin_supports_variable_channel_catalog() -> None:
    page_data = build_page_data(
        "core-event",
        {
            "channelCount": 6,
            "channels": ["1", "2", "3", "4", "5", "6"],
            "events": [
                {
                    "row_id": 1,
                    "norm_time": 0,
                    "name": "Bus Example",
                    "set_clear": "set",
                    "channels": ["2", "5"],
                }
            ],
        },
    )

    assert page_data["logData"]["channels"] == ["1", "2", "3", "4", "5", "6"]
    assert page_data["logData"]["channelCount"] == 6
    assert page_data["view"]["rowSettings"]["channels"] == ["1", "2", "3", "4", "5", "6"]
    assert page_data["logData"]["events"][0]["channels"] == ["2", "5"]


def test_core_event_plugin_can_use_channel_count_header() -> None:
    page_data = build_page_data(
        "core-event",
        {
            "channelCount": 6,
            "events": [
                {
                    "row_id": 1,
                    "norm_time": 0,
                    "name": "Bus Example",
                    "set_clear": "set",
                    "channels": ["B", "E"],
                }
            ],
        },
    )

    assert page_data["logData"]["channels"] == ["A", "B", "C", "D", "E", "F"]
    assert page_data["logData"]["channelCount"] == 6
    assert page_data["view"]["rowSettings"]["channels"] == ["A", "B", "C", "D", "E", "F"]
    assert page_data["logData"]["events"][0]["channels"] == ["B", "E"]


def test_page_data_script_emits_json_assignment() -> None:
    script = build_page_data_script({"plugin": {"id": "core-event"}, "logData": {"events": []}})
    assert script.startswith("window.EVENTLOG2_PAGE_DATA = ")
    payload = json.loads(script.removeprefix("window.EVENTLOG2_PAGE_DATA = ").removesuffix(";"))
    assert payload["plugin"]["id"] == "core-event"


def test_plugin_accepts_simple_page_data_wrapper(tmp_path: Path) -> None:
    plugin = get_plugin("core-event")
    source = tmp_path / "wrapped.js"
    source.write_text(
        'window.EVENTLOG2_PAGE_DATA = {"plugin":"core-event","payload":{"events":[]}};',
        encoding="utf-8",
    )

    payload = plugin.read_payload_file(source)

    assert payload == {"events": []}
