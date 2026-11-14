from pathlib import Path

import pytest

from cinc.plugins.core_event import CoreEventLogType
from cinc.plugins.text_log import TextLogType
from fake_logs import CHANNELS, core_event_rows, text_rows


def test_core_event_links_and_durations_survive_row_id_offset():
    payload = {"channels": CHANNELS, "events": core_event_rows(40)}
    events = CoreEventLogType().normalize_events(payload, row_id_base=40)
    ids = {event["row_id"] for event in events}
    links = [
        match["linkedRowId"]
        for event in events
        for match in event.get("matchSummary", {}).get("items", [])
    ]
    assert links and all(link in ids for link in links)
    assert any(
        match["durationSeconds"] > 0
        for event in events
        for match in event["pairedChannels"].values()
    )


def test_core_event_rejects_raw_events_without_channel_provenance():
    payload = {"channels": ["A"], "events": [{"row_id": 1, "set_clear": "set"}]}
    with pytest.raises(ValueError, match="requires channels"):
        CoreEventLogType().normalize_events(payload)


def test_text_log_contract_and_config():
    log_type = TextLogType()
    payload = {"events": text_rows(10)}
    events = log_type.normalize_events(payload)
    assert [event["row_id"] for event in events] == list(range(1, 11))
    assert events[0]["time"].endswith("Z")
    assert "message" in log_type.search_config(payload)["fields"]
    assert log_type.view_config(payload)["levels"][0] == "DEBUG"


def test_text_plugin_isolated():
    for path in Path("src/cinc/plugins/text_log").rglob("*"):
        if path.is_file() and path.suffix in {".py", ".js"}:
            assert "plugins.core_event" not in path.read_text()
