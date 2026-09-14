import io
import json
from pathlib import Path

import pytest

from cinc.plugins.text_log import TextLogType


def test_core_event_links_and_durations_survive_row_id_offset():
    from cinc.plugins.core_event import CoreEventLogType

    payload = json.loads(
        Path("src/cinc/plugins/core_event/samples/sample-3ch.json").read_text()
    )
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
    from cinc.plugins.core_event import CoreEventLogType

    payload = {"channels": ["A"], "events": [{"row_id": 1, "set_clear": "set"}]}
    with pytest.raises(ValueError, match="requires channels"):
        CoreEventLogType().normalize_events(payload)


def test_text_sample_contract_and_config():
    log_type = TextLogType()
    payload = json.loads(
        Path("src/cinc/plugins/text_log/samples/sample.json").read_text()
    )
    events = log_type.normalize_events(payload)
    assert len(events) == 2500
    assert events[0]["time"].endswith("Z")
    assert events[0]["data"]["attempt"] == 1
    assert log_type.search_config(payload)["labelField"] == "message"
    assert log_type.view_config(payload)["timelineViews"] == ["levels"]


def test_text_upload_parser():
    source = io.BytesIO(b"2026-01-01T00:00:00Z ERROR api failed")
    name, payload = TextLogType().parse_import(file=source)
    assert name == "text log"
    assert payload["lines"][0]["level"] == "ERROR"


def test_text_plugin_isolated():
    for path in Path("src/cinc/plugins/text_log").rglob("*"):
        if path.is_file() and path.suffix in {".py", ".js"}:
            assert "plugins.core_event" not in path.read_text()
