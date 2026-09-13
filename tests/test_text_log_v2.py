import io
import json
from pathlib import Path

from cinc.plugins.text_log import TextLogType


def test_text_sample_contract_and_config():
    log_type = TextLogType()
    payload = json.loads(
        Path("src/cinc/plugins/text_log/samples/sample.json").read_text()
    )
    events = log_type.normalize_events(payload)
    assert len(events) == 40
    assert events[0]["time"].endswith("Z")
    assert events[3]["data"]["attempt"] == 3
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
