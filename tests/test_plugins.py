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


def test_a_broken_plugin_is_skipped_with_a_warning(monkeypatch, capsys):
    from cinc import __main__ as cli
    from cinc.core import registry as registry_module
    from cinc.plugins import core_event, text_log

    class Point:
        def __init__(self, name, load):
            self.name, self.load = name, load

    def broken(ctx):
        ctx.add_log_type(TextLogType)  # registered, then undone
        raise RuntimeError("boom")

    def bad_import():
        raise ImportError("no module named acme")

    points = [
        Point("core_event", lambda: core_event.setup),
        Point("broken", lambda: broken),
        Point("missing", bad_import),
        Point("text_log", lambda: text_log.setup),
    ]
    monkeypatch.setattr(registry_module, "entry_points", lambda group: points)
    registry = registry_module.Registry.discover()
    assert [plugin for plugin, _ in registry.failed] == ["broken", "missing"]
    assert set(registry.plugins) == {"core_event", "text_log"}
    assert registry.get("text_log") is not None
    warning = "plugin 'broken' failed to load: RuntimeError: boom"
    assert warning in capsys.readouterr().err

    assert cli.main(["types"]) is None
    assert "text_log" in capsys.readouterr().out
    cli.main(["plugins"])
    assert "missing  FAILED TO LOAD: ImportError" in capsys.readouterr().out
