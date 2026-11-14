"""Plugins: extending a type from another package, subclassing, pages,
commands, assets, and the Python API for producing logs."""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from cinc import LogWriter, build_standalone, track_sample, write_standalone
from cinc.app import create_app
from cinc.core.assemble import assemble_page_data
from cinc.core.bundle import log_dir_sections
from cinc.core.registry import Registry
from cinc.logdir import LogDir
from cinc.plugins import core_event, text_log
from cinc.plugins.core_event import CoreEventLogType, event

EXAMPLE = Path(__file__).resolve().parents[1] / "examples/acme_plugin/src"
if str(EXAMPLE) not in sys.path:
    sys.path.insert(0, str(EXAMPLE))

import acme_cinc  # noqa: E402

BUILTINS = {"core_event": core_event.setup, "text_log": text_log.setup}
T0 = datetime(2026, 3, 1, 9, tzinfo=timezone.utc)


def acme_registry():
    return Registry.from_plugins({**BUILTINS, "acme": acme_cinc.setup})


def test_extension_adds_modules_styles_config_and_augmenter(tmp_path):
    registry = acme_registry()
    log = LogDir(acme_cinc.write_demo_log(tmp_path, minutes=4))
    page = assemble_page_data(log_dir_sections(log, registry), registry)
    entry = page["logTypes"]["core_event"]
    assert entry["modules"] == [
        "cinc-plugins/cinc.plugins.core_event/static/row.js",
        "cinc-plugins/cinc.plugins.core_event/static/charts.js",
    ]
    (extension,) = entry["extensions"]
    assert extension["plugin"] == "acme"
    assert extension["modules"] == ["cinc-plugins/acme_cinc/static/owners.js"]
    assert "Avionics" in extension["config"]["owners"]
    assert any(".acme-bars" in style for style in page["view"]["styles"])
    owners = {
        e["owner"] for e in page["logData"]["events"] if e["log_type"] == "core_event"
    }
    assert owners == {"Electrical", "Thermal", "Avionics"}


def test_subclass_keeps_base_scripts_under_its_own_id():
    class PumpEvents(CoreEventLogType):
        id = "pump_event"
        name = "Pump Events"
        defaults = {
            "severities": [
                {"name": "OK", "tone": "green"},
                {"name": "Alarm", "tone": "red"},
            ]
        }

    registry = Registry.from_plugins({"pump": lambda ctx: ctx.add_log_type(PumpEvents)})
    rows = [
        event(T0, "Pump hot", system="Pump", channels=["A"], color="Alarm"),
        event(T0, "Pump odd", system="Pump", channels=["A"], color="Unknown"),
    ]
    page = assemble_page_data(
        [(registry.get("pump_event"), {"events": rows})], registry
    )
    entry = page["logTypes"]["pump_event"]
    assert all("cinc.plugins.core_event/static/" in m for m in entry["modules"])
    assert [s["name"] for s in entry["config"]["severities"]] == ["OK", "Alarm"]
    tones = [(e["color"], e["tone"]) for e in page["logData"]["events"]]
    assert tones == [("Alarm", "red"), ("OK", "green")]  # unknown -> lowest


def test_extending_a_missing_type_or_asset_fails_early():
    with pytest.raises(ValueError, match="no installed plugin provides"):
        Registry.from_plugins({"x": lambda ctx: ctx.extend("nope", config={})})
    with pytest.raises(ValueError, match="not found in package"):
        Registry.from_plugins(
            {**BUILTINS, "x": lambda ctx: ctx.add_script("missing.js")}
        )
    with pytest.raises(ValueError, match="taken"):
        Registry.from_plugins({"x": lambda ctx: ctx.add_command("serve", print)})


def test_plugin_pages_and_assets_are_served(tmp_path):
    client = create_app(tmp_path, registry=acme_registry()).test_client()
    page = client.get("/acme/")
    assert page.status_code == 200 and b"Open faults by owner" in page.data
    assert b'href="/acme/"' in client.get("/logs").data  # in the top bar
    script = client.get("/plugin-assets/acme_cinc/static/owners.js")
    assert script.status_code == 200 and script.mimetype == "text/javascript"
    assert (
        client.get("/plugin-assets/cinc.plugins.core_event/static/row.js").status_code
        == 200
    )
    for bad in (
        "/plugin-assets/acme_cinc/__init__.py",
        "/plugin-assets/acme_cinc/static/../__init__.py",
        "/plugin-assets/os/static/x.js",
    ):
        assert client.get(bad).status_code == 404
    demo = client.post("/acme/demo")
    assert demo.status_code == 302 and "/logs/" in demo.location


def test_standalone_inlines_plugin_modules_and_their_imports(tmp_path):
    path = acme_cinc.write_demo_log(tmp_path, minutes=2)
    html = build_standalone(path, "Demo", registry=acme_registry())
    for module in ("acme_cinc/static/owners.js", "acme_cinc/static/bars.js"):
        assert f'"cinc-plugins/{module}"' in html
    # owners.js's relative import now names the module id
    assert 'from \\"cinc-plugins/acme_cinc/static/bars.js\\"' in html
    assert '"aliases": {"logview/lib"' in html


def test_plugin_command(tmp_path, capsys):
    command = acme_registry().commands["acme-demo"]
    assert command.function([str(tmp_path / "out")]) == 0
    assert (tmp_path / "out").is_dir() and "standalone:" in capsys.readouterr().out


def test_writer_and_build_standalone_from_memory(tmp_path):
    with LogWriter(tmp_path, "Run 7", log_id="run-7", started=T0) as log:
        log.header("core_event", channels=["A"])
        log.write("core_event", event(T0, "Hot", system="Pump", channels=["A"]))
        log.write("track", [track_sample(T0, 1.0, 2.0), track_sample(T0, 1.1, 2.1)])
    assert log.path == tmp_path / "run-7"
    assert LogDir(log.path).header("core_event") == {"channels": ["A"]}
    assert LogDir(log.path).rows("track")[0]["time"] == "2026-03-01T09:00:00Z"

    html = build_standalone(
        {
            "core_event": {
                "channels": ["A"],
                "events": [event(T0, "Hot", system="P", channels=["A"])],
            }
        },
        "Memory",
    )
    assert "Memory" in html and '"core_event"' in html
    with pytest.raises(ValueError, match="Unknown log type"):
        build_standalone({"nope": {"events": []}})
    out = write_standalone(log.path, tmp_path / "p.html")
    assert "<!doctype html>" in out.read_text()
