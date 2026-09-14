import json

from cinc.standalone import build_standalone_bundle


def test_mixed_standalone_bundle_embeds_plugin_assets(tmp_path):
    core = tmp_path / "core.json"
    text = tmp_path / "text.json"
    core.write_text(
        json.dumps(
            {
                "channels": ["A"],
                "events": [
                    {
                        "utctime": "2026-01-01T00:00:00Z",
                        "channels": ["A"],
                        "system": "Power",
                        "set_clear": "set",
                    }
                ],
            }
        )
    )
    text.write_text(
        json.dumps(
            {
                "lines": [
                    {
                        "ts": "2026-01-01T00:00:01Z",
                        "level": "INFO",
                        "logger": "demo",
                        "message": "ready",
                    }
                ]
            }
        )
    )
    output = tmp_path / "mixed.html"
    build_standalone_bundle([core, text], output)
    html = output.read_text()
    assert '"core_event"' in html and '"text_log"' in html
    assert "registerTimelineView" in html
    assert "severity" in html and "levels" in html
    assert 'src="http' not in html and 'href="http' not in html
