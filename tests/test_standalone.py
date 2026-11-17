import json
import re
from html.parser import HTMLParser
from pathlib import Path

from cinc.standalone import (
    build_standalone,
    build_standalone_bundle,
    inline_script_code,
)


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


def test_standalone_from_log_folder(tmp_path):
    from fake_logs import write_log

    log = write_log(tmp_path / "data", rows=10)
    output = tmp_path / "out.html"
    build_standalone_bundle([log], output)
    html = output.read_text()
    assert '"core_event"' in html and '"text_log"' in html
    assert "handled request 9" in html


def test_standalone_from_single_jsonl(tmp_path):
    from fake_logs import write_log

    log = write_log(tmp_path / "data", rows=3)
    output = tmp_path / "out.html"
    build_standalone_bundle([log / "text_log.jsonl"], output)
    html = output.read_text()
    assert '"text_log"' in html and '"core_event"' not in html


HOSTILE = ["</SCRIPT><img src=x onerror=1>", "<!--<script>", "a & b > c"]


class _Scripts(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tags = []
        self.scripts = []
        self.injected = False
        self._in_script = False

    def handle_starttag(self, tag, attrs):
        self.tags.append(tag)
        if tag == "img" and dict(attrs).get("src") == "x":
            self.injected = True
        if tag == "script":
            self._in_script = True
            self.scripts.append("")

    def handle_endtag(self, tag):
        if tag == "script":
            self._in_script = False

    def handle_data(self, data):
        if self._in_script:
            self.scripts[-1] += data


def _parse(html):
    parser = _Scripts()
    parser.feed(html)
    return parser


def _page_data(parser):
    prefix = "window.CINC_PAGE_DATA = "
    (script,) = [s.strip() for s in parser.scripts if s.strip().startswith(prefix)]
    return json.loads(script[len(prefix):].rstrip(";"))


def _fake_folder(root):
    from fake_logs import write_log

    return write_log(root, rows=3)


def _hostile_log(root):
    from fake_logs import text_rows, write_log

    rows = text_rows(len(HOSTILE))
    for row, message in zip(rows, HOSTILE):
        row["message"] = message
    log = write_log(root, rows=3)
    (log / "text_log.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in rows)
    )
    return log


def _messages(page_data):
    return [
        e["message"] for e in page_data["logData"]["events"] if "message" in e
    ]


def test_standalone_page_data_cannot_break_out_of_its_script(tmp_path):
    clean = _parse(build_standalone(_fake_folder(tmp_path / "clean")))
    parser = _parse(build_standalone(_hostile_log(tmp_path / "hostile")))
    assert not parser.injected
    assert parser.tags.count("script") == clean.tags.count("script")
    assert _messages(_page_data(parser)) == HOSTILE


def test_served_page_data_cannot_break_out_of_its_script(tmp_path):
    from cinc.app import create_app

    log = _hostile_log(tmp_path)
    client = create_app(tmp_path).test_client()
    page = client.get(f"/logs/{log.name}").data.decode()
    parser = _parse(page)
    assert not parser.injected
    assert _messages(_page_data(parser)) == HOSTILE
    download = _parse(client.get(f"/logs/{log.name}/standalone.html").data.decode())
    assert not download.injected
    assert _messages(_page_data(download)) == HOSTILE


def test_inlined_vendor_code_has_no_unescapable_markup():
    vendor = Path(__file__).resolve().parents[1] / "src/cinc/static/vendor"
    for path in [*vendor.rglob("*.js"), *vendor.rglob("*.mjs")]:
        text = inline_script_code(path.read_text(encoding="utf-8"))
        assert "<!--" not in text, path
        assert not re.search(r"</script", text, re.IGNORECASE), path


def test_inline_script_code_is_case_insensitive():
    assert inline_script_code("a</ScRiPt>b") == "a<\\/ScRiPt>b"
