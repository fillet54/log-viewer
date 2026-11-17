import pytest

from cinc.logdir import LogDir, list_logs
from fake_logs import write_log


def test_roundtrip(tmp_path):
    log = LogDir.create(tmp_path, "Run 1")
    log.set_header("core_event", {"channels": ["A"]})
    log.append("core_event", [{"x": 1}, {"x": 2}])
    log.append("text_log", [{"y": 1}])
    assert log.meta()["name"] == "Run 1"
    assert log.types() == ["core_event", "text_log"]
    assert log.payloads()[0] == (
        "core_event",
        {"channels": ["A"], "events": [{"x": 1}, {"x": 2}]},
    )
    assert log.row_count() == 3


def test_unfinished_last_line_is_skipped(tmp_path):
    log = LogDir.create(tmp_path)
    (log.path / "text_log.jsonl").write_text('{"a": 1}\n{"a": 2')
    assert log.rows("text_log") == [{"a": 1}]


def test_bad_line_reports_location(tmp_path):
    log = LogDir.create(tmp_path)
    (log.path / "text_log.jsonl").write_text("{}\nnope\n")
    with pytest.raises(ValueError, match="text_log.jsonl:2"):
        log.rows("text_log")


def test_list_logs_newest_first_and_includes_tool_written(tmp_path):
    written = write_log(tmp_path)  # started 2026-01-01
    created = LogDir.create(tmp_path)  # started now
    (tmp_path / "not-a-log").mkdir()
    assert [log.id for log in list_logs(tmp_path)] == [created.id, written.name]


def test_a_corrupt_log_json_does_not_break_other_logs(tmp_path):
    from cinc.app import create_app

    good = write_log(tmp_path)
    bad = LogDir.create(tmp_path, "Bad")
    (bad.path / "log.json").write_text("{not json")
    (bad.path / "text_log.jsonl").write_text("")
    meta = bad.meta()
    assert meta["name"] == bad.id and "log.json" in meta["error"]
    client = create_app(tmp_path).test_client()
    history = client.get("/logs")
    assert history.status_code == 200
    assert "unreadable log.json" in history.data.decode()
    assert client.get(f"/logs/{good.name}").status_code == 200
