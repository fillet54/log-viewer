import json

import pytest

from cinc.app import create_app
from fake_logs import CHANNELS, core_event_rows, text_rows, write_log


def make(tmp_path):
    app = create_app(tmp_path)
    return app, app.test_client(), app.extensions["cinc"]["hub"]


def test_posting_without_start_creates_a_log_on_disk(tmp_path):
    app, client, hub = make(tmp_path)
    response = client.post(
        "/api/events", json={"type": "text_log", "events": text_rows(3)}
    )
    assert response.status_code == 202
    log_id = response.get_json()["log"]
    lines = (tmp_path / log_id / "text_log.jsonl").read_text().splitlines()
    assert [json.loads(line) for line in lines] == text_rows(3)
    assert client.get("/").headers["Location"].endswith(f"/logs/{log_id}")
    page = client.get(f"/logs/{log_id}")
    assert page.status_code == 200
    assert b'"live":{"logId"' in page.data


def test_start_new_log_ends_live_for_the_old_one(tmp_path):
    app, client, hub = make(tmp_path)
    first = client.post("/api/logs", json={"name": "First"}).get_json()["id"]
    client.post("/api/events", json={"type": "text_log", "event": text_rows(1)[0]})
    second = client.post("/api/logs", json={"name": "Second"}).get_json()["id"]
    client.post("/api/events", json={"type": "text_log", "events": text_rows(1)})
    assert first != second
    assert b'"live"' not in client.get(f"/logs/{first}").data
    history = client.get("/logs").data.decode()
    assert history.index("Second") < history.index("First")


def test_history_shows_tool_written_logs(tmp_path):
    write_log(tmp_path, rows=4, name="From a tool")
    app, client, hub = make(tmp_path)
    assert "From a tool" in client.get("/logs").data.decode()
    assert client.get("/logs/fake-20260101").status_code == 200
    download = client.get("/logs/fake-20260101/standalone.html")
    assert download.status_code == 200 and b"CINC_PAGE_DATA" in download.data


def test_bad_posts_are_rejected_and_not_persisted(tmp_path):
    app, client, hub = make(tmp_path)
    assert (
        client.post("/api/events", json={"type": "nope", "events": []}).status_code
        == 400
    )
    bad = client.post(
        "/api/events", json={"type": "core_event", "events": [{"set_clear": "set"}]}
    )
    assert bad.status_code == 400 and "channels" in bad.get_json()["error"]
    assert hub.current is None and not list(tmp_path.iterdir())


def test_stream_resumes_after_position_and_reloads_on_new_type(tmp_path):
    app, client, hub = make(tmp_path)
    hub.post("core_event", core_event_rows(2), {"channels": CHANNELS})  # seq 1
    hub.post("core_event", core_event_rows(2, 2))  # seq 2
    hub.post("core_event", core_event_rows(2, 4))  # seq 3

    subscriber = hub.subscribe(hub.current.id, after=1)
    resumed = [subscriber.get_nowait(), subscriber.get_nowait()]
    assert [m["seq"] for m in resumed] == [2, 3]
    assert resumed[0]["type"] == "events"
    assert [e["row_id"] for e in resumed[0]["events"]] == [3, 4]

    hub.post("text_log", text_rows(1))  # first text_log rows: needs config
    assert subscriber.get_nowait()["type"] == "reload"
    hub.start("next")
    assert subscriber.get_nowait() == {"type": "log", "logId": hub.current.id}


def test_live_row_ids_never_collide_with_page_ids(tmp_path):
    app, client, hub = make(tmp_path)
    hub.post("text_log", text_rows(5))
    page = hub.page_data(hub.current)
    page_ids = {e["row_id"] for e in page["logData"]["events"]}
    hub.post("text_log", text_rows(2, 5))
    subscriber = hub.subscribe(hub.current.id, after=page["live"]["seq"])
    live_ids = {e["row_id"] for e in subscriber.get_nowait()["events"]}
    assert live_ids and not live_ids & page_ids


def test_set_clear_pairing_spans_posts(tmp_path):
    app, client, hub = make(tmp_path)
    rows = core_event_rows(2)  # one SET, then its CLEAR five seconds later
    hub.post("core_event", rows[:1], {"channels": CHANNELS})
    subscriber = hub.subscribe(
        hub.current.id, after=hub.page_data(hub.current)["live"]["seq"]
    )
    hub.post("core_event", rows[1:])
    message = subscriber.get_nowait()
    [clear] = message["events"]
    [set_row] = message["updates"]
    assert clear["set_clear"] == "clear" and set_row["set_clear"] == "set"
    assert clear["matchSummary"]["items"][0]["linkedRowId"] == set_row["row_id"]
    assert set_row["matchSummary"]["items"][0]["linkedRowId"] == clear["row_id"]
    assert set_row["matchSummary"]["items"][0]["label"] == "5s"


def test_live_events_match_a_fresh_page_load(tmp_path):
    app, client, hub = make(tmp_path)
    hub.post("core_event", core_event_rows(3), {"channels": CHANNELS})
    hub.post("text_log", text_rows(2))  # new type: viewers reload
    page = hub.page_data(hub.current)
    subscriber = hub.subscribe(hub.current.id, after=page["live"]["seq"])
    hub.post("core_event", core_event_rows(3, 3))
    hub.post("text_log", text_rows(2, 2))
    live = {e["row_id"]: e for e in page["logData"]["events"]}
    for _ in range(2):
        message = subscriber.get_nowait()
        live.update({e["row_id"]: e for e in message["updates"] + message["events"]})
    fresh = hub.page_data(hub.current)["logData"]["events"]
    assert live == {e["row_id"]: e for e in fresh}


def test_row_key_survives_a_live_log_gaining_a_type(tmp_path):
    app, client, hub = make(tmp_path)
    hub.post("text_log", text_rows(3))
    before = hub.page_data(hub.current)["logData"]["events"]
    hub.post("core_event", core_event_rows(2), {"channels": CHANNELS})
    after = hub.page_data(hub.current)["logData"]["events"]
    by_key = {e["row_key"]: e for e in after}
    for event in before:
        moved = by_key[event["row_key"]]
        assert moved["message"] == event["message"]
    assert {e["row_id"] for e in before} != {
        by_key[e["row_key"]]["row_id"] for e in before
    }


def test_a_bad_header_is_rejected_and_not_saved(tmp_path):
    app, client, hub = make(tmp_path)
    bad = client.post(
        "/api/events", json={"type": "core_event", "header": {"severities": "nope"}}
    )
    assert bad.status_code == 400
    assert hub.current is None or not (hub.current.path / "core_event.json").exists()
    good = client.post(
        "/api/events", json={"type": "core_event", "events": core_event_rows(1)}
    )
    assert good.status_code == 202
    log_id = good.get_json()["log"]
    assert client.get(f"/logs/{log_id}").status_code == 200


def test_unreadable_logs_are_400_not_500(tmp_path):
    log = write_log(tmp_path, rows=2)
    (log / "mystery.jsonl").write_text('{"x": 1}\n')
    app, client, hub = make(tmp_path)
    assert client.get(f"/logs/{log.name}").status_code == 400
    download = client.get(f"/logs/{log.name}/standalone.html")
    assert download.status_code == 400 and b"mystery" in download.data
    empty = tmp_path / "empty-log"
    empty.mkdir()
    (empty / "log.json").write_text("{}")
    assert client.get("/logs/empty-log/standalone.html").status_code == 404


def test_stream_ignores_a_bad_after(tmp_path):
    app, client, hub = make(tmp_path)
    hub.post("text_log", text_rows(1))  # so the stream has a message right away
    response = client.get(f"/api/stream?log={hub.current.id}&after=x", buffered=False)
    assert response.status_code == 200
    assert next(response.response).startswith(b"data: ")
    response.close()


def test_api_posts_must_be_json(tmp_path):
    app, client, hub = make(tmp_path)
    response = client.post(
        "/api/logs", data='{"name": "x"}', headers={"Content-Type": "text/plain"}
    )
    assert response.status_code == 415
    assert hub.current is None and not any(tmp_path.iterdir())
    form = client.post("/api/events", data={"type": "text_log"})
    assert form.status_code == 415
    assert client.post("/api/logs", json={"name": "x"}).status_code == 201


def test_closing_the_hub_ends_open_streams(tmp_path):
    import threading
    import time

    app, client, hub = make(tmp_path)
    hub.post("text_log", text_rows(1))
    url = f"/api/stream?log={hub.current.id}&after={hub._seq}"
    threading.Timer(0.2, hub.close).start()
    started = time.monotonic()
    # the test client reads the first chunk, which waits until close()
    response = client.get(url, buffered=False)
    assert b"".join(response.response) == b""  # no ping, no message
    assert time.monotonic() - started < 2
    assert not hub._subscribers


def test_live_page_from_memory_equals_page_from_disk(tmp_path):
    from cinc.core.assemble import assemble_page_data
    from cinc.core.bundle import log_dir_sections

    app, client, hub = make(tmp_path)
    hub.post("core_event", core_event_rows(3), {"channels": CHANNELS})
    hub.post("text_log", text_rows(2))
    hub.post("core_event", core_event_rows(3, 3))
    hub.post("text_log", text_rows(2, 2))
    page = hub.page_data(hub.current)
    assert page.pop("live") == {"logId": hub.current.id, "seq": hub._seq}
    registry = hub.registry
    assert page == assemble_page_data(log_dir_sections(hub.current, registry), registry)


def test_a_warm_post_reads_nothing_from_disk(tmp_path, monkeypatch):
    from cinc.logdir import LogDir

    app, client, hub = make(tmp_path)
    hub.post("core_event", core_event_rows(2), {"channels": CHANNELS})
    hub.post("core_event", core_event_rows(1, 2))  # warm the cache

    def no_reads(*args, **kwargs):
        raise AssertionError("read from disk")

    for name in ("types", "header", "rows", "meta"):
        monkeypatch.setattr(LogDir, name, no_reads)
    hub.post("core_event", core_event_rows(1, 3))
    hub.page_data(hub.current)


@pytest.mark.parametrize("type_id", ["text_log", "track"])
def test_rows_independent_types_post_batch_by_batch(tmp_path, type_id):
    from cinc.core.assemble import assemble_page_data
    from cinc.core.bundle import log_dir_sections
    from fake_logs import TRACKS, track_rows

    make_rows = {"text_log": text_rows, "track": track_rows}[type_id]
    header = {"tracks": TRACKS} if type_id == "track" else None
    app, client, hub = make(tmp_path)
    assert hub.registry.get(type_id).rows_independent
    hub.post("core_event", core_event_rows(2), {"channels": CHANNELS})
    hub.post(type_id, make_rows(1), header)  # new type: the page reloads
    subscriber = hub.subscribe(hub.current.id, after=hub._seq)
    start = 1
    for size in (1, 7, 12):
        hub.post(type_id, make_rows(size, start))
        start += size
    messages = [subscriber.get_nowait() for _ in range(3)]
    assert all(m["type"] == "events" and not m["updates"] for m in messages)

    live = hub.page_data(hub.current)
    live.pop("live")
    registry = hub.registry
    disk = assemble_page_data(log_dir_sections(hub.current, registry), registry)
    assert live == disk
    streamed = [e for m in messages for e in m["events"]]
    on_page = [e for e in disk["logData"]["events"] if e["log_type"] == type_id]
    assert streamed == on_page[1:]


def test_rows_independent_types_normalize_only_the_batch(tmp_path, monkeypatch):
    from cinc.plugins.text_log import TextLogType

    sizes = []
    original = TextLogType.normalize_events

    def counting(self, payload, **kwargs):
        sizes.append(len(payload["events"]))
        return original(self, payload, **kwargs)

    monkeypatch.setattr(TextLogType, "normalize_events", counting)
    app, client, hub = make(tmp_path)
    hub.post("text_log", text_rows(5))
    hub.post("text_log", text_rows(2, 5))
    hub.post("text_log", text_rows(1, 7))
    assert sizes == [5, 2, 1]
    ids = [e["row_id"] for e in hub.page_data(hub.current)["logData"]["events"]]
    assert ids == list(range(1, 9))


def _page_matches_disk(hub):
    from cinc.core.assemble import assemble_page_data
    from cinc.core.bundle import log_dir_sections

    live = hub.page_data(hub.current)
    live.pop("live")
    registry = hub.registry
    assert live == assemble_page_data(log_dir_sections(hub.current, registry), registry)
    return live


def _apply(messages, events):
    """What the browser does with stream messages."""
    for message in messages:
        by_id = {e["row_id"]: e for e in message["updates"]}
        events = [by_id.get(e["row_id"], e) for e in events] + message["events"]
    return events


def test_core_event_streams_one_row_at_a_time(tmp_path, monkeypatch):
    from cinc.plugins.core_event import CoreEventLogType

    app, client, hub = make(tmp_path)
    rows = core_event_rows(40)
    hub.post("core_event", rows[:2], {"channels": CHANNELS})
    hub.post("text_log", text_rows(3))
    start = hub.page_data(hub.current)
    subscriber = hub.subscribe(hub.current.id, after=start["live"]["seq"])

    calls = []
    original = CoreEventLogType.normalize_events
    monkeypatch.setattr(
        CoreEventLogType,
        "normalize_events",
        lambda self, *a, **k: calls.append(1) or original(self, *a, **k),
    )
    for row in rows[2:]:
        hub.post("core_event", [row])
    assert not calls  # no full re-derive per post

    messages = [subscriber.get_nowait() for _ in rows[2:]]
    assert any(m["updates"] for m in messages)
    page = _page_matches_disk(hub)
    core = [e for e in page["logData"]["events"] if e["log_type"] == "core_event"]
    initial = [e for e in start["logData"]["events"] if e["log_type"] == "core_event"]
    assert _apply(messages, initial) == core


def test_core_event_rebuilds_on_an_out_of_order_row(tmp_path, caplog):
    app, client, hub = make(tmp_path)
    rows = core_event_rows(6)
    hub.post("core_event", rows[:3], {"channels": CHANNELS})
    hub.post("core_event", rows[3:4])
    late = dict(rows[4], utctime=rows[0]["utctime"])
    with caplog.at_level("WARNING", logger="cinc.live"):
        hub.post("core_event", [late])
    assert "earlier than the last row" in caplog.text
    hub.post("core_event", rows[5:])  # a fresh stream, replayed
    _page_matches_disk(hub)


def test_core_event_header_change_rebuilds(tmp_path):
    app, client, hub = make(tmp_path)
    hub.post("core_event", core_event_rows(4), {"channels": CHANNELS})
    hub.post("core_event", core_event_rows(1, 4))
    severities = [{"name": "Green", "tone": "green"}, {"name": "Red", "tone": "red"}]
    hub.post("core_event", [], {"channels": CHANNELS, "severities": severities})
    hub.post("core_event", core_event_rows(3, 5))
    page = _page_matches_disk(hub)
    assert {e["color"] for e in page["logData"]["events"]} <= {"Green", "Red"}
