import json

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
