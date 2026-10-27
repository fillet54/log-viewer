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
