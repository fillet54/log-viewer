import json
import time

from cinc.live.replay import SampleReplaySource
from cinc.live.session import SessionManager
from cinc.logs.store import LogStore
from cinc.plugins.text_log import TextLogType


def test_live_poll_batches_use_contiguous_row_ids(tmp_path):
    path = tmp_path / "sample.json"
    path.write_text(
        json.dumps(
            {
                "events": [
                    {"time": "2026-01-01T00:00:00Z", "message": "a"},
                    {"time": "2026-01-01T00:00:01Z", "message": "b"},
                ]
            }
        )
    )
    log_type = TextLogType()
    source = SampleReplaySource(path, speed=1, poll_interval=0.001)
    source.start("session")
    first = source.poll()
    second = source.poll()
    first_events = log_type.normalize_events({"events": first}, row_id_base=0)
    second_events = log_type.normalize_events(
        {"events": second}, row_id_base=len(first_events)
    )
    assert [event["row_id"] for event in first_events + second_events] == [1, 2]


def test_replay_is_deterministic(tmp_path):
    path = tmp_path / "sample.json"
    path.write_text(
        json.dumps(
            {
                "events": [
                    {"time": "2026-01-01T00:00:00Z", "message": "a"},
                    {"time": "2026-01-01T00:00:01Z", "message": "b"},
                ]
            }
        )
    )
    first = SampleReplaySource(path, speed=1, poll_interval=0.001)
    second = SampleReplaySource(path, speed=1, poll_interval=0.001)
    first.start("one")
    second.start("two")
    assert first.poll() == second.poll()
    assert first.poll() == second.poll()


def test_session_stop_start_again(tmp_path):
    path = tmp_path / "sample.json"
    path.write_text(
        json.dumps({"events": [{"time": "2026-01-01T00:00:00Z", "message": "a"}]})
    )
    store = LogStore(tmp_path / "cinc.sqlite")
    manager = SessionManager(
        store, TextLogType(), SampleReplaySource(path, poll_interval=0.001)
    )
    first = manager.start()
    time.sleep(0.02)
    manager.stop()
    assert store.get_record(first).status == "completed"
    second = manager.start()
    manager.stop()
    assert second != first
