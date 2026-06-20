from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from eventlog2.logs.store import LogStore


def test_sqlite_store_persists_records_and_events(tmp_path: Path) -> None:
    store = LogStore(tmp_path / "eventlog2.sqlite")
    started_at = datetime(2026, 4, 3, 8, 0, tzinfo=timezone.utc)

    record = store.create_log(
        "core_event",
        "core-event",
        "Example",
        {"event_count": 0, "channels": ["A"], "payload_header": {"channels": ["A"]}},
        source="import",
        status="completed",
        started_at=started_at,
    )
    store.append_events(
        record.id,
        [
            {
                "row_id": 1,
                "time": "2026-04-03T08:00:00Z",
                "utctime": "2026-04-03T08:00:00Z",
                "name": "Power Bus Drift",
            }
        ],
    )

    stored = store.get_record("core_event", record.id)
    assert stored is not None
    assert stored.metadata["event_count"] == 1
    assert store.get_events(record.id)[0]["time"] == "2026-04-03T08:00:00Z"

    records, total = store.list_records("core_event", search="example")
    assert total == 1
    assert records[0].id == record.id

    assert store.delete(record.id) is True
    assert store.get_record(record.id) is None


def test_sqlite_store_finds_active_live_record(tmp_path: Path) -> None:
    store = LogStore(tmp_path / "eventlog2.sqlite")
    record = store.create_log(
        "core_event",
        "core-event",
        "Live capture",
        {"event_count": 0},
        source="live",
        status="active",
        started_at=datetime(2026, 4, 3, 8, 0, tzinfo=timezone.utc),
    )

    active = store.get_active_live_record("core_event")
    assert active is not None
    assert active.id == record.id

    store.complete_log(record.id)
    assert store.get_active_live_record("core_event") is None
