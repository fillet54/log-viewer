"""Helpers for testing plugins."""

from __future__ import annotations

from typing import Any, Iterable


def assert_stream_matches(
    log_type: Any,
    payload: dict[str, Any],
    batch_sizes: Iterable[int],
    *,
    row_id_base: int = 0,
    row_id_step: int = 1,
) -> None:
    """Feed ``payload``'s rows to ``log_type.live_stream`` in batches and
    check that after every batch the events so far are exactly what
    ``normalize_events`` gives for the rows so far: every new event is
    right, and every earlier event that changed was returned as updated.

    ``batch_sizes`` is cycled (``[1]``: one row at a time; ``[len(rows)]``:
    all at once). ``cinc.NeedsRebuild`` from the stream propagates."""
    from .core.log_type import row_id as make_row_id

    header = {key: value for key, value in payload.items() if key != "events"}
    rows = list(payload.get("events") or [])
    sizes = [int(size) for size in batch_sizes]
    if not sizes or any(size < 1 for size in sizes):
        raise ValueError("batch_sizes must be positive")
    stream = log_type.live_stream(
        header, row_id_base=row_id_base, row_id_step=row_id_step
    )
    assert stream is not None, f"{log_type.id} has no live stream"

    def index_of(event: dict[str, Any]) -> int:
        return (event["row_id"] - row_id_base - 1) // row_id_step

    events: list[dict[str, Any]] = []
    done = 0
    batch = 0
    while done < len(rows):
        size = sizes[batch % len(sizes)]
        batch += 1
        chunk = rows[done : done + size]
        new, updated = stream.append(chunk)
        for event in updated:
            index = index_of(event)
            assert 0 <= index < len(events), f"update for unknown row {index}"
            events[index] = event
        for event in new:
            expected_id = make_row_id(len(events), row_id_base, row_id_step)
            assert event["row_id"] == expected_id, (
                f"new event has row_id {event['row_id']}, expected {expected_id}"
            )
            events.append(event)
        done += len(chunk)
        full = log_type.normalize_events(
            {**header, "events": rows[:done]},
            row_id_base=row_id_base,
            row_id_step=row_id_step,
        )
        assert len(events) == len(full), (
            f"after {done} rows: {len(events)} events, expected {len(full)}"
        )
        for index, (got, want) in enumerate(zip(events, full)):
            assert got == want, (
                f"after {done} rows, event {index} differs:\n"
                f"  stream: {got}\n  full:   {want}"
            )
