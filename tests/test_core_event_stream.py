"""core_event's live stream gives exactly the events of a full normalization."""

from __future__ import annotations

import random

import pytest

from cinc import NeedsRebuild
from cinc.plugins.core_event import CoreEventLogType
from cinc.testing import assert_stream_matches
from fake_logs import CHANNELS, CORE_EVENT_HEADER, core_event_rows


def _payload(rows, **header):
    return {**CORE_EVENT_HEADER, **header, "events": rows}


@pytest.mark.parametrize("sizes", [[1], [7], [10_000], [3, 1, 5]])
def test_fake_log_in_batches(sizes):
    assert_stream_matches(CoreEventLogType(), _payload(core_event_rows(120)), sizes)


@pytest.mark.parametrize("base,step", [(0, 1), (1, 3)])
def test_row_ids_follow_the_slot(base, step):
    assert_stream_matches(
        CoreEventLogType(),
        _payload(core_event_rows(30)),
        [4],
        row_id_base=base,
        row_id_step=step,
    )


def _random_rows(seed, count=200):
    """SETs and CLEARs of a few faults on random channels, in time order,
    with ties and CLEARs that have no open SET."""
    rng = random.Random(seed)
    rows = []
    second = 0
    for _ in range(count):
        second += rng.choice([0, 0, 1, 2, 5])
        rows.append(
            {
                "utctime": f"2026-01-01T00:{second // 60 % 60:02d}:{second % 60:02d}Z",
                "system": "Power",
                "unit": rng.choice(["PDU-1", "PDU-2"]),
                "code": rng.choice(["P1", "P2", "P3"]),
                "set_clear": rng.choice(["set", "set", "clear"]),
                "channels": rng.sample(CHANNELS, rng.randint(1, len(CHANNELS))),
                "color": rng.choice(["Red", "Yellow", "Green"]),
            }
        )
    return rows


@pytest.mark.parametrize("seed", range(8))
@pytest.mark.parametrize("sizes", [[1], [7], [10_000]])
def test_random_set_clear_sequences(seed, sizes):
    rows = _random_rows(seed, count=120)
    assert_stream_matches(CoreEventLogType(), _payload(rows), sizes)


def test_earlier_row_needs_rebuild():
    rows = core_event_rows(4)
    rows[3]["utctime"] = rows[0]["utctime"]
    with pytest.raises(NeedsRebuild, match="earlier"):
        assert_stream_matches(CoreEventLogType(), _payload(rows), [1])


def test_new_channel_without_declared_channels_needs_rebuild():
    rows = core_event_rows(4)
    rows[3]["channels"] = ["Z"]
    payload = {"events": rows}
    with pytest.raises(NeedsRebuild, match="new channel 'Z'"):
        assert_stream_matches(CoreEventLogType(), payload, [1])
    # all at once, the catalog comes from the rows themselves
    assert_stream_matches(CoreEventLogType(), payload, [4])


def test_late_first_utctime_needs_rebuild():
    rows = core_event_rows(3)
    rows[0].pop("utctime")
    rows[0]["norm_time"] = 0
    with pytest.raises(NeedsRebuild, match="utctime"):
        assert_stream_matches(CoreEventLogType(), _payload(rows), [1])


def test_a_bad_row_leaves_the_stream_usable():
    stream = CoreEventLogType().live_stream(CORE_EVENT_HEADER)
    rows = core_event_rows(4)
    stream.append(rows[:2])
    with pytest.raises(ValueError, match="channels"):
        stream.append([{"utctime": rows[2]["utctime"]}])
    new, updated = stream.append(rows[2:])
    assert [e["row_id"] for e in new] == [3, 4]
