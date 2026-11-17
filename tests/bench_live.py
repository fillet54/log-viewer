"""Time single-row live posts against logs of growing size (not a pytest test).

python tests/bench_live.py [SIZES] [POSTS]

SIZES is a comma-separated list of stored row counts (default 1000,5000,20000,
50000); POSTS is how many single-row posts are timed at each size (default 10).
For each log type it writes a log folder with that many rows, makes it the live
log, warms the hub with one post, then prints the median time per post and the
time of a page load.
"""

from __future__ import annotations

import statistics
import sys
import tempfile
import time
from pathlib import Path

try:
    from cinc import LogWriter
except ImportError:  # run from a checkout without installing: use ../src
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
    from cinc import LogWriter

from cinc.core.registry import Registry
from cinc.live import LiveHub
from cinc.logdir import LogDir

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fake_logs import (  # noqa: E402
    CORE_EVENT_HEADER,
    TRACKS,
    core_event_rows,
    text_rows,
    track_rows,
)


def _track_rows(count: int, start: int = 0) -> list[dict]:
    """``count`` track samples (track_rows gives about 1.2 per second)."""
    rows = track_rows(count // 5 + 2, start // 5)
    return rows[:count]


TYPES = {
    "core_event": (core_event_rows, CORE_EVENT_HEADER),
    "text_log": (text_rows, {}),
    "track": (_track_rows, {"tracks": TRACKS}),
}


def bench(type_id: str, size: int, posts: int, registry: Registry) -> dict:
    make_rows, header = TYPES[type_id]
    with tempfile.TemporaryDirectory() as root:
        writer = LogWriter(Path(root), "bench", log_id="bench")
        log_path = Path(root) / "bench"
        if header:
            writer.header(type_id, header)
        writer.write(type_id, make_rows(size))
        hub = LiveHub(Path(root), registry)
        hub._current = LogDir(log_path)

        started = time.perf_counter()
        hub.post(type_id, make_rows(1, size))  # warm: builds the hub's cache
        warm = time.perf_counter() - started

        times = []
        for i in range(posts):
            row = make_rows(1, size + 1 + i)
            started = time.perf_counter()
            hub.post(type_id, row)
            times.append(time.perf_counter() - started)

        started = time.perf_counter()
        hub.page_data(hub.current)
        page = time.perf_counter() - started
    return {
        "post_ms": statistics.median(times) * 1000,
        "warm_ms": warm * 1000,
        "page_ms": page * 1000,
    }


def main(argv: list[str]) -> None:
    sizes = [int(s) for s in (argv[0] if argv else "1000,5000,20000,50000").split(",")]
    posts = int(argv[1]) if len(argv) > 1 else 10
    registry = Registry.discover()
    print(f"{'type':<12}{'rows':>8}{'post ms':>10}{'warm ms':>10}{'page ms':>10}")
    for type_id in TYPES:
        for size in sizes:
            result = bench(type_id, size, posts, registry)
            print(
                f"{type_id:<12}{size:>8}{result['post_ms']:>10.1f}"
                f"{result['warm_ms']:>10.1f}{result['page_ms']:>10.1f}",
                flush=True,
            )


if __name__ == "__main__":
    main(sys.argv[1:])
