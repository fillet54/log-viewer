"""Bookmarks and comments (src/cinc/static/state/activity.js), run in Node."""

from __future__ import annotations

from node_harness import needs_node, run_js

pytestmark = needs_node

SETUP = """
const { signal } = await import("preact/signals");
const { createActivityState } = await import(`${STATIC}/state/activity.js`);
const rows = (n) =>
  Array.from({ length: n }, (_, i) => ({ row_id: i + 1, time: "2026-01-01T00:00Z" }));
const open = (opts = {}) => {
  const events = signal(opts.events || rows(3));
  const logData = { events: events.value };
  return createActivityState({ logData, allEventsSignal: events, logId: "A", ...opts });
};
"""


def test_add_comment_and_read_it_back(tmp_path):
    result = run_js(
        tmp_path,
        SETUP
        + """
const activity = open();
const comment = await activity.addComment(2, "  looks wrong ");
return { comment, threads: activity.buildThreads(2).map((c) => c.body) };
""",
    )
    assert result["comment"]["body"] == "looks wrong"
    assert result["threads"] == ["looks wrong"]


def test_bookmarks_are_per_log(tmp_path):
    result = run_js(
        tmp_path,
        SETUP
        + """
const a = open({ logId: "A" });
a.setColor(2, 3);
await a.addComment(1, "note");
const b = open({ logId: "B" });
const afterB = storageSnapshot();
const a2 = open({ logId: "A" });
return {
  inB: b.getAll(),
  bComments: b.comments.value.length,
  afterB,
  a2: a2.getAllWithColors(),
  a2Comments: a2.comments.value.map((c) => c.body),
};
""",
    )
    assert result["inB"] == []
    assert result["bComments"] == 0
    # opening B neither cleared A's entries nor wrote empty ones of its own
    assert set(result["afterB"]) == {"loglayout.bookmarks.A", "loglayout.comments.A"}
    assert result["a2"] == {"2": 3}
    assert result["a2Comments"] == ["note"]


def test_empty_keys_are_removed(tmp_path):
    result = run_js(
        tmp_path,
        SETUP
        + """
const a = open();
a.setColor(2, 1);
const set = Object.keys(storageSnapshot());
a.setColor(2, 0);
return { set, cleared: Object.keys(storageSnapshot()) };
""",
    )
    assert result["set"] == ["loglayout.bookmarks.A"]
    assert result["cleared"] == []


def test_legacy_keys_are_removed(tmp_path):
    result = run_js(
        tmp_path,
        SETUP
        + """
const a = open();
return { keys: Object.keys(storageSnapshot()), bookmarks: a.getAll() };
""",
        storage={
            "loglayout.bookmarks": '{"1": 2}',
            "loglayout.comments": '[{"row_id": 1, "body": "x"}]',
            "loglayout.density": '"compact"',
        },
    )
    assert result["keys"] == ["loglayout.density"]
    assert result["bookmarks"] == []


def test_no_log_id_disables_activity(tmp_path):
    result = run_js(
        tmp_path,
        SETUP
        + """
const a = open({ logId: null });
const color = a.setColor(1, 2);
return { enabled: a.enabled, color, keys: Object.keys(storageSnapshot()) };
""",
    )
    assert result == {"enabled": False, "color": 0, "keys": []}
