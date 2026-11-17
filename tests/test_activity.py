"""Bookmarks and comments (src/cinc/static/state/activity.js), run in Node."""

from __future__ import annotations

from node_harness import needs_node, run_js

pytestmark = needs_node

SETUP = """
const { signal } = await import("preact/signals");
const { createActivityState } = await import(`${STATIC}/state/activity.js`);
const row = (i) => ({ row_id: i + 1, row_key: `t:${i}`, time: "2026-01-01" });
const rows = (n) => Array.from({ length: n }, (_, i) => row(i));
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
const event = activity.eventForKey("t:1");
const comment = await activity.addComment(event, "  looks wrong ");
return { comment, threads: activity.buildThreads(event).map((c) => c.body) };
""",
    )
    assert result["comment"]["body"] == "looks wrong"
    assert result["comment"]["row_key"] == "t:1"
    assert result["threads"] == ["looks wrong"]


def test_bookmarks_are_per_log(tmp_path):
    result = run_js(
        tmp_path,
        SETUP
        + """
const a = open({ logId: "A" });
a.setColor("t:1", 3);
await a.addComment("t:0", "note");
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
    assert result["a2"] == {"t:1": 3}
    assert result["a2Comments"] == ["note"]


def test_empty_keys_are_removed(tmp_path):
    result = run_js(
        tmp_path,
        SETUP
        + """
const a = open();
a.setColor("t:1", 1);
const set = Object.keys(storageSnapshot());
a.setColor("t:1", 0);
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
const color = a.setColor("t:0", 2);
return { enabled: a.enabled, color, keys: Object.keys(storageSnapshot()) };
""",
    )
    assert result == {"enabled": False, "color": 0, "keys": []}


def test_bookmark_stays_on_its_row_when_row_ids_change(tmp_path):
    # A live log gains a second type: every row id changes, row keys do not.
    result = run_js(
        tmp_path,
        SETUP
        + """
const events = signal(rows(3));
const reopen = () =>
  createActivityState({ logData: {}, allEventsSignal: events, logId: "A" });
const a = reopen();
const target = events.value[1];
a.setColor(target, 2);
await a.addComment(target, "here");
events.value = [
  ...events.value.map((e, i) => ({ ...e, row_id: 1 + i * 2 })),
  { row_id: 2, row_key: "u:0", time: "2026-01-01" },
];
const moved = events.value.find((e) => e.row_key === "t:1");
const reopened = reopen();
return {
  color: a.getColor(moved),
  marker: a.eventForKey("t:1").row_id,
  reopened: reopened.getColor(moved),
  thread: reopened.buildThreads(moved).map((c) => c.body),
};
""",
    )
    assert result == {"color": 2, "marker": 3, "reopened": 2, "thread": ["here"]}
