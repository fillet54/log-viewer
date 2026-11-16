"""Bookmarks and comments (src/cinc/static/state/activity.js), run in Node."""

from __future__ import annotations

from node_harness import needs_node, run_js

pytestmark = needs_node

SETUP = """
const { signal } = await import("preact/signals");
const { createActivityState } = await import(`${STATIC}/state/activity.js`);
const rows = (n) => Array.from({ length: n }, (_, i) => ({ row_id: i + 1, time: "2026-01-01T00:00:00Z" }));
const open = (opts = {}) => {
  const events = signal(opts.events || rows(3));
  return createActivityState({ logData: { events: events.value }, allEventsSignal: events, ...opts });
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
