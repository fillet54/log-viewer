"""Live updates in the browser store (src/cinc/static/state/viewer-store.js)."""

from __future__ import annotations

import json

from cinc.core.assemble import assemble_page_data
from cinc.plugins.text_log import TextLogType
from fake_logs import text_rows
from node_harness import needs_node, run_js

pytestmark = needs_node


def test_out_of_order_live_events_land_in_page_order(tmp_path):
    # Two text logs whose rows interleave, with sub-second times that sort
    # wrongly as strings.
    first = text_rows(6)
    second = text_rows(6)
    for i, row in enumerate(second):
        row["ts"] = row["ts"].replace("Z", f".{i + 1}Z")

    class Other(TextLogType):
        id = "other_log"

    sections = [(TextLogType(), {"lines": first}), (Other(), {"lines": second})]
    page = assemble_page_data(sections)["logData"]["events"]
    initial = [e for e in page if e["log_type"] == "text_log" or e["row_id"] % 4 == 0]
    batch = [e for e in page if e not in initial][::-1]
    assert batch

    order = run_js(
        tmp_path,
        """
const { createViewerStore } = await import(`${STATIC}/state/viewer-store.js`);
const data = %s;
const store = createViewerStore({ logData: { events: data.initial }, logId: "A" });
store.applyLiveEvents(data.batch.slice(0, 2), []);
store.applyLiveEvents(data.batch.slice(2), []);
return store.allEvents.map((e) => e.row_id);
"""
        % json.dumps({"initial": initial, "batch": batch}),
    )
    assert order == [e["row_id"] for e in page]
