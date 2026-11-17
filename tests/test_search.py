"""The browser search language (src/cinc/static/services/search.js), run in Node.

Skipped when Node isn't installed. Each case is (query, row ids it should match)
over ROWS; a `None` expectation means the query must be rejected as a syntax error.
"""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

STATIC = Path(__file__).resolve().parents[1] / "src/cinc/static/services"
NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="needs node")

ROWS = [
    {
        "row_id": 1,
        "log_type": "core_event",
        "time": "2026-03-01T09:02:00Z",
        "name": "Bus voltage low",
        "system": "Power",
        "subsystem": "PDU",
        "unit": "PDU-2",
        "code": "PWR-01",
        "color": "Red",
        "set_clear": "set",
        "severity_rank": 2,
        "channels": ["A", "B"],
        "data": {"bus": {"load_pct": 72, "state": "overheat"}},
    },
    {
        "row_id": 2,
        "log_type": "core_event",
        "time": "2026-03-01T09:05:00Z",
        "name": "Battery temp high",
        "system": "Power",
        "subsystem": "Battery",
        "unit": "BAT-1",
        "code": "PWR-07",
        "color": "Yellow",
        "set_clear": "set",
        "severity_rank": 1,
        "channels": ["A"],
        "data": {"bus": {"load_pct": 12}},
    },
    {
        "row_id": 3,
        "log_type": "core_event",
        "time": "2026-03-01T09:12:00Z",
        "name": "Pump flow low",
        "system": "Thermal",
        "subsystem": "Coolant",
        "unit": "Pump-A",
        "code": "THM-05",
        "color": "Red",
        "set_clear": "clear",
        "severity_rank": 2,
        "channels": ["B"],
        "data": {"items": [{"code": 7, "ratio": 5.0}], "busLoad": 70},
    },
    {
        "row_id": 4,
        "log_type": "text_log",
        "time": "2026-03-01T09:13:00Z",
        "level": "ERROR",
        "logger": "radio",
        "message": "Link timeout after 3s",
        "flag": True,
    },
]
CONFIGS = {
    "core_event": {
        "excludeFromBareTerms": ["data"],
        "fields": ["time", "name", "system", "color"],
    },
    "text_log": {
        "excludeFromBareTerms": ["data"],
        "fields": ["time", "level", "logger", "message"],
    },
}

CASES = [
    ("", [1, 2, 3, 4]),
    ("system:Power", [1, 2]),
    ("System:power", [1, 2]),
    # terms side by side are an AND; the second term must not lose its first token
    ("system:Power -color:Red", [2]),
    ("system:Power NOT color:Red", [2]),
    ("system:Power (color:Red OR color:Yellow)", [1, 2]),
    ("unit:(P* -PDU-2)", [3]),
    ("subsystem:(PDU OR Coolant) set_clear:set", [1]),
    ("system:Power OR system:Thermal color:Red", [1, 2, 3]),
    ("system:Power | system:Thermal", [1, 2, 3]),
    # wildcards and literal characters
    ("code:PWR-*", [1, 2]),
    ("name:*low", [1, 3]),
    ("name:?", []),
    ("name~volt", [1]),
    ("Bus", [1]),
    ('"Battery temp"', [2]),
    ("PDU-2", [1]),
    # numbers, missing fields, booleans, lists
    ("severity_rank>=2", [1, 3]),
    ("data.bus.load_pct<20", [2]),
    ("data.bus.load_pct>50", [1]),
    ("data.busLoad>60", [3]),
    ("data.items.ratio:5", [3]),
    ("data.items.code:7*", [3]),
    ("data.items.0.ratio:5.0", [3]),
    ("flag:true", [4]),
    ("channels:A -channels:B", [2]),
    ("channels:(A B)", [1]),
    # nested and key searches
    ("data.$.load_pct>50", [1]),
    ("$.load_pct<50", [2]),
    ("data~overheat", [1]),
    ("$:load_pct", [1, 2]),
    ("$~busl", [3]),
    ("$.*:overheat", [1]),
    # text logs
    ("level:error", [4]),
    ("message~TIMEOUT", [4]),
    ('time:"2026-03-01T09:1*"', [3, 4]),
    # syntax errors are reported, never "match everything"
    ("(system:Power", None),
    ("system:Power)", None),
    ("name:", None),
    ('"open', None),
    ("system:Power OR", None),
    ("NOT", None),
    ("-", None),
    ("()", None),
]

RUNNER = """
const [searchPath, helpPath, payloadPath] = process.argv.slice(2);
const fs = await import("node:fs");
const { getQueryPredicate, describeQueryError } = await import(searchPath);
const { SYNTAX, EXAMPLES } = await import(helpPath);
const { rows, configs, queries } = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
const run = (query) => {
  try {
    getQueryPredicate(query);
    const matches = (row) => getQueryPredicate(query, configs[row.log_type])(row);
    return rows.filter(matches).map((row) => row.row_id);
  } catch (error) {
    return { error: describeQueryError(query, error) };
  }
};
const help = [
  ...SYNTAX.map((row) => row[2]),
  ...EXAMPLES.flatMap((group) => group.items.map((item) => item[0])),
];
console.log(JSON.stringify({
  results: Object.fromEntries(queries.map((q) => [q, run(q)])),
  help: Object.fromEntries(help.map((q) => [q, run(q)])),
}));
"""


@pytest.fixture(scope="module")
def outcome(tmp_path_factory):
    folder = tmp_path_factory.mktemp("search")
    (folder / "run.mjs").write_text(RUNNER)
    payload = folder / "payload.json"
    payload.write_text(
        json.dumps({"rows": ROWS, "configs": CONFIGS, "queries": [q for q, _ in CASES]})
    )
    done = subprocess.run(
        [
            NODE,
            str(folder / "run.mjs"),
            (STATIC / "search.js").as_uri(),
            (STATIC / "search-help.js").as_uri(),
            str(payload),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(done.stdout)


@pytest.mark.parametrize("query,expected", CASES)
def test_query(outcome, query, expected):
    result = outcome["results"][query]
    if expected is None:
        assert isinstance(result, dict) and result["error"].startswith(
            "Can't run this search"
        )
    else:
        assert result == expected


def test_every_help_example_parses(outcome):
    broken = {q: r for q, r in outcome["help"].items() if isinstance(r, dict)}
    assert not broken
