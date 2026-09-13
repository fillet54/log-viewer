# Cinc Log Viewer — Implementation Spec v2

**Repository:** `~/Repos/log-viewer` (package `cinc`)
**Audience:** an implementing coding agent
**Status:** authoritative. Where this document and the current code disagree, this document wins.

---

## 0. How to use this document

Read this section fully before writing any code.

1. **Work strictly in phase order.** Phases 1 → 11. Do not start a phase until the previous
   phase's *Acceptance* checklist passes.
2. **Each phase ends with a commit.** Run `python -m pytest -q` and the manual smoke test
   in §11 before committing. Never commit with failing tests.
3. **Do not invent scope.** If a change is not listed here, do not make it. If you believe a
   listed change is wrong or impossible, stop and write your reasoning into
   `docs/SPEC_QUESTIONS.md` rather than improvising.
4. **This is a clean break.** There is no backwards-compatibility requirement for plugin
   APIs, JS globals, page-data key names, or the `cinc-data/` database. Delete compatibility
   shims and aliases rather than preserving them. Old SQLite databases are disposable
   (they hold dev data only).
5. **Exact names matter.** Class names, file paths, JSON keys, and CSS class names given in
   this document are normative. Copy them literally.
6. **Terminology used throughout:**
   - **plugin** — a directory under `src/cinc/plugins/<name>/` shipping one or more log types.
   - **log type** — the single runtime extension unit. Class `LogType`. Has a globally
     unique snake_case `id` (e.g. `core_event`, `text_log`).
   - **core** — everything in `src/cinc/` that is *not* under `src/cinc/plugins/`.
   - **leak** — core code that knows a specific log type's field names or semantics.
7. **§1.1 outranks this document.** Before every phase, re-read the four hard constraints:
   single-file standalone, minimal Python deps, no npm/Node/bundler, CDN libraries vendored
   into the repo. If a step in this spec would violate one of them, the constraint wins and
   the step is wrong — record it in `docs/SPEC_QUESTIONS.md` and stop.
8. **The prime directive of this refactor:** *core must contain no knowledge of any specific
   log type.* Phase 7 adds a second log type specifically so this can be verified rather
   than assumed. If at any point you find yourself writing `core_event`, `channels`,
   `set_clear`, `color`, `system`, `subsystem`, `norm_time`, or `utctime` into a file under
   `src/cinc/` that is not under `src/cinc/plugins/core_event/`, you have made a mistake.

---

## 1. Product intent

Cinc is a **frontend for viewing, searching, and correlating timestamped event log data.**

- Data arrives as **timestamped rows**. Row acquisition is somebody else's problem — Cinc
  never scrapes, polls hardware, or generates data.
- Two ingestion shapes, both first-class:
  - **Batch** — a complete set of rows for one log, imported at once.
  - **Live** — rows arrive incrementally and are appended to an open log.
- **The data declares its own type.** A file says what kind of rows it holds; the command
  line does not. This is what lets one report carry rows from several plugins and render
  each with its own row component, at its own row height, in one time-ordered list.
- **Everything domain-specific lives in a plugin.** The core provides: storage, import
  plumbing, the virtualized row list, the query language, bookmarks/comments, the chart
  frame, the detail panel, the standalone HTML build, and the live-append pipeline.
- Each plugin ships a **small curated sample data set** committed to the repo, so the app is
  demonstrable and testable with no data source attached.

Explicit non-goals for this version: authentication, multi-user sync, server-side search
indexing, dataset joins across logs.

### 1.1 Hard constraints — non-negotiable

These four outrank everything else in this document, including architectural cleanliness.
A change that improves the design but violates one of them is rejected. Re-read this section
at the start of every phase.

**C1 — The single-file standalone output must always work.**

`cinc build` produces exactly one `.html` file that opens from `file://` on a machine with
**no network, no server, and no sibling assets**, and is fully functional: rows, virtual
scrolling, search, bookmarks, the chart, the detail panel, plugin row rendering, and plugin
charts. Concretely:

- No `<script src=…>` or `<link href=…>` in the output pointing at anything but a `data:` URI.
- No `fetch`, `XMLHttpRequest`, `EventSource`, or dynamic `import()` of a network URL on any
  code path reachable in standalone mode. (`app.js` already guards SSE behind
  `pageData.live.sessionId`, which standalone never sets — keep it that way.)
- No web fonts, no remote images. Logos are already `data:` URIs; keep them that way.
- Every phase's acceptance includes rebuilding standalone and opening it with the network
  disabled.

If a proposed improvement cannot be expressed inside one self-contained file, the improvement
does not happen.

**C2 — Minimal Python dependencies.**

The complete runtime set is:

```toml
dependencies = ["flask", "jinja2", "waitress", "docopt"]
```

Nothing may be added without recording the decision in `docs/dependencies.md`. Test-only
packages go in `[project.optional-dependencies] dev`, never in `dependencies`. Prefer the
standard library — `sqlite3`, `json`, `dataclasses`, `importlib.resources`,
`importlib.metadata`, `datetime`, `re`, `threading`, `queue` — all of which the current code
already uses well.

Explicitly forbidden without a recorded decision: pydantic, SQLAlchemy, alembic, marshmallow,
attrs, cattrs, click, typer, requests, httpx, python-dateutil, pandas, numpy, celery, and any
async web framework.

**C3 — No npm, no Node, no bundler, no build step for JavaScript.**

There is never a `package.json`, `node_modules/`, webpack, vite, rollup, esbuild, babel,
swc, or TypeScript compilation in this repository. The JavaScript that ships is the
JavaScript in the repo, byte for byte. `pip install -e . && cinc serve` is the entire
toolchain. This is why the frontend is plain ES modules with an HTML import map and `htm`
tagged templates instead of JSX — that choice is load-bearing, not incidental. Do not
"improve" it.

**C4 — Third-party JS comes from CDN distributions, vendored into the repo.**

Libraries are the plain browser-ready UMD or ESM files a CDN serves (cdnjs, jsDelivr,
unpkg). Each is downloaded once, pinned to an exact version, committed under
`src/cinc/static/vendor/`, and loaded from there at runtime. Neither the served app nor the
standalone build fetches from a CDN at request time — that would break C1 and the offline
target.

> "From CDN" describes where a file is **obtained**, not where it is **loaded from**.

Adding a library means: choose a CDN build, pin the exact version, commit the file, add it to
the vendor manifest, and wire it into the import map plus `standalone.py`'s `BARE_MODULES`.
It never means `npm install`. Prefer libraries that publish a single dependency-free ESM or
UMD file; a library that only ships as an npm package requiring a bundler is disqualified by
C3, however good it is.

**Consequences for plugin authors** — state these in `docs/plugins.md`:

- A plugin's `script_paths` are ES modules that are read as text and inlined into page data.
  They may import only: the bare specifiers core provides (`preact`, `preact/hooks`,
  `preact/signals`, `@preact/signals-core`, `htm`, `htm/preact`, `logview/lib`,
  `static/runtime.js`), and relative paths to the plugin's own files.
- A plugin may not add a runtime Python dependency, and may not fetch anything at runtime.
- A plugin that needs a new third-party JS library must vendor it under C4 and get it added
  to the import map — it cannot ship its own copy of a bundler output.

---

## 2. Current state inventory

Read these files before starting. Line counts are approximate.

### Backend (`src/cinc/`)

| File | Lines | Role today |
|---|---|---|
| `app.py` | 49 | Flask app. **Hardwires `core-event`**: imports `CoreEventLiveMonitor`, calls `get_plugin("core-event")`, looks up `_log_registry.get("core_event")`, serves `/` from `samples/dev-data.json`. |
| `__main__.py` | 107 | docopt CLI: `serve`, `build`, `plugins`. |
| `plugin_manager.py` | 42 | Discovers `EventLogSourcePlugin` via entry-point group `cinc.plugins` + a hardcoded built-in list. |
| `standalone.py` | 218 | Inlines the whole frontend into one HTML file. **`SCRIPT_PATHS` is a hand-maintained list of ~60 JS files.** |
| `log_generator.py` | 278 | **Dead code.** Random fault generator with a hardcoded `FAULT_CATALOG`. Referenced by nothing. |
| `plugins/base.py` | 128 | `EventLogDocument`, `EventLogViewPlugin`, `EventLogSourcePlugin`. |
| `plugins/core_event/plugin.py` | 503 | `CoreEventPlugin` — normalization, set/clear pairing, channel catalog, row display strings. |
| `plugins/core_event/log_types.py` | 128 | `CoreEventBootLogType` — a **second, parallel** abstraction wrapping the same plugin. |
| `plugins/core_event/live_monitor.py` | 104 | **Random event generator** masquerading as a live source (`_SAMPLE_FAULTS`, `random.choices`). |
| `logs/types.py` | 132 | `LogRecord` dataclass + `LogTypeDefinition` ABC. |
| `logs/registry.py` | 36 | `LogTypeRegistry`, with **hardcoded `core_event` aliases** at lines 17–19. |
| `logs/store.py` | 352 | SQLite `log_records` + `log_events`. `_event_time()` falls back to `norm_time`. |
| `logs/routes.py` | 131 | `/logs`, `/logs/<type>`, `/logs/<type>/import`, `/logs/<type>/<id>`, delete. |
| `live/monitor.py` | 177 | `LiveMonitorPlugin` ABC + `SessionManager` (poll thread, SSE fan-out). |
| `live/routes.py` | 71 | `/live`, `/live/start`, `/live/stop`, `/live/<id>/stream`. |

### Frontend (`src/cinc/static/`)

| File | Role |
|---|---|
| `runtime.js` | `EventLog2` global: row-component / chart-type / timeline-view registries. Load-order-safe via `_pendingViewRegistrations`. |
| `app.js` | Bootstrap: reads `window.EVENTLOG2_PAGE_DATA`, injects `view.styles`, blob-imports `view.scripts`, mounts Preact, opens SSE for `pageData.live.sessionId`. |
| `services/app-services.js` | Builds `services = { logType, plugin, view, logData, viewerStore }`. |
| `state/*.js` | Signals: navigation, layout, search, chart, activity (bookmarks/comments), storage. |
| `services/search.js` | Query language (tokenizer + Pratt parser). **Domain-clean except for 4 hardcoded conventions.** |
| `logview/features/main/*` | Virtual list, toolbar, shell. |
| `logview/features/chart/LogMainChart.js` | Chart frame + registries + built-in `timeline`/`events` view. |
| `logview/features/detail/*` | Detail panel, data tree, bookmarks, comments. |
| `logview/features/search/*` | Search panel, history, filters, help dialog. |
| `logview/features/rows/PluginLogRow.js` | Resolves and renders the plugin's row component. |

### Known defects to fix in passing (each is called out again in its phase)

- **D1** `state/activity.js:28` — `validIds` is captured from the *initial* events array, so
  bookmarks and comments are silently rejected on live-appended events.
- **D2** `live/monitor.py:114` — `if not self._thread: self._store.complete_log(...)` is
  inverted, and `self._thread` is never reset to `None`, so stop/start cycles misbehave.
- **D3** `logs/store.py:49-58` — `_event_time()` falls back to `float(norm_time)` interpreted
  as a Unix epoch, producing 1970 timestamps.
- **D4** `logs/registry.py:17-19` — hardcoded `core_event` alias table in core.
- **D5** `logs/registry.py:26-28` — `try: fn(**kwargs) / except TypeError: fn()` signature
  guessing.
- **D6** `plugins/base.py:19` — `row_settings: dict[str, Any] = {}` is a mutable class
  attribute shared across instances.
- **D7** `app.py` `/` builds page data via `plugin.build_page_data()`, which omits the
  `search` and `charts` keys that `build_view_page_data()` adds, so the demo page behaves
  differently from `/logs/<type>/<id>`.
- **D8** `templates/live_view.html` is dead (no route renders it).
- **D9** `standalone.html` (1.9 MB build output) sits untracked in the repo root and is not
  in `.gitignore`.
- **D10** `cinc-data/eventlog2.sqlite` is a stale database from a previous naming scheme.
- **D11** `docutils` is declared in `pyproject.toml` and `requirements.txt` but **used
  nowhere** — `grep -rn docutils src tests` returns nothing. Violates C2.
- **D12** `standalone.py:10` imports `jinja2` directly, but `jinja2` is not a declared
  dependency — it works only because Flask happens to pull it in. Declare it (C2 lists it).
- **D13** Six of the fourteen files in `static/vendor/` have **zero references** and are
  leftovers from the pre-ESM era: `htm.min.js`, `htm.mjs`, `preact.min.js`,
  `preact-hooks.min.js`, `preact-signals.min.js`, `preact-signals-core.min.js` (~26 KB).
  The eight live ones are `chart.umd.min.js` (Chart.js 4.4.1), `split.min.js` (Split.js
  1.6.5), `preact.mjs`, `hooks.mjs`, `signals.mjs`, `signals-core.mjs`, `htm-core.mjs`,
  `htm-preact.mjs`.
- **D14** `docs/search_syntax.rst` and `src/cinc/static/search_syntax.html` (492 lines, a
  docutils render) are both orphaned — no code references either, and there is no script that
  regenerates the HTML from the RST. The live search documentation is the hand-written
  `SearchHelpDialog.js`. This is almost certainly why `docutils` was added (D11).

---

## 3. Target architecture

```
src/cinc/
  app.py                  # app factory; discovers log types; zero plugin imports
  __main__.py             # CLI
  standalone.py           # single-file HTML build; auto-discovers frontend modules
  core/
    contract.py           # NormalizedEvent contract + validation helpers
    registry.py           # LogTypeRegistry (discovery via entry points)
  logs/
    types.py              # LogRecord, LogType (THE extension unit), LogDocument, Sample
    store.py              # SQLite storage (unchanged schema)
    routes.py             # /logs/*
  live/
    session.py            # LiveSource ABC + SessionManager + LiveSessionRegistry
    replay.py             # SampleReplaySource — generic dev live source
    routes.py             # /live/*
  static/                 # core frontend, domain-agnostic
  templates/              # core templates, domain-agnostic
  plugins/
    core_event/           # reference plugin #1 (domain-rich)
      __init__.py
      log_type.py
      live.py
      static/row.js
      static/row.css      # NEW — plugin-owned styles
      static/charts.js
      samples/sample-3ch.json
      samples/sample-4ch.json
    text_log/             # reference plugin #2 (minimal) — the conformance test
      __init__.py
      log_type.py
      static/row.js
      static/row.css
      static/charts.js
      samples/sample.json
```

### The two abstractions collapse into one

Today `EventLogSourcePlugin` (id `core-event`) and `LogTypeDefinition` (id `core_event`) are
two parallel abstractions describing the same thing, glued together by
`CoreEventBootLogType(self.plugin_id, self.build_page_data)` and reconciled by an alias table
in the core registry. The frontend then has to guess which identity applies:

```js
// services/app-services.js + PluginLogRow.js today
services?.logType || services?.plugin || services?.logData?.logTypeId || services?.logData?.pluginId
```

**v2: there is exactly one class, `LogType`, with exactly one id.** The word "plugin" survives
only as the name for a *directory* that ships log types. `plugin_manager.py`,
`plugins/base.py`, and `plugins/core_event/log_types.py` are deleted; their behaviour moves
into a single `LogType` subclass per plugin.

---

## 4. Normative interfaces

This section is the reference documentation the plugin author reads. Implement it exactly.
It is reproduced (lightly reworded, with prose) into `docs/plugins.md` in Phase 10.

### 4.1 The event contract

The single most important rule in the system:

> **A normalized event is a JSON object with three required keys, `row_id`, `time`, and
> `log_type`. Core reads those three keys and nothing else.**

```
row_id   : integer  Unique within one log. Stable. Used for selection, bookmarks,
                    comments, cross-links, and chart hit-testing.
time     : string   ISO-8601 UTC instant, e.g. "2026-04-03T08:00:15Z" or
                    "2026-04-03T08:00:15.250Z". Must parse with
                    datetime.fromisoformat() in Python and Date.parse() in JS.
log_type : string   The id of the LogType that produced this row. This is what makes
                    the data self-describing: core dispatches row rendering, detail
                    summary, search config, and row height off this field, per row.
                    A single log may contain rows of several types (§4.9).
```

`log_type` is set by the base class, not by the plugin — `build_page_data` stamps
`event["log_type"] = self.id` on every event returned by `normalize_events` before
validating. A plugin must not set it, and `validate_events` rejects an event whose
`log_type` disagrees with the producing type.

The column `log_events.log_type` already exists in the SQLite schema and is currently just a
copy of the record's type. It now becomes genuinely per-event, so no migration is needed.

Every other key is plugin territory. Core stores it, indexes it for search, shows it in the
data tree, and hands the whole object to the plugin's row component — but core never reads
it by name.

Reserved key prefixes and suffixes, which core *does* understand generically:

| Pattern | Meaning |
|---|---|
| `data` | Conventional container for the nested payload. Rendered by the detail-panel data tree. Excluded from bare-term search by default (configurable, §4.5). |
| `<field>_search` | Optional array of extra strings that should match when the user queries `<field>`. Produced by the plugin; consumed generically by `services/search.js`. |

`norm_time` and `utctime` are **abolished as core concepts**. A plugin may keep them as
plugin-private fields on its events (core_event does), but core must never read them.

Where core previously worked in "seconds since log start", it now works in **absolute
milliseconds** derived from `Date.parse(event.time)`.

### 4.2 `LogType` — the backend extension unit

Lives in `src/cinc/logs/types.py`.

```python
class LogType(ABC):
    # --- identity (class attributes, required) ---
    id: str                      # globally unique, snake_case, e.g. "core_event"
    name: str                    # human label, e.g. "Core Event"
    description: str = ""        # one sentence, shown on /logs

    # --- assets (class attributes, optional) ---
    asset_package: str | None = None   # defaults to this class's package
    script_paths: tuple[str, ...] = () # ES modules, inlined into page data
    style_paths: tuple[str, ...] = ()  # CSS, inlined into page data
    sample_paths: tuple[str, ...] = () # committed sample files, relative to asset_package

    # --- import (required) ---
    @abstractmethod
    def parse_import(self, *, file=None, json_data=None) -> tuple[str, dict]:
        """Return (display_name, raw_payload). Raise ValueError with a
        user-facing message on bad input."""

    # --- type detection (optional but strongly recommended) ---
    def detect(self, payload: dict) -> float:
        """Confidence in [0.0, 1.0] that this log type can parse `payload`.
        Used only when the data does not name its type explicitly (§4.9).
        Return 0.0 when the shape is clearly not yours. Be strict: a type that
        returns 0.5 for anything makes detection useless for everyone."""
        return 0.0

    # --- normalization (required) ---
    @abstractmethod
    def normalize_events(self, payload: dict, *, row_id_base: int = 0) -> list[dict]:
        """Turn a raw payload into normalized events.
        Every returned dict MUST have an integer `row_id` and an ISO-8601 `time`.
        Do NOT set `log_type` — the base class stamps it.
        This is the ONLY place raw data becomes canonical.

        `row_id_base` is the offset core has allocated to this section. Assign
        `row_id = row_id_base + n` (n starting at 1) and use those same values for
        any internal cross-links, so links survive being merged into a bundle
        alongside other sections (§4.9)."""

    # --- storage round-trip (optional; defaults are usually right) ---
    def extract_metadata(self, payload: dict) -> dict: ...
    def build_payload_from_events(self, record: LogRecord, events: list[dict]) -> dict: ...

    # --- list rendering (optional) ---
    def list_columns(self) -> list[dict[str, str]]: ...     # [{"key":…, "label":…}]
    def format_list_row(self, record: LogRecord) -> dict[str, str]: ...

    # --- viewer configuration (optional) ---
    def view_config(self, payload: dict) -> dict: ...       # -> page_data["view"]
    def search_config(self, payload: dict) -> dict: ...     # -> page_data["search"]
    def log_summary(self, payload: dict) -> dict: ...       # -> page_data["log"]["summary"]

    # --- live capture (optional) ---
    def create_live_source(self) -> "LiveSource | None": return None

    # --- multi-document sources (optional) ---
    def read_source(self, path: Path) -> Any: ...
    def build_documents(self, source, source_path=None) -> list[LogDocument]: ...

    # --- import template (optional) ---
    import_template: str | None = None

    # --- final, provided by the base class; DO NOT override ---
    def build_page_data(self, payload: dict) -> dict: ...
    def read_asset_text(self, relative_path: str) -> str: ...
    def inline_scripts(self) -> list[str]: ...
    def inline_styles(self) -> list[str]: ...
    def samples(self) -> list[Sample]: ...
```

`build_page_data()` is `final` because it is the one place the page-data envelope (§4.4) is
assembled. Today `CoreEventPlugin.build_page_data` and
`CoreEventBootLogType.build_view_page_data` both build partial envelopes, which is why the
`/` route and the `/logs/…` route render differently (defect **D7**). One assembler, one
shape.

Base-class implementation of `build_page_data`:

Single-type `build_page_data` is a one-section case of the bundle assembler in §4.9; both
call the same `assemble_page_data(sections)` in `core/assemble.py`. Keeping one assembler is
what stops single-type and mixed-type pages from drifting apart:

```python
# core/assemble.py
def assemble_page_data(sections: list[Section]) -> dict:
    """sections: [(log_type, payload)] in the order they should be merged."""
    events, base = [], 0
    for log_type, payload in sections:
        produced = log_type.normalize_events(payload, row_id_base=base)
        for event in produced:
            event["log_type"] = log_type.id     # stamped by core, never by the plugin
        validate_events(produced, log_type.id)  # raises ValueError
        events.extend(produced)
        base += len(produced)
    events.sort(key=lambda e: (e["time"], e["row_id"]))

    types = {lt.id: lt for lt, _ in sections}
    return {
        "apiVersion": 2,
        "logTypes": {
            lt.id: {
                "name": lt.name,
                "rowSettings": lt.view_config(payload_for[lt.id]).get("rowSettings", {}),
                "charts": …, "timelineViews": …,
                "search": lt.search_config(payload_for[lt.id]),
            }
            for lt in types.values()
        },
        "log": {"summary": merged_summary(sections)},
        "logData": {"events": events},
        "view": {
            "scripts": dedupe([s for lt in types.values() for s in lt.inline_scripts()]),
            "styles":  dedupe([s for lt in types.values() for s in lt.inline_styles()]),
        },
    }

# LogType
def build_page_data(self, payload: dict) -> dict:
    return assemble_page_data([(self, payload)])
```

Note the merge sorts by `(time, row_id)`. `row_id` is the tiebreaker, so rows from one
section keep their relative order when timestamps collide — which they will, since
`SampleReplaySource` and many real sources emit bursts inside one second.

Note the shape change: `logData` now carries **only** `events`. Anything a plugin previously
stuffed into `logData` (core_event put `channels`, `channelCount`, `start`, `end`, `hours`,
`seed`, `modes` there) moves to `view` (if the frontend needs it to render) or to
`log.summary` (if it is display-only header text).

### 4.3 Discovery and registration

```toml
# pyproject.toml
[project.entry-points."cinc.log_types"]
core-event = "cinc.plugins.core_event:CoreEventLogType"
text-log   = "cinc.plugins.text_log:TextLogType"
```

`src/cinc/core/registry.py`:

```python
class LogTypeRegistry:
    def __init__(self) -> None: self._types: dict[str, LogType] = {}

    def register(self, log_type: LogType) -> None:
        """Reject duplicate ids and non-LogType objects with a clear error."""

    def get(self, log_type_id: str) -> LogType | None: ...
    def all(self) -> list[LogType]:  # sorted by .name
        ...

    @classmethod
    def discover(cls) -> "LogTypeRegistry":
        """Load every entry point in group 'cinc.log_types', instantiate it,
        assert isinstance(obj, LogType), register it."""
```

Rules:

- **No alias table.** Ids are exact. (Removes **D4**.)
- **No built-in fallback list.** Everything, including `core_event`, is discovered through
  entry points. This guarantees a third-party plugin is not second-class.
- **No signature guessing.** `discover()` calls `EntryPoint.load()()` with no arguments.
  (Removes **D5**.)
- A duplicate id raises `ValueError` at startup rather than silently overwriting.

### 4.4 The page-data envelope (v2)

`window.CINC_PAGE_DATA` — the single contract between backend and frontend.

```jsonc
{
  "apiVersion": 2,

  // Every log type present in this log, keyed by id. One entry for a single-type log;
  // several for a bundle (§4.9). Per-type config lives here, NOT at the top level,
  // because with mixed rows there is no single answer.
  "logTypes": {
    "core_event": {
      "name": "Core Event",
      "rowSettings": { "channels": ["A", "B", "C", "D"] },
      "charts": ["systems"],
      "timelineViews": ["severity", "bus-load"],
      "search": {
        "labelField": "name",
        "excludeFromBareTerms": ["data"],
        "aliasSuffix": "_search",
        "fields": ["time", "name", "system", "color"],
        "examples": ["color:Red", "system:Power"]
      }
    },
    "text_log": { "name": "Text Log", "rowSettings": { "levels": ["DEBUG","INFO","WARN","ERROR"] },
                  "charts": [], "timelineViews": ["levels"], "search": { … } }
  },

  "log": {
    "id": "…",                  // present for stored logs; absent for the / demo page
    "name": "Boot sequence 04-03",
    "source": "import",         // "import" | "live"
    "status": "completed",      // "active" | "completed" | "error"
    "summary": {                // plugin-authored display-only header pairs
      "Channels": "A, B, C, D",
      "Duration": "2.0h",
      "Events": "62"
    }
  },

  "logData": {
    "events": [ /* normalized; row_id + time + log_type required; sorted by time */ ]
  },

  "view": {
    "scripts": [ "…js source…" ],   // inlined ES modules, union over all logTypes
    "styles":  [ "…css source…" ]   // inlined CSS, union over all logTypes
  },

  "live": { "sessionId": "…" }      // present only while a live capture is active
}
```

Renames from v1, all mandatory:

| v1 | v2 |
|---|---|
| `window.EVENTLOG2_PAGE_DATA` | `window.CINC_PAGE_DATA` |
| `apiVersion: 1` | `apiVersion: 2` |
| `plugin: {id, name}` | *removed* — `log_type` on each row is the only identity |
| `logType: {id, name}` | *removed* — replaced by the `logTypes` map |
| `logData.pluginId`, `logData.logTypeId` | *removed* |
| everything else in `logData` | moved to `logTypes[id]` or `log.summary` |
| `view.rowSettings`, `view.charts`, `view.timelineViews` | moved into `logTypes[id]` |
| top-level `search` | moved into `logTypes[id].search` |

`view` now holds only the two things that are genuinely global: the union of every present
type's inlined scripts and styles. Everything a component needs to know about *a specific
row* is reached by `logTypes[event.log_type]`.

### 4.5 Frontend runtime API

`src/cinc/static/runtime.js` exports a global named **`Cinc`** (rename from `EventLog2`).
Registrations are keyed by **log type id** only.

```js
import { Cinc } from "static/runtime.js";

// Required for any log type whose rows should render.
Cinc.registerRow(logTypeId, Component)

// Optional. A chart type is a full panel; the app renders one at a time.
Cinc.registerChart(logTypeId, definition)

// Optional. A timeline view is a dataset spec drawn in the built-in timeline chart.
Cinc.registerTimelineView(logTypeId, definition)

// Optional. Overrides how the chart axis and hover labels format an instant.
Cinc.registerTimeFormatter(logTypeId, (ms) => string)
```

The old names `registerLogRowComponent`, `registerLogChartType`, `registerLogTimelineView`,
`registerPluginRowComponent`, `registerPluginChartType`, `registerPluginTimelineView` are all
**deleted**. There are no aliases.

Keep the existing `_pendingViewRegistrations` deferral mechanism — it makes registration
order-independent, which matters because plugin scripts are blob-imported by `app.js` before
`LogMainChart.js` may have loaded.

#### Row component props

```js
({
  event,            // the normalized event object
  selected,         // boolean
  highlighted,      // boolean — transient post-jump flash
  highlightNonce,   // number — changes to re-trigger the flash animation
  bookmarkColor,    // 0..5
  extraClasses,     // string[]
  view,             // page_data.view — read rowSettings from here
  onSelect,         // (event?) => void
  onBookmark,       // (event?) => number
  onJump,           // (rowId) => void — scroll to and select another row
}) => VNode
```

**Row component contract (must-do list for plugin authors):**

1. The root element must be a `<div>` carrying `class="log-line …"` and
   `data-row-id=${event.row_id}`.
2. It must include `extraClasses`, plus `log-selected` when `selected`, `log-highlight`
   when `highlighted`, and `is-bookmarked` when `bookmarkColor > 0`.
3. It must call `onSelect(event)` on click.
4. **Every row of one log type must render at the same fixed height.** Different log types
   may have different heights — core measures one probe row per type and handles the mixed
   layout (§4.10). But *within* a type the height must be constant and must not depend on
   the row's content: no wrapping text, no conditionally rendered second line, no
   content-driven `min-height`. Set the height in the plugin's CSS on
   `.log-line.<prefix>-row` and clip overflow. A type whose rows vary in height will corrupt
   scrolling for the whole log.
5. All plugin-specific styling must come from the plugin's own `style_paths` CSS. See §4.6
   for the class names core owns.
6. Read per-type config from `logTypes[event.log_type]`, never from a top-level `view` key —
   the row may be sharing a log with other types.

#### Timeline-view definition

```js
{
  id: "severity",          // unique within the log type
  label: "Severity",
  kind: "histogram" | "line" | "scatter" | "bar",
  stacked: false,
  datasets: [{
    label: "Red",
    filter: (event) => boolean,     // which events feed this dataset
    value:  (event) => number,      // required for line/scatter/bar; ignored by histogram
    borderColor, backgroundColor, pointRadius, tension,
  }],
  buildDatasets: (events, helpers) => [...],  // escape hatch, replaces `datasets`
  configureChart: (chart) => void,            // mutate Chart.js options after build
}
```

#### Chart-type definition

```js
{
  id: "systems",
  label: "Subsystem Status",
  panelHtml: "…",                       // static markup, OR
  renderPanel: (panelEl, context) => ({ update?, resize?, destroy? }),
  buildCommands: (containerEl) => void, // toolbar controls
  activate, deactivate, destroy, resize,
}
```

`context` contains `{ services, logData, viewerStore, helpers }`. `helpers` exposes
`startMs`, `endMs`, and `eventMs(event)`.

### 4.6 CSS ownership

Core owns exactly these row-related selectors. Plugins may rely on them and must not
redefine them:

```
.log-line          base row: grid container, height, border, hover
.log-selected      selected row background
.log-highlight     post-jump flash animation
.is-bookmarked     bookmarked row accent
.bookmark-toggle   bookmark button
.log-row-error     fallback shown when no row component is registered
--bookmark-color-1 … --bookmark-color-5
```

Core owns `.log-line`'s `display: grid`, `align-items`, `height`, and `border-bottom`.
**The plugin owns `grid-template-columns` and every cell class.** Today core's stylesheet
hardcodes core_event's exact 7-column geometry; that moves out in Phase 6.

Everything else a row renders — severity colours, channel chips, action badges, fault
prefixes, offset columns — belongs in the plugin's stylesheet, under a class prefix
namespaced to the log type (`.ce-*` for core_event, `.tl-*` for text_log).

### 4.7 `LiveSource` — incremental row delivery

`src/cinc/live/session.py`:

```python
class LiveSource(ABC):
    poll_interval: float = 2.0

    @abstractmethod
    def available(self) -> bool:
        """True if this source can start right now (device present, file readable…).
        The UI disables Start and shows `unavailable_reason()` when False."""

    def unavailable_reason(self) -> str: return "Source not available"

    @abstractmethod
    def start(self, session_id: str) -> dict:
        """Begin a capture. Return a raw payload header dict (no events) that will
        be stored as the log's payload_header — e.g. {"channels": [...]}."""

    @abstractmethod
    def poll(self) -> list[dict]:
        """Return raw rows produced since the last call. May return []. Must not block
        for longer than poll_interval. Raising is logged and the loop continues."""

    @abstractmethod
    def stop(self) -> None: ...

    def finished(self) -> bool:
        """Return True to end the capture on the source's own initiative
        (e.g. a replay file ran out). Default False."""
```

`SessionManager` (one per log type) drives the poll loop, calls
`log_type.normalize_events({"events": raw, **header})`, appends to the store, and fans the
normalized events out to SSE subscribers. `LiveSessionRegistry` maps log-type id → manager
and enforces one active capture per log type.

**Rows in, rows out.** A `LiveSource` returns *raw rows in the plugin's own input shape* and
`normalize_events` canonicalizes them — the exact same code path as batch import. There must
not be a separate normalization path for live. (Today `CoreEventPlugin.normalize_stream_events`
is that separate path; it is deleted.)

### 4.8 The vendor manifest

Implements C4. Create `src/cinc/static/vendor/MANIFEST.json` — the single source of truth for
every third-party JS file.

```jsonc
{
  "note": "Vendored CDN distributions. No npm. See docs/dependencies.md. Refresh with tools/refresh_vendor.py.",
  "libraries": [
    {
      "name": "chart.js",
      "version": "4.4.1",
      "file": "chart.umd.min.js",
      "kind": "global",                    // "global" (UMD, sets window.X) | "module" (ESM)
      "global": "Chart",
      "url": "https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js",
      "sha256": "…"
    },
    {
      "name": "split.js", "version": "1.6.5", "file": "split.min.js",
      "kind": "global", "global": "Split",
      "url": "https://cdn.jsdelivr.net/npm/split.js@1.6.5/dist/split.min.js", "sha256": "…"
    },
    {
      "name": "preact", "version": "…", "file": "preact.mjs",
      "kind": "module", "specifier": "preact",
      "url": "https://cdn.jsdelivr.net/npm/preact@…/dist/preact.module.js", "sha256": "…"
    }
    // …hooks.mjs → "preact/hooks", signals.mjs → "preact/signals",
    //   signals-core.mjs → "@preact/signals-core",
    //   htm-core.mjs → "htm", htm-preact.mjs → "htm/preact"
  ]
}
```

Chart.js 4.4.1 and Split.js 1.6.5 are readable from the file banners. The preact / htm /
signals builds carry no version banner — determine each version by re-downloading the pinned
release you intend to keep from jsDelivr and diffing against the committed file. If a
committed file does not match any published release, replace it with the nearest published
version and note the swap in `docs/dependencies.md`. Do not leave a `version` field as
`"unknown"`.

Add `tools/refresh_vendor.py` — a stdlib-only script (`urllib.request`, `hashlib`, `json`) that
re-downloads each manifest entry, verifies the `sha256`, and writes the file. It is a
developer utility run by hand; it is never invoked by the app, the tests, or the build, so it
does not make the network a runtime dependency.

Three places must agree on this list, and a test enforces it (Phase 11, `test_dependencies.py`):

1. `MANIFEST.json`
2. the import map in `src/cinc/templates/base.html` (`kind: "module"` entries) and the
   `<script src>` tags for `kind: "global"` entries
3. `standalone.py`'s `GLOBAL_SCRIPTS` (globals, load-order sensitive) and `BARE_MODULES`
   (specifier → file)

Any file in `static/vendor/` that is absent from the manifest is a test failure. That is what
prevents another six-file drift like **D13**.

### 4.9 Self-describing data and mixed-type logs

**The data says what it is. The command line does not.**

`cinc build --plugin core-event --data x.json` is wrong: it puts knowledge in the invocation
that belongs in the file, it cannot express a file containing more than one kind of row, and
it silently produces garbage when the flag and the file disagree. Type identity moves into
the data, at two levels.

#### Container level — the bundle envelope

Core understands one envelope, and only one, before any plugin is consulted:

```jsonc
{
  "cinc": 2,                          // presence of this key marks a bundle
  "name": "Integration run 4471",     // optional; default log name
  "sections": [
    { "logType": "core_event", "payload": { /* core_event's native shape */ } },
    { "logType": "text_log",   "payload": { /* text_log's native shape   */ } },
    { "logType": "text_log",   "payload": { /* another text_log section  */ } }
  ]
}
```

A section's `payload` is passed to that type's `normalize_events` **unchanged**. Plugins never
see the envelope and do not need to know bundles exist. A bundle may repeat a `logType`.

A plain single-type file stays exactly what it is today — the plugin's native payload, with
no wrapper — so nothing about existing inputs breaks. Such a file may name its type at the
top level:

```jsonc
{ "logType": "core_event", "channels": ["A","B","C","D"], "events": [ … ] }
```

`logType` is a reserved top-level key: core strips it before handing the payload to the
plugin, so a plugin never has to tolerate it.

#### Row level

Within a section, rows inherit the section's type. Per-row override is deliberately **not**
supported in the raw input: a heterogeneous row stream would force every plugin to parse
rows it does not own. Sections are the unit of heterogeneity on the way in.

On the way *out* it is per-row: every normalized event carries `log_type` (§4.1), and the
frontend dispatches per row. So a bundle of three sections produces one time-ordered event
list whose rows render with three different components — which is exactly the "single report
with multiple plugins" goal.

#### Type resolution order

Given an input, core resolves the type by the first rule that matches:

1. It is a bundle (`cinc` key present) → each section's declared `logType`.
2. A top-level `logType` key → that type.
3. An explicit `--log-type` CLI flag or the UI's chosen import type → that type.
4. **Sniffing**: call `detect(payload)` on every registered type. Take the single highest
   score above `0.5`.
5. Otherwise → error.

Rules 4 and 5 must produce actionable errors, not guesses:

- no type scores above 0.5 → `Cannot determine log type for <path>. Registered types: …
  Pass --log-type, or add a "logType" key to the file.`
- two or more tie at the top → `Ambiguous log type for <path>: core_event and text_log both
  match. Pass --log-type to disambiguate.`

An unknown `logType` string is always a hard error listing the registered ids — never a
fallback to sniffing. Explicit-but-wrong is a typo, and guessing past a typo is how you get a
log that silently imports as the wrong type.

`detect` implementations for the two reference plugins:

```python
# core_event: an object with an events array whose first element has set_clear or channels
def detect(self, payload):
    events = payload.get("events")
    if not isinstance(events, list) or not events: return 0.0
    first = events[0]
    if not isinstance(first, dict): return 0.0
    keys = set(first)
    if {"set_clear", "channels"} & keys: return 0.9
    if {"norm_time", "utctime"} <= keys: return 0.7
    return 0.0

# text_log: an object with a lines array whose first element has ts + level + message
def detect(self, payload):
    lines = payload.get("lines")
    if not isinstance(lines, list) or not lines: return 0.0
    first = lines[0]
    if isinstance(first, dict) and {"ts", "level", "message"} <= set(first): return 0.9
    return 0.0
```

#### Multiple input files

`cinc build --data a.json --data b.json --output out.html` resolves each file's type
independently and merges them into one bundle, in the order given. Identical in effect to
authoring a bundle by hand. Same for `cinc build --data bundle.json`.

The web import route accepts multiple files in one multipart upload with the same semantics.

#### `row_id` allocation across sections

Each plugin numbers rows from 1 within its own section, and core would collide them on merge.
Worse, core_event's cross-links (`pairedChannels[].linkedRowId`,
`matchSummary[].linkedRowId`) reference row ids, so a naive renumbering pass by core would
silently break every jump link.

The fix is that core allocates the range *before* calling the plugin and passes the offset
in: `normalize_events(payload, *, row_id_base)`. The plugin numbers from `row_id_base + 1`
and its internal cross-links, computed from those same ids, stay correct with no remapping.
This is the reason for the signature change in §4.2, and it is the single easiest thing to
get wrong in this whole feature.

#### Storage

`log_records.log_type` is **removed**. Membership of a log in a type is many-to-many and
lives in a new table:

```sql
CREATE TABLE log_record_types (
    log_id   TEXT NOT NULL REFERENCES log_records(id) ON DELETE CASCADE,
    log_type TEXT NOT NULL,
    PRIMARY KEY (log_id, log_type)
);
CREATE INDEX idx_log_record_types_type ON log_record_types(log_type);
```

`LogRecord` gains `log_type_ids: list[str]`. `list_records(log_type_id=…)` joins through the
table, so a mixed log appears under **every** type it contains and the `/logs` counts stay
exact. `log_events.log_type` continues to hold the per-event type, which is now meaningful.

The database is disposable (§0.4) — delete `cinc-data/` rather than migrating.

### 4.10 Variable row heights

Two log types will not have the same row height, and rows of both appear in one virtual
list. The two options are to pad every row to the tallest type, or to lay out each type at
its natural height. **This spec requires the latter.** Padding wastes vertical space
permanently and makes a compact type look broken next to a tall one.

This is affordable because of the rule in §4.5 item 4: **height is a property of the log
type, not of the individual row.** With N types there are N known heights, so the layout is
*piecewise uniform* rather than arbitrarily variable, and it reduces to a prefix-sum plus a
binary search — not the estimate-and-correct machinery general variable-height
virtualization needs.

#### The model

```js
// Built once per (filtered list, height table) change.
// heights: Map<logTypeId, px>
offsets = Float64Array(n + 1);
offsets[0] = 0;
for (i = 0; i < n; i++) offsets[i + 1] = offsets[i] + heights.get(events[i].log_type);
```

| Operation | Before (fixed stride) | After (prefix sums) |
|---|---|---|
| total scroll height | `n * stride` | `offsets[n]` — O(1) |
| index → top | `i * stride` | `offsets[i]` — O(1) |
| scrollTop → first visible index | `floor(top / stride)` | binary search over `offsets` — O(log n) |
| build / rebuild | none | one O(n) pass |
| append k rows (live) | none | extend in place, O(k) |

Cost at 100k rows: an 800 KB `Float64Array` and a ~1 ms build, rebuilt only when the filtered
list or a measured height actually changes. Per-scroll-frame work goes from O(1) to O(log n)
— about 17 comparisons. That is comfortably below the budget for a `requestAnimationFrame`
handler, and the existing code already does more work than that per frame. **Recommendation:
natural heights, as requested; there is no performance reason to compromise.**

#### Measuring

Heights come from CSS, so they must be measured, not declared. Replace the single hidden
probe (`measureRef`, which renders `filteredEvents[0]`) with **one probe per log type
present in the log**: render one hidden row per type, measure each with
`getBoundingClientRect().height`, add the list's `rowGap`, and store the result in the
heights map. Re-measure on container resize and on theme change.

Until a type has been measured, use a per-type fallback from
`logTypes[id].rowHeightHint` (an integer the plugin may declare; core_event 38, text_log 56)
or 38 if absent. The hint prevents a visible jump on first paint; it is never authoritative.

#### What changes

`useVirtualList` grows an optional `offsets` input. When present it replaces the `rowHeight`
arithmetic; when absent the existing fixed-stride path is used unchanged, so nothing that
does not need variable heights pays for them.

```js
useVirtualList({
  containerRef,
  itemCount,
  rowHeight,          // fallback when `offsets` is absent
  offsets,            // Float64Array | null
  overscan, maxVisible, dependencies,
})
// returns { startIndex, endIndex, offsetY, totalHeight, visibleCount, invalidate, scrollToIndex }
```

`offsetY` becomes `offsets[startIndex]` and `totalHeight` becomes `offsets[n]`; the two
consumers keep using them unchanged, which is what keeps this contained.

Everything downstream that assumes a single stride must change with it:

| Location | Assumption to remove |
|---|---|
| `hooks/use-virtual-list.js:3-20` | `buildRange` — `floor(scrollTop / rowHeight)`, `startIndex * rowHeight`, `itemCount * rowHeight` |
| `hooks/use-virtual-list.js:108-124` | `scrollToIndex` — `index * rowHeight`, and the `center`/`end` alignment maths must use *that row's* height |
| `MainViewShell.js:89-113` | `smoothScrollToIndex` — `index * rowStride - clientHeight/2 + rowStride/2` |
| `MainViewShell.js:138-154` | `emitScrollState` — `floor((scrollTop + clientHeight/2) / rowStride)` becomes a binary search |
| `MainViewShell.js:283-286` | the lock-to-bottom "at bottom" test uses `rowStride * 1.5` as slack; use the last row's height |
| `MainViewShell.js:204-220` | the single-probe measure effect becomes the per-type measurement pass |
| `LogVirtualList.js:84` | spacer `height: filteredEvents.length * rowStride` → `offsets[n]` |
| `LogVirtualList.js:152-156` | `MainLogPane`'s hidden probe renders one row → one row per present type |
| `SearchResultsContent.js:46,50` | spacer and `translateY` use `resultRowStride` → the search pane's own offsets array |
| `SearchResultsPane.js:55-67` | the search pane's hidden probe → one per present type |
| `SearchPanel.js:170-188` | the row-measure effect → per-type |

Both lists need their own offsets array, because the search pane renders a *different*
subset (and adds the `search-result-row` class, which may change height).

#### Boundary — state this in `docs/plugins.md`

Piecewise-uniform covers per-type heights, which is what mixed-plugin logs need. It does
**not** support rows of one type varying with their content (a message wrapping to three
lines). Supporting that needs measured-and-corrected virtualization with scroll anchoring,
which is a large amount of subtle code, and it is out of scope. If a plugin wants a
multi-line row, it declares a taller fixed height and clips — which is exactly what
`text_log` does, deliberately, to exercise this feature.

---

## 5. Recommended interface improvements — rationale

Summarized so the reasoning is not lost. Each maps to a phase.

| # | Improvement | Why | Phase |
|---|---|---|---|
| I1 | Collapse `EventLogSourcePlugin` + `LogTypeDefinition` into one `LogType` | Two ids for one thing forced an alias table in core, a callback-injection constructor, and four-way identity guessing in the frontend | 3 |
| I2 | Require `time` + `row_id`; abolish `norm_time`/`utctime` in core | Core's chart, list, store, and navigation all encode core_event's dual time representation. One canonical instant makes every core path log-type-agnostic | 4 |
| I3 | Navigation and chart work in absolute ms, not "seconds since start" | "Seconds since start" is only meaningful for a bounded mission log. A syslog file has no start offset | 4 |
| I4 | One `build_page_data` assembler on the base class | Today two half-assemblers produce different envelopes for `/` and `/logs/…` (D7) | 3 |
| I5 | `logData` carries only `events` | `logData` had become a junk drawer of core_event header fields that core had to route around | 3 |
| I6 | Plugin-owned CSS via `style_paths` | ~90 lines of core_event visual vocabulary currently live in core's stylesheet. The mechanism to fix this already exists and is simply unused | 6 |
| I7 | Declarative `search` config in page data | `services/search.js` hardcodes `name` as the label field, skips `data`, and knows the `_search` suffix convention. Those are per-log-type policies | 6 |
| I8 | `LiveSource` per log type, resolved through the registry | `app.py` imports `CoreEventLiveMonitor` directly — the single worst leak in the codebase | 5 |
| I9 | Live and batch share one normalization path | Two paths guarantee divergence; core_event already has a subtly different `normalize_stream_events` | 5 |
| I10 | Samples are a first-class `LogType` asset | Makes "one curated sample per plugin" enforceable and gives the demo route and tests a supported way to find data | 2 |
| I11 | Auto-discover frontend modules in `standalone.py` | A 60-entry hand-maintained list; any new core component silently breaks the standalone build (C1) | 9 |
| I12 | Namespace plugin CSS classes (`.ce-*`, `.tl-*`) | With two plugins loaded in one browser session, unnamespaced `.log-red` will collide | 6/7 |
| I16 | Data declares its own type; `--plugin` becomes an optional override | Type identity in the invocation cannot express a file with more than one kind of row, and silently produces garbage when flag and file disagree | 8 |
| I17 | `row_id_base` passed into `normalize_events` | The only way to merge sections without either colliding ids or breaking plugin-internal cross-links | 3/8 |
| I18 | Piecewise-uniform virtualization via prefix sums | Two plugins in one list need different row heights; per-type uniformity makes natural heights an O(log n) binary search rather than estimate-and-correct | 8 |
| I13 | Vendor manifest with pinned versions + checksums | Six dead vendor files accumulated unnoticed (**D13**) and no file records what version anything is. Makes C4 auditable | 1 |
| I14 | Trim Python deps to the C2 set | `docutils` is declared and unused; `jinja2` is used and undeclared | 1 |
| I15 | Standalone self-containment is a *test*, not a convention | C1 is the product's headline property and currently nothing enforces it | 9 |

---

## 6. Leakage inventory

Every location where core knows about `core_event`. Each row has a fix and a phase. **This
table is the definition of done for the de-leaking work.**

### Backend leaks

| # | Location | Leak | Fix | Phase |
|---|---|---|---|---|
| B1 | `app.py:16` | `from .plugins.core_event.live_monitor import CoreEventLiveMonitor` | `LiveSessionRegistry` built from `log_type.create_live_source()` | 5 |
| B2 | `app.py:20` | `get_plugin("core-event")` | app factory holds only the registry | 3 |
| B3 | `app.py:26-27` | `_log_registry.get("core_event")` + `raise` if missing | delete | 3 |
| B4 | `app.py:39-43` | `/` reads `samples/dev-data.json` from core_event | `/` lists all log types and their samples; `/demo/<log_type_id>/<sample>` renders one | 2 |
| B5 | `logs/registry.py:17-19` | hardcoded `core-event.boot-log` / `core_event.boot_log` aliases | delete registry aliasing entirely | 3 |
| B6 | `logs/store.py:49-58` | `_event_time()` reads `utctime`, then `norm_time` as an epoch | require `time`; raise if absent | 4 |
| B7 | `live/monitor.py:14,17-22,25` | `detect_system()`, `get_channels()`, `channels` threaded through `SessionManager` | `LiveSource.available()` + opaque header dict from `start()` | 5 |
| B8 | `live/monitor.py:83-88,136` | `SessionManager` constructs `{"channels": …}` payload headers | header comes from `LiveSource.start()`, unread by core | 5 |
| B9 | `templates/live_sessions.html:10,35,40` | prose "core event log"; `log_type_id='core_event'` in three `url_for` calls | iterate live-capable log types | 5 |
| B10 | `templates/log_view.html:26` | `record.metadata.channels \| join(", ")` in the live banner | render `log.summary` pairs generically | 5 |
| B11 | `templates/live_view.html:27,59` | same, in a dead template | delete the file (**D8**) | 1 |
| B12 | `docs/api-design.md` | describes v1, `eventlog2.sqlite`, "ships one log type" | replaced by `docs/plugins.md` | 9 |

### Frontend leaks

| # | Location | Leak | Fix | Phase |
|---|---|---|---|---|
| F1 | `LogMainChart.js:279-282` | `getEventAbsoluteMs` reads `event.utctime` | read `event.time` | 4 |
| F2 | `LogMainChart.js:296-302` | timeline bounds from `norm_time * 1000` | bounds from first/last `Date.parse(event.time)` | 4 |
| F3 | `LogMainChart.js:318-322` | `getEventMs` — `utctime` else `norm_time * 1000` | `Date.parse(event.time)` | 4 |
| F4 | `LogMainChart.js:462` | `getEventAbsoluteMs(last)` | follows F1 | 4 |
| F5 | `LogMainChart.js:575,666` | axis ticks and hover hardcode `HH:MM:SS` (assumes a sub-24h session) | span-aware default formatter + `Cinc.registerTimeFormatter` override | 4 |
| F6 | `LogMainChart.js:598,603-604` | jump payload `{seconds}` in norm_time space | `{timeMs}` absolute | 4 |
| F7 | `MainViewShell.js:152` | `setLogScroll({seconds: current.norm_time, …})` | `{timeMs: eventMs(current), …}` | 4 |
| F8 | `LogVirtualList.js:27,35` | `findClosestIndexBySeconds` binary-searches `norm_time` | `findClosestIndexByTime` over `Date.parse(event.time)` | 4 |
| F9 | `SearchPanel.js:34` | bookmark list sorts by `norm_time` | sort by `time` | 4 |
| F10 | `state/navigation.js:4-20` | jump/scroll payloads normalize a `seconds` field | `timeMs` | 4 |
| F11 | `EventSummary.js:6,8` | renders `event.utctime`, `set_clear`, `system/subsystem/unit/code` | log type supplies a detail-summary component: `Cinc.registerDetailSummary(id, C)`; core falls back to `name` + `time` | 6 |
| F12 | `SearchHelpDialog.js:3-13,67` | hardcoded examples `system:Power`, `color:Red`, `data.bus.load_pct>=68` | read `pageData.search.examples`; generic fallback | 6 |
| F13 | `services/search.js:88-90,116-117` | `name` hardcoded as the prefix-glob label field | `search.labelField` | 6 |
| F14 | `services/search.js:120` | `data` hardcoded as excluded from bare terms | `search.excludeFromBareTerms` | 6 |
| F15 | `services/search.js:27` | `<field>_search` suffix convention hardcoded | `search.aliasSuffix` | 6 |
| F16 | `styles.css:45-51` | `--event-green/-yellow/-red/-dark-red`, `--event-text-*` | move to `core_event/static/row.css` | 6 |
| F17 | `styles.css:54-56,97` | `--color-channel-active-*` | move to plugin | 6 |
| F18 | `styles.css:1016-1070` | `.system-status-board/-card/-name`, `.status-green/-yellow/-red/-flashing-red` (note: `.system-status-board` is declared twice — 1016 and 1021 — the duplicate is dead) | move to plugin as `.ce-status-*` | 6 |
| F19 | `styles.css:1161-1176` | `.log-green/-yellow/-red/-dark-red/-flashing-red` | move to plugin as `.ce-sev-*` | 6 |
| F20 | `styles.css:1188-1202` | `.log-line[data-set-clear="clear"]` dimming | move to plugin | 6 |
| F21 | `styles.css:1216-1258` | `.log-action` fixed 84px + `[data-event-color]` + `[data-contrast]` | move to plugin as `.ce-action*` | 6 |
| F22 | `styles.css:1338-1347` | `.log-prefix` (fault prefix) | move to plugin as `.ce-prefix` | 6 |
| F23 | `styles.css:1349-1364` | `.log-offset`, `.log-meta`, `.log-desc` | move to plugin | 6 |
| F24 | `styles.css:1366-1391` | `.log-channels`, `.log-channel`, `.log-channel.is-on` | move to plugin as `.ce-channels`, `.ce-channel` | 6 |
| F25 | `styles.css:1141-1143` | `.log-line { grid-template-columns: 88px 180px 92px 164px minmax(220px,1fr) 24px 28px }` | core keeps `display:grid` + height; plugin sets `grid-template-columns` on `.log-line.ce-row` | 6 |
| F26 | `styles.css:1989-2008` | responsive block re-lays out the same core_event columns | move with F25 | 6 |
| F27 | `app.js:9` | `window.EVENTLOG2_PAGE_DATA` | `window.CINC_PAGE_DATA` | 3 |
| F28 | `app-services.js:6-22` | four-way identity guessing across `logType`/`plugin`/`logData.logTypeId`/`logData.pluginId` | single `pageData.logType.id` | 3 |
| F29 | `PluginLogRow.js:21` | same four-way guess | `services.logType.id` | 3 |

---

## 7. Phase plan

### Phase 1 — Repo hygiene, dependency floor, and the vendor manifest

No behaviour changes. This phase establishes C2, C3, and C4 before any refactoring starts, so
that later phases have something to violate.

**Dead code**

1. Delete `src/cinc/log_generator.py`. Confirm first with
   `grep -rn "log_generator" src tests docs` — it must return nothing but the file itself.
2. Delete `src/cinc/templates/live_view.html` (**D8**). Confirm no route renders it.
3. Delete `cinc-data/eventlog2.sqlite` and `cinc-data/sessions/` (**D10**).
   `cinc-data/` is already gitignored; this is local cleanup only.
4. Delete `docs/preact-migration-plan.md` and `docs/transition-to-esm.md` — both describe
   completed migrations.
5. Delete `docs/search_syntax.rst` and `src/cinc/static/search_syntax.html` (**D14**). Both
   are orphaned; the live documentation is `SearchHelpDialog.js`, which Phase 6 makes
   log-type-aware. Removing the RST is what makes dropping `docutils` safe. *(If you find any
   reference to `search_syntax.html` that this spec missed, stop and keep both files — then
   record the finding in `docs/SPEC_QUESTIONS.md`.)*
6. Fix **D6**: in `plugins/base.py`, change `row_settings: dict[str, Any] = {}` to
   `row_settings: Mapping[str, Any] = MappingProxyType({})`. (This file is deleted in
   Phase 3, but fix it now so the tree is never knowingly broken.)

**Python dependency floor (C2)**

7. `pyproject.toml`:
   ```toml
   dependencies = ["flask", "jinja2", "waitress", "docopt"]

   [project.optional-dependencies]
   dev = ["pytest", "flake8", "black"]
   ```
   Drop `docutils` (**D11**), add `jinja2` (**D12**).
8. `requirements.txt`: mirror the four runtime deps exactly, or delete the file and point the
   README at `pip install -e .` — do not maintain two drifting lists. Prefer deleting it.
9. Recreate the virtualenv from scratch and confirm the app still starts:
   `rm -rf .venv && python -m venv .venv && .venv/bin/pip install -e ".[dev]"`.

**Vendor cleanup and manifest (C4, I13)**

10. Delete the six unreferenced vendor files (**D13**): `htm.min.js`, `htm.mjs`,
    `preact.min.js`, `preact-hooks.min.js`, `preact-signals.min.js`,
    `preact-signals-core.min.js`. Verify each has zero references first:
    ```bash
    for f in src/cinc/static/vendor/*; do
      b=$(basename "$f")
      echo "$b: $(grep -rl "$b" src/cinc --include='*.py' --include='*.html' --include='*.js' \
        | grep -v '/vendor/' | wc -l)"
    done
    ```
    Exactly eight files must remain, each with 2 references.
11. Write `src/cinc/static/vendor/MANIFEST.json` per §4.8, with a real pinned version and
    sha256 for all eight.
12. Write `tools/refresh_vendor.py` (stdlib only).
13. Write `docs/dependencies.md`: the C2 list with one line of justification each; the C3
    prohibition; the C4 vendoring procedure; how to add or upgrade a library; how to verify
    C1 after any vendor change.

**Housekeeping**

14. Add to `.gitignore` (**D9**):
    ```
    /standalone.html
    /standalone/
    ```
15. Add a `[tool.setuptools.package-data]` entry for `static/vendor/MANIFEST.json` so it
    ships with the package (the test in Phase 11 reads it via `importlib.resources`).

**Acceptance**
- `pip install -e ".[dev]"` in a clean venv installs exactly flask, jinja2, waitress, docopt
  (plus their transitive deps) and the dev extras.
- `python -m pytest -q` passes.
- `grep -rn "log_generator\|live_view.html\|docutils\|search_syntax" src tests docs pyproject.toml`
  returns nothing.
- `ls src/cinc/static/vendor` shows exactly eight `.js`/`.mjs` files plus `MANIFEST.json`.
- No `package.json`, `node_modules/`, or bundler config exists anywhere:
  `find . -name package.json -not -path "./.venv/*"` is empty.
- `cinc build --plugin core-event --data src/cinc/plugins/core_event/samples/dev-data.json
  --output /tmp/pre.html` still produces a working offline file. **Keep `/tmp/pre.html` as
  the baseline** — Phase 6 compares against it to prove the CSS move changed nothing visible.

---

### Phase 2 — Curated sample data

Remove all generated data; commit small, hand-authored, feature-complete samples.

#### 2.1 Delete the generated samples

```
src/cinc/plugins/core_event/samples/dev-data.json      (639 KB, 1000 events)  → delete
src/cinc/plugins/core_event/samples/dev-data-3ch.json  (607 KB, 1000 events)  → delete
```

#### 2.2 Author the replacements

Two files. **3-channel and 4-channel data are never mixed in one log**, so there are exactly
two, one per channel configuration:

```
src/cinc/plugins/core_event/samples/sample-3ch.json
src/cinc/plugins/core_event/samples/sample-4ch.json
```

Requirements for **both** files:

- **40–60 events.** Small enough to read in a diff; large enough to scroll.
- Pretty-printed with 2-space indentation, one file ending in a newline.
- Header keys: `start`, `end`, `hours`, `channels`, `channelCount`. **No `seed`, no `modes`**
  — those were generator artifacts. (If `modes` is still consumed anywhere, remove that
  consumer; grep before deleting.)
- `start`/`end` are realistic ISO-8601 UTC instants spanning ~30 minutes.
- Events are ordered by ascending `utctime` with `row_id` starting at 1 and incrementing by 1.

Feature checklist — the sample must exercise **every** core_event feature, because these
files are the only fixtures the viewer and the tests have:

| Must contain | Why |
|---|---|
| At least one event of each severity: `Green`, `Yellow`, `Red`, `Flashing Red` | severity timeline view + row colour classes |
| At least 3 matched `set`→`clear` pairs on the **same** `system/subsystem/unit/code/channel` | pairing, duration labels, jump links |
| One pair where the clear lands on **all** channels | exercises the `collapsed: true` "ALL" match summary |
| One pair where the clear lands on a **subset** of channels | exercises the per-channel, non-collapsed match summary |
| At least one **unpaired `set`** (never cleared) | open-fault handling |
| At least one **unpaired `clear`** (no preceding set) | defensive path in `_derive_core_events` |
| At least 6 events with `id: "pwr_bus"` and `data.bus.load_pct` as a number, values 60–72, spread across the timespan | **required** — the `bus-load` timeline view filters on exactly `event.id === "pwr_bus" && typeof event.data.bus.load_pct === "number"` and will render empty without them |
| At least 4 distinct `system` values | subsystem-status chart panel |
| At least one event with a **nested `data`** object 3 levels deep | detail-panel data tree, `$.*` deep search |
| At least one event with **no `data`** | the "no event data" row indicator |
| At least one event where `system` is an **object** `{"id": …, "name": …}`, and one using the `subsystem_id` / `subsystem_name` split form | `_normalize_entity_fields` and the `_search` alias arrays |
| At least one event with a `code` containing a hyphen (e.g. `"PWR-214"`) and one without | the two branches of `_build_fault_prefix` |
| At least one event with a long `description` (>80 chars) | row truncation / ellipsis |

`sample-3ch.json` uses `channels: ["A","B","C"]`, `channelCount: 3`.
`sample-4ch.json` uses `channels: ["A","B","C","D"]`, `channelCount: 4`.
The two files should not be copies — vary the systems and fault names so a reviewer can tell
them apart at a glance.

#### 2.3 Make samples a declared asset

Add to `LogType` (Phase 3 will move this file; declare the attribute now on the existing
plugin base so Phase 2 is independently shippable):

```python
@dataclass(frozen=True)
class Sample:
    slug: str        # "sample-4ch"
    title: str       # "4-channel boot sequence"
    path: str        # "samples/sample-4ch.json"
```

`sample_paths = ("samples/sample-3ch.json", "samples/sample-4ch.json")`, and a
`samples()` helper that turns each path into a `Sample` (slug = filename stem, title =
slug with hyphens replaced and title-cased unless the plugin overrides `samples()`).

#### 2.4 Rework the `/` route (leak **B4**)

`/` currently hardcodes `core-event` and `samples/dev-data.json`. Replace with:

- `GET /` → a new template `templates/home.html` listing every registered log type, its
  description, its log count, and a "View sample" link per declared sample.
- `GET /demo/<log_type_id>/<sample_slug>` → loads that sample from disk, runs it through
  `log_type.build_page_data()`, renders `index.html`. Returns 404 for unknown ids.

This gives every plugin — not just core_event — a zero-setup demo, and it removes the last
core reference to a specific sample file.

**Acceptance**
- `src/cinc/plugins/core_event/samples/` contains exactly two files, each < 100 KB.
- `grep -rn "dev-data" src tests docs templates` returns nothing.
- `cinc serve`, then `/` lists Core Event with two sample links; both render a working viewer.
- The severity timeline, bus-load timeline, and subsystem-status chart all show data for
  both samples.
- A test asserts each sample satisfies the feature checklist (see Phase 11, `test_samples.py`).

---

### Phase 3 — Unify the backend into `LogType`

The largest phase. Do it in one commit; the tree will not compile halfway through.

#### 3.1 Create `src/cinc/core/contract.py`

```python
REQUIRED_EVENT_KEYS = ("row_id", "time")

def validate_events(events: list[dict], log_type_id: str) -> None:
    """Raise ValueError naming the log type, the offending index, and the missing or
    malformed key. Checks: row_id is an int; time parses via fromisoformat (accepting a
    trailing 'Z'); row_id values are unique."""

def parse_time(value: str) -> datetime: ...
def format_time(value: datetime) -> str:   # "…Z", millisecond precision at most
    ...
```

#### 3.2 Rewrite `src/cinc/logs/types.py`

Contains `LogRecord` (unchanged), `LogDocument` (renamed from `EventLogDocument`), `Sample`,
and `LogType` exactly as specified in §4.2. Implement the `final` methods on the base class,
including asset resolution (`read_asset_text` via `importlib.resources`, defaulting
`asset_package` to the subclass's package).

#### 3.3 Create `src/cinc/core/registry.py`

`LogTypeRegistry` exactly as specified in §4.3.

#### 3.4 Delete

```
src/cinc/plugin_manager.py
src/cinc/plugins/base.py
src/cinc/plugins/core_event/log_types.py
```

and remove the `EventLogSourcePlugin` / `EventLogViewPlugin` / `CoreEventPlugin` exports from
`src/cinc/plugins/__init__.py`.

#### 3.5 Rewrite `src/cinc/plugins/core_event/log_type.py`

One class, `CoreEventLogType(LogType)`, merging today's `CoreEventPlugin` (503 lines) and
`CoreEventBootLogType` (128 lines). Keep every normalization helper (`_parse_datetime`,
`_normalize_entity_fields`, `_resolve_channel_catalog`, `_derive_core_events`,
`_build_fault_prefix`, `_build_row_display`, `_summarize_matches`, …) verbatim — they are
correct plugin logic and this refactor must not change core_event's output semantics.

Required changes inside the merge:

- `id = "core_event"`, `name = "Core Event"`.
- `normalize_events(payload)` returns the derived event list, and **every event carries
  `time` as its canonical ISO-8601 instant** (today the value is computed into both `time`
  and `utctime`; keep `utctime` as a plugin-private field, since `row.js` displays it).
- `view_config(payload)` returns
  `{"rowSettings": {"channels": [...]}, "charts": ["systems"], "timelineViews": ["severity", "bus-load"]}`.
- `search_config(payload)` returns
  `{"labelField": "name", "excludeFromBareTerms": ["data"], "aliasSuffix": "_search",
    "fields": [...], "examples": ["color:Red", "system:Power", "set_clear:set", "data.$.*~voltage"]}`.
- `log_summary(payload)` returns the header pairs previously stuffed into `logData`:
  `{"Channels": "A, B, C, D", "Duration": "…", "Events": "…", "Window": "start → end"}`.
- `style_paths = ("static/row.css",)` (the file is created in Phase 6; create an empty
  placeholder now).
- **Delete `normalize_stream_events`** — live and batch now share `normalize_events`
  (improvement I9). Guard: if `payload` has no `start`/`end`, the existing
  `_resolve_fallback_start` path already handles it.
- **Do not** put `pluginId`, `logTypeId`, `channels`, `channelCount`, `start`, `end`, `hours`,
  `seed`, or `modes` into `logData` (improvement I5).

#### 3.6 Rewrite `src/cinc/app.py` as an app factory

```python
def create_app(data_dir: Path | None = None) -> Flask:
    registry = LogTypeRegistry.discover()
    store = LogStore((data_dir or Path(os.environ.get("CINC_DATA_DIR", "./cinc-data"))) / "cinc.sqlite")
    live = LiveSessionRegistry(registry, store)      # Phase 5; a no-op stub for now
    app = Flask(__name__)
    app.extensions["cinc"] = {"registry": registry, "store": store, "live": live}
    app.register_blueprint(create_home_blueprint(registry, store))
    app.register_blueprint(create_logs_blueprint(registry, store))
    app.register_blueprint(create_live_blueprint(live))
    return app
```

`app = create_app()` stays at module scope so `__main__.py` and any WSGI entry keep working.
**`app.py` must contain zero imports from `cinc.plugins`.** That is the headline acceptance
criterion for this phase.

#### 3.7 Update `logs/routes.py`, `logs/registry.py`, `standalone.py`, `__main__.py`

Store schema, done now so Phase 8 does not have to revisit it:

- Drop the `log_type` column from `log_records`; add the `log_record_types` table from §4.9;
  add `LogRecord.log_type_ids: list[str]`; make `list_records(log_type_id=…)` join through
  it. With one log type registered this behaves exactly as before. Delete `cinc-data/`
  rather than migrating (§0.4).
- `normalize_events` takes `*, row_id_base: int = 0` from the start, and `assemble_page_data`
  exists with the single-section path only. Phase 8 adds the multi-section caller, not the
  mechanism.

Routes and CLI:

- Delete `src/cinc/logs/registry.py` (superseded by `core/registry.py`).
- `logs/routes.py`: `registry.get(log_type_id)` with no alias fallback; call
  `log_type.build_page_data(payload)` (not the removed `build_view_page_data`); attach
  `page_data["log"] = {...record fields...}` and `page_data["live"]` in the route, since
  those are record-level, not payload-level.
- `standalone.py`: `build_page_data_script` emits `window.CINC_PAGE_DATA = …`. Replace the
  `build_page_data_*_from_path` helpers with registry lookups.
- `__main__.py`: `cinc plugins` → `cinc log-types`; add `cinc samples` listing each log
  type's declared samples; `cinc build --log-type <id> --data <path>`.

#### 3.8 Frontend identity cleanup (F27, F28, F29)

- `runtime.js`: rename the export `EventLog2` → `Cinc`; rename the four registration methods
  per §4.5; delete all aliases.
- `app.js`: `window.CINC_PAGE_DATA`.
- `app-services.js`: `const logType = pageData?.logType ?? null;` — delete the `plugin`
  fallback chain and the `pageData.payload` fallback.
- `PluginLogRow.js:21`: `const logTypeId = services?.logType?.id || "";`
- Update every importer of `runtime.js`: `plugins/core_event/static/row.js`,
  `plugins/core_event/static/charts.js`, `LogMainChart.js`.
- `templates/base.html`: the import map entry `"static/runtime.js"` stays; no change needed.

**Acceptance**
- `grep -rn "cinc.plugins\|core_event\|core-event" src/cinc --include="*.py" | grep -v "^src/cinc/plugins/"`
  returns **nothing**.
- `grep -rn "EventLog2\|EVENTLOG2\|registerPlugin\|plugin_manager\|LogTypeDefinition\|EventLogSourcePlugin"
  src tests` returns **nothing**.
- `python -m pytest -q` passes (tests updated in Phase 11).
- Import, view, delete, and standalone build all work end-to-end for both samples.

---

### Phase 4 — The time model

Make `time` the only temporal concept core understands.

#### 4.1 Backend

- `core/contract.py`: `validate_events` also requires `log_type` on every event and rejects
  one whose value disagrees with the producing type (§4.1).
- `logs/store.py`: delete `_event_time()`. `append_events` requires `event["time"]` and
  `event["log_type"]`, and raises `ValueError` if either is missing or the time is
  unparseable. (Fixes **D3** / **B6**.) Keep the `time` column and its index unchanged, and
  write the per-event type into the existing `log_events.log_type` column instead of copying
  the record's type.
- `logs/store.py`: order `get_events` by `COALESCE(row_id, id), id` as today — row_id order
  is the authoring order and must be preserved.

#### 4.2 Frontend — one accessor, used everywhere

Add to `src/cinc/static/shared.js`:

```js
export const eventTimeMs = (event) => {
  const parsed = Date.parse(event?.time);
  return Number.isFinite(parsed) ? parsed : NaN;
};
```

Then apply, in this order:

| File | Change |
|---|---|
| `LogMainChart.js:279-282` | `getEventAbsoluteMs` → `eventTimeMs` (F1) |
| `LogMainChart.js:296-302` | bounds = `eventTimeMs(events[0])` / `eventTimeMs(events.at(-1))`; drop the `norm_time` arithmetic and its comment (F2) |
| `LogMainChart.js:318-322` | `getEventMs` → `eventTimeMs` (F3) |
| `LogMainChart.js:462` | follows F1 (F4) |
| `LogMainChart.js:575,666` | replace the fixed `.toISOString().slice(11,19)` with `formatInstant(ms, spanMs)`: span < 2 min → `HH:MM:SS.mmm`; < 24 h → `HH:MM:SS`; ≥ 24 h → `MM-DD HH:MM`. Consult `Cinc.resolveTimeFormatter(logTypeId)` first (F5) |
| `LogMainChart.js:598,603-604` | jump payload becomes `{rowId, timeMs}` (F6) |
| `MainViewShell.js:152` | `setLogScroll({ timeMs: eventTimeMs(current), rowId: current.row_id })` (F7) |
| `LogVirtualList.js:20-40` | rename `findClosestIndexBySeconds` → `findClosestIndexByTime`; binary-search on `eventTimeMs`; update the two call sites in `MainViewShell.js` (F8) |
| `SearchPanel.js:34` | sort bookmark events by `eventTimeMs` (F9) |
| `state/navigation.js:4-20` | `normalizeJumpTarget` / `normalizeScrollState` take `timeMs` (a finite number, no `Math.floor`, no `Math.max(0, …)` clamp — epoch ms are large and must not be floored to seconds) instead of `seconds` (F10) |

#### 4.3 Defect fixes in this phase

- **D1**: `state/activity.js` — replace the `validIds` set captured at construction with a
  lookup against `allEventsSignal.value`, so bookmarks and comments work on live-appended
  events. `createActivityState` must therefore receive the store's `allEventsSignal`, not a
  snapshot array; adjust `createViewerStore` accordingly.

**Acceptance**
- `grep -rn "norm_time\|utctime" src/cinc/static src/cinc/logs src/cinc/live src/cinc/templates`
  returns **nothing**.
- Clicking a chart marker scrolls the list to that row; scrolling the list moves the chart
  cursor; both work on a sample whose events span more than 24 hours (author a throwaway
  fixture to check, do not commit it).
- Bookmarking an event that arrived over SSE during a live capture persists and reappears
  after reload.

---

### Phase 5 — Generalize live capture

#### 5.1 `src/cinc/live/session.py`

- `LiveSource` ABC exactly as §4.7.
- `SessionManager(store, log_type, source)` — keep the existing thread + `queue.Queue`
  fan-out design. Changes:
  - `start()` calls `source.start(session_id)` and stores the returned dict as
    `metadata["payload_header"]` untouched.
  - `_poll_loop` calls `log_type.normalize_events({"events": raw, **header})` — the same
    entry point batch import uses (I9).
  - Honour `source.finished()`: end the capture cleanly when the source says it is done.
  - Use `source.poll_interval` rather than the class constant `POLL_INTERVAL`.
  - **Fix D2**: `stop()` must set `self._stop_event`, join the thread, call `source.stop()`,
    set `self._thread = None`, and let `_poll_loop`'s own epilogue call `complete_log`.
    Delete the inverted `if not self._thread:` branch. Make `stop()` idempotent.
- `LiveSessionRegistry(registry, store)`:
  - `managers() -> dict[str, SessionManager]` — one per log type whose
    `create_live_source()` returns non-`None`.
  - `get(log_type_id)`, `active()` → list of `(log_type_id, session_id)`.

#### 5.2 `src/cinc/live/replay.py` — the generic dev live source

Replaces the random generator. Any plugin can use it; it lives in core because it contains
no domain knowledge.

```python
class SampleReplaySource(LiveSource):
    """Replays a committed sample file as if it were arriving live.

    Reads the sample once at start(). Emits the events whose original timestamps fall
    inside the elapsed window, scaled by `speed`. Header keys other than the events list
    are returned verbatim from start(), so the log type sees exactly the shape it would
    see from a real source. Deterministic: the same sample always yields the same rows
    in the same order.
    """
    def __init__(self, sample_path: Path, *, events_key: str = "events",
                 time_key: str = "utctime", speed: float = 10.0,
                 poll_interval: float = 1.0): ...
```

`time_key` is a constructor argument, not a hardcoded field, because the *raw* input shape
differs per plugin — this class reads raw rows, before normalization.

#### 5.3 `CoreEventLogType.create_live_source()`

```python
def create_live_source(self) -> LiveSource | None:
    return SampleReplaySource(self.asset_path("samples/sample-4ch.json"), speed=20.0)
```

**Delete `src/cinc/plugins/core_event/live_monitor.py` in full** — the `_SAMPLE_FAULTS`
catalogue and `random.choices` weighting are exactly the "event generation" this refactor
removes.

#### 5.4 Routes and templates (B7–B10)

- `live/routes.py`: routes become `/live` (index over all live-capable log types),
  `/live/<log_type_id>/start`, `/live/<log_type_id>/stop`, `/live/<session_id>/stream`.
- `templates/live_sessions.html`: iterate `live.managers()`; show each log type's name, its
  `available()` state and `unavailable_reason()`; remove the `core_event` literals and the
  "core event log" prose.
- `templates/log_view.html:26`: replace `record.metadata.channels | join(", ")` with a loop
  over `page_data.log.summary` rendering `key: value` pairs.

**Acceptance**
- `grep -rn "random\|SAMPLE_FAULTS\|detect_system\|get_channels" src/cinc` returns nothing.
- Start live → rows appear over SSE in timestamp order, the list follows when "lock to
  bottom" is on, and the count in the banner increases.
- Stop → the record flips to `completed`, appears in the list, and reopens with all rows.
- Start → stop → start again works (regression test for **D2**).
- The replayed live log and the directly-imported same sample produce **identical**
  normalized events apart from `row_id` assignment — assert this in a test (I9).

---

### Phase 6 — De-leak the frontend

#### 6.1 Plugin-owned CSS (F16–F26)

1. Create `src/cinc/plugins/core_event/static/row.css`.
2. **Move** (do not copy) the line ranges listed in F16–F26 out of
   `src/cinc/static/styles.css` into it.
3. Rename every moved class to the `ce-` namespace (I12), and update
   `plugins/core_event/static/row.js` and `charts.js` to emit the new names:

   | old | new |
   |---|---|
   | `.log-green` … `.log-flashing-red` | `.ce-sev-green` … `.ce-sev-flashing-red` |
   | `.log-channels`, `.log-channel` | `.ce-channels`, `.ce-channel` |
   | `.log-action`, `.log-action-label` | `.ce-action`, `.ce-action-label` |
   | `.log-prefix` | `.ce-prefix` |
   | `.log-offset`, `.log-meta`, `.log-desc`, `.log-details`, `.log-title-row`, `.log-name`, `.log-time` | `.ce-offset`, `.ce-meta`, `.ce-desc`, `.ce-details`, `.ce-title-row`, `.ce-name`, `.ce-time` |
   | `.log-match`, `.match-link`, `.match-value`, `.match-preview`, `.match-detail`, `.match-separator` | `.ce-match`, `.ce-match-link`, … |
   | `.system-status-*`, `.status-*` | `.ce-status-*` |
   | `.data-indicator`, `.data-icon*` | `.ce-data-indicator`, `.ce-data-icon*` |

   Keep `.log-line`, `.log-selected`, `.log-highlight`, `.is-bookmarked`, `.bookmark-toggle`
   in core — those are the contract (§4.6).
4. F25: core's `.log-line` keeps `display:grid`, `align-items`, `height`, `border-bottom`,
   hover, and the state classes. The plugin's row root becomes
   `class="log-line ce-row ce-sev-red …"`, and `row.css` sets
   `.log-line.ce-row { grid-template-columns: 88px 180px 92px 164px minmax(220px,1fr) 24px 28px; }`
   plus the responsive override from F26.
5. Set `style_paths = ("static/row.css",)` on `CoreEventLogType`.
6. Delete the duplicate `.system-status-board` block (`styles.css:1016` vs `1021`).

Verify the plumbing works: `app.js:loadPageStyles` already injects `view.styles` into a
`<style data-role="plugin-styles">` element, and `standalone.py` inlines page data, so this
path needs no new code — it is currently just unused because
`CoreEventPlugin._build_view_config` omitted `styles`. Phase 3's base-class
`build_page_data` fixes that.

#### 6.2 Search configuration (F12–F15)

`services/search.js` gains a config object rather than constants:

```js
export const DEFAULT_SEARCH_CONFIG = {
  labelField: "name",
  excludeFromBareTerms: ["data"],
  aliasSuffix: "_search",
};
export const getQueryPredicate = (query, config = DEFAULT_SEARCH_CONFIG) => { … };
```

- line 27: `getFieldValue(event, path + config.aliasSuffix)` — skip when `aliasSuffix` is
  falsy.
- lines 88–90, 116–117: `field === config.labelField`.
- line 120: `if (config.excludeFromBareTerms.includes(key)) continue;`

Call sites (`SearchPanel.js:138`, `MainViewShell.js:126`) pass
`services.search` (from `pageData.search`, merged over the defaults). Add
`search` to the object returned by `createRootServices`.

`SearchHelpDialog.js`: delete `HELP_EXAMPLES`; render `services.search.examples` with a
generic fallback (`["name:foo", "time>2026-01-01", "description~timeout", "NOT level:DEBUG"]`).
Replace the `data.bus.load_pct` reference in the prose at line 67 with a neutral example.
Prefer `services.search.fields` over the sampled-first-event field list when it is present,
falling back to `getSearchFieldPaths(events)` when it is not.

#### 6.3 Detail summary (F11)

`EventSummary.js` currently hardcodes core_event's field layout. Replace with:

- New runtime hook `Cinc.registerDetailSummary(logTypeId, Component)`; the component
  receives `{ event, view }`.
- Core's `EventSummary` resolves the registered component and renders it. If none is
  registered it falls back to the two contract fields:
  `<div class="detail-title">${event.name ?? event.row_id}</div>` and
  `<div class="detail-meta">${event.time}</div>`.
- `plugins/core_event/static/row.js` registers a `CoreEventDetailSummary` that renders the
  current four lines (`name` / `utctime • set_clear` / `description` /
  `system/subsystem/unit/code`).

**Acceptance**
- `grep -rn "set_clear\|subsystem\|channels\|Flashing\|bus-load\|core_event\|core-event"
  src/cinc/static src/cinc/templates` returns **nothing**.
- `styles.css` no longer contains `--event-green`, `.log-green`, `.log-channel`,
  `.system-status-`, `[data-set-clear]`, or `[data-event-color]`.
- The core_event viewer looks pixel-identical to before the move (compare screenshots).
- The standalone build still styles rows correctly — plugin CSS travels via `view.styles`.

---

### Phase 7 — The second reference plugin (conformance test)

This phase exists to *prove* Phases 3–6 worked. Build it strictly against `docs/plugins.md`
as written; if you need to read core source to make it work, that is a bug in the interface
or the docs, and you must fix the interface or the docs rather than the plugin.

Create `src/cinc/plugins/text_log/`, deliberately sharing **none** of core_event's concepts —
no channels, no set/clear pairing, no severity ladder, no system hierarchy, no time offsets.

**Input shape** (`samples/sample.json`):

```json
{
  "source": "app.log",
  "host": "build-agent-04",
  "lines": [
    { "ts": "2026-04-03T08:00:01.120Z", "level": "INFO",  "logger": "server",
      "message": "listening on :8080" },
    { "ts": "2026-04-03T08:00:04.880Z", "level": "ERROR", "logger": "db.pool",
      "message": "connection refused after 3 retries",
      "fields": { "attempt": 3, "host": "db-primary", "timeout_ms": 2000 } }
  ]
}
```

Note that the input uses `ts` and `lines`, **not** `time` and `events`. This is intentional:
it verifies that `normalize_events` is genuinely the only place raw shape is interpreted.

**`TextLogType`:**

| Member | Value |
|---|---|
| `id` | `text_log` |
| `name` | `Text Log` |
| `description` | `Plain timestamped application log lines` |
| `parse_import` | accepts a `.json` file or JSON body with a `lines` array; also accepts a plain `.log`/`.txt` upload parsed with the regex `^(?P<ts>\S+)\s+(?P<level>[A-Z]+)\s+(?P<logger>\S+)\s+(?P<message>.*)$` |
| `detect` | as given in §4.9 |
| `normalize_events` | `row_id` = `row_id_base + 1-based index`; `time` = normalized `ts`; carries `level`, `logger`, `message`, and `data` = the `fields` object when present |
| `rowHeightHint` | `56` (see the note below on why it differs from core_event) |
| `list_columns` | `[{"key":"line_count","label":"Lines"},{"key":"errors","label":"Errors"}]` |
| `view_config` | `{"rowSettings": {"levels": ["DEBUG","INFO","WARN","ERROR"]}, "charts": [], "timelineViews": ["levels"]}` |
| `search_config` | `{"labelField": "message", "excludeFromBareTerms": ["data"], "aliasSuffix": null, "fields": ["time","level","logger","message"], "examples": ["level:ERROR","logger~db","message~refused","NOT level:DEBUG"]}` |
| `log_summary` | `{"Source": …, "Host": …, "Lines": …, "Errors": …}` |
| `create_live_source` | `SampleReplaySource(self.asset_path("samples/sample.json"), events_key="lines", time_key="ts", speed=8.0)` |
| `sample_paths` | `("samples/sample.json",)` |
| `style_paths` | `("static/row.css",)` |
| `script_paths` | `("static/row.js", "static/charts.js")` |

**`static/row.js`** — deliberately a **two-line** row: `message` on the first line, then
`time · level · logger` on the second, with a level badge and the bookmark button. Root class
`log-line tl-row tl-level-<lowercase level>`. Register via `Cinc.registerRow`. Also register a
`Cinc.registerDetailSummary` component showing `message` / `time · level · logger`.

**`static/row.css`** — `.log-line.tl-row { grid-template-columns: 1fr 28px; height: 56px; }`
plus `.tl-level-*` badge colours. Nothing else.

> The two-line, ~56 px row is a **deliberate test fixture**: it must differ from core_event's
> ~38 px single-line row so that Phase 8's variable-height virtualization is exercised by a
> real case rather than a synthetic one. Put that sentence in a comment at the top of
> `row.css` so nobody later "fixes" the inconsistency. Both lines must be clipped, not
> wrapped — the height is fixed regardless of message length (§4.5 item 4).

**`static/charts.js`** — one timeline view:

```js
Cinc.registerTimelineView("text_log", {
  id: "levels", label: "Levels", kind: "histogram", stacked: true,
  datasets: ["DEBUG","INFO","WARN","ERROR"].map((level) => ({
    label: level, filter: (event) => event.level === level, /* colours */
  })),
});
```

**`samples/sample.json`** — 40–60 lines, ~10 minutes of wall time, all four levels present,
at least three lines with a nested `fields` object, at least one message over 120 characters,
at least one burst of five lines inside the same second (tests sub-second ordering).

Register the entry point:

```toml
[project.entry-points."cinc.log_types"]
core-event = "cinc.plugins.core_event:CoreEventLogType"
text-log   = "cinc.plugins.text_log:TextLogType"
```

**Acceptance — this is the real test of the whole refactor**
- `pip install -e .` then `cinc log-types` lists both.
- `/` lists both log types with their samples.
- Importing the text-log sample renders rows, the levels timeline, search, bookmarks,
  comments, the detail panel, and the data tree — with **zero** core_event CSS or JS loaded
  on that page (verify in devtools: no `ce-` classes, no `charts.js` from core_event).
- The chart type dropdown shows only `timeline` for text_log (it registers no chart type),
  and shows `timeline` + `systems` for core_event.
- `cinc build --log-type text_log --data <sample>` produces a working standalone HTML.
- Live replay works for text_log.
- **No file under `src/cinc/plugins/text_log/` imports anything from
  `src/cinc/plugins/core_event/`.** Assert this with a test.

---

### Phase 8 — Mixed-type logs and variable row heights (I16, I17, I18)

Everything here needs two log types to exist, which is why it follows Phase 7 rather than
sitting with the backend work. Phases 3 and 4 already put the enabling pieces in place: the
`row_id_base` parameter, the `log_type` event key, the `logTypes` envelope map, and
`log_record_types`.

Do this phase in three commits — bundles, then dispatch, then heights — because the third is
the one most likely to need iteration and you want a clean revert point.

#### 8.1 Bundles and type resolution (I16, I17)

1. `core/bundle.py`:
   - `is_bundle(payload) -> bool` — the `cinc` key is present.
   - `read_sections(payload, registry, *, explicit_type=None) -> list[Section]` implementing
     the resolution order in §4.9, with the two exact error messages specified there.
   - Strip the reserved top-level `logType` key before handing a payload to a plugin.
2. `core/assemble.py`: `assemble_page_data(sections)` per §4.2, including `row_id_base`
   allocation, the `log_type` stamp, per-section validation, and the `(time, row_id)` merge
   sort.
3. `LogType.detect()` on both reference plugins, exactly as given in §4.9.
4. `LogType.normalize_events(payload, *, row_id_base=0)` honoured by both plugins. For
   core_event this means `row_id = row_id_base + (raw row_id or index + 1)`; its cross-links
   already read the normalized `row_id`, so they follow automatically. **Write the test for
   this before the code** — it is the subtlest part of the phase.
5. Store: drop `log_records.log_type`, add `log_record_types`, add
   `LogRecord.log_type_ids`, and make `list_records(log_type_id=…)` join through it.
   Delete `cinc-data/` rather than migrating.
6. CLI: `cinc build [--log-type TYPE] --data PATH... [--output PATH]`. `--data` repeats;
   `--log-type` becomes an optional override applied only to files that declare nothing.
7. Import route: accept multiple files in one multipart upload; resolve each independently.
8. `templates/logs_import.html`: allow multiple file selection; explain that the type is
   read from the file and that mixed bundles are accepted.

#### 8.2 Per-row dispatch in the frontend (I16)

Everything that currently reads one log-type identity now reads `event.log_type`:

| Location | Change |
|---|---|
| `PluginLogRow.js:21-22` | `Cinc.resolveRow(event.log_type)` instead of `services.logType.id` |
| `PluginLogRow.js` (props) | pass `logTypeConfig = services.logTypes[event.log_type]` as the `view` prop, so a row component reads *its own* `rowSettings` |
| `EventSummary.js` | resolve the detail-summary component by `selectedEvent.log_type` |
| `services/search.js` | `getQueryPredicate(query, resolveConfig)` where `resolveConfig(event)` is `configs.get(event.log_type) ?? DEFAULT_SEARCH_CONFIG` — one `Map.get` per event per predicate |
| `SearchHelpDialog.js` | with one type, show its examples and fields; with several, group them under type-name subheadings |
| `LogMainChart.js:86-89, 426-431` | a chart type or timeline view is offered if its `pluginId` matches **any** type present in the log; label it `"<Type name> · <view label>"` when more than one type is present |
| `app-services.js` | expose `logTypes` (the map) and `presentTypes` (ids actually appearing in `logData.events`, computed once); drop the single `logType` |

A row whose `log_type` has no registered component keeps the existing `.log-row-error`
fallback — with two plugins this is now a reachable state, so make the message name the
missing type.

#### 8.3 Variable row heights (I18)

Implement §4.10 in this order, verifying after each step:

1. `hooks/use-virtual-list.js` — add the optional `offsets` input; `buildRange` binary-searches
   when it is present and keeps the existing arithmetic when it is not. Fix `scrollToIndex`
   to use the target row's own height for `center`/`end` alignment.
2. A shared helper — `static/logview/row-metrics.js`:
   - `buildOffsets(events, heights, fallback) -> Float64Array`
   - `indexAtOffset(offsets, top) -> number` (binary search)
   - `measureRowHeights(hostEl, presentTypes) -> Map<string, number>`
3. `MainLogPane` / `SearchResultsPane` — hidden probe renders one row per present type,
   each tagged `data-probe-type`, measured together in one pass.
4. `MainViewShell` — replace `rowStride` with the offsets array in `smoothScrollToIndex`,
   `emitScrollState`, the lock-to-bottom slack test, and the spacer height.
5. `SearchPanel` / `SearchResultsContent` — same, with the search pane's own offsets array.
6. Re-measure on `ResizeObserver` fire and on `data-theme` change.

Verify at each step with a single-type log first (behaviour must be identical to before),
then with the mixed sample.

#### 8.4 Test data

1. Give `text_log` a deliberately **taller** row than `core_event` — two lines, message
   above and `time · level · logger` below, ~56 px against core_event's ~38 px. This is a
   test fixture as much as a design choice; say so in a comment in `row.css`.
2. Add `src/cinc/samples/mixed.json` — a bundle interleaving both types over the same
   ~10-minute window, roughly 30 core_event rows and 30 text_log rows, with timestamps that
   genuinely interleave rather than concatenate, and at least three instants where a row of
   each type shares the same second (exercises the `(time, row_id)` tiebreak). Core-level
   samples are not owned by a log type, so the home route lists them separately from
   per-type samples.
3. `cinc build --data <core_event sample> --data <text_log sample>` must produce the same
   merged result as building `mixed.json` — assert it.

**Acceptance**
- `cinc build --data src/cinc/plugins/core_event/samples/sample-4ch.json` works with **no**
  `--log-type` flag, resolved by rule 2 or 4.
- A file with a bogus `"logType": "nope"` fails with an error listing the registered ids.
- A file that no type claims fails with the "cannot determine" message; a deliberately
  ambiguous fixture fails with the "ambiguous" message.
- `mixed.json` renders one time-ordered list in which core_event rows and text_log rows
  are visibly different heights and each is rendered by its own component.
- Scrolling the mixed list is smooth; the scrollbar length is stable; jumping to a row by
  chart click, search result, and cross-link all land the row correctly centred.
- `/logs` shows the mixed log under **both** Core Event and Text Log, and the counts are exact.
- Search across the mixed log applies each row's own `labelField` — verify with a bare term
  that prefix-matches a core_event `name` and a text_log `message` differently.
- Live capture still works for each type individually.
- Single-type logs are byte-identical in behaviour to Phase 7: same heights, same scrolling.

---

### Phase 9 — Standalone build hardening (C1, I11, I15)

This phase makes the headline constraint mechanically enforced rather than merely observed.

#### 9.1 Auto-discover frontend modules (I11)

`standalone.py`'s `SCRIPT_PATHS` is a hand-maintained list of ~60 module paths. Every new core
component must be added by hand or it silently vanishes from the standalone build — a C1
violation that ships silently because the served app still works.

Replace with discovery:

```python
def _discover_frontend_modules() -> dict[str, str]:
    """Walk the packaged static/ tree for *.js and *.mjs, excluding the vendor globals
    listed in the manifest. Order does not matter: the loader registers every module into
    a blob-URL map keyed by path and resolves imports lazily from ENTRY_POINT."""
```

Derive `GLOBAL_SCRIPTS` and `BARE_MODULES` from `vendor/MANIFEST.json` (§4.8) rather than from
duplicate literals: globals are the `kind: "global"` entries in manifest order, and
`BARE_MODULES` maps each `kind: "module"` entry's `specifier` to its `file`. This makes the
manifest genuinely single-source and removes the third place that could drift.

#### 9.2 A standalone-mode contract in the frontend

Standalone mode is currently detected by `document.body.classList.contains("app-body-standalone")`
in `app-services.js`. Make the guarantee explicit rather than incidental:

- `app.js` must not call `connectLiveStream` when standalone. Today this happens to hold
  because `pageData.live` is never set by the build; add an explicit
  `if (isStandalone()) return;` so a future change cannot break it silently.
- Any core code that would reach the network must be behind the same check.

#### 9.3 The self-containment test (I15)

Add `tests/test_standalone.py::test_output_is_self_contained`, which builds a standalone file
for **every** registered log type and every declared sample, then asserts on the HTML text:

| Assertion | Rationale |
|---|---|
| no `src="http`, `src="//`, `href="http`, `href="//` | C1: no remote assets |
| no `@import url(` with a non-`data:` target | C1: no remote CSS |
| the only `src=` / `href=` values are `data:` URIs or absent | C1 |
| the string `fetch(`, `XMLHttpRequest`, `EventSource` appears **only** inside vendor library text, never in core or plugin module text | C1: no runtime network |
| every `.js`/`.mjs` under `static/` except vendor globals appears in the module map | I11 |
| every `kind: "global"` manifest file appears inline | I11 |
| the plugin's CSS from `view.styles` appears inline | Phase 6 regression |
| `window.CINC_PAGE_DATA` appears exactly once | envelope |
| output size is under 4 MB | keeps the file mailable; today's is 1.9 MB |

Split the "fetch/EventSource only in vendor" check by extracting the module map JSON from the
built HTML and scanning each entry keyed by path — vendor paths are exempt, everything else
is not. That is more robust than scanning raw text.

**Acceptance**
- `cinc build --log-type core_event --data src/cinc/plugins/core_event/samples/sample-4ch.json`
  and the same for `sample-3ch.json` and `text_log` all produce files that open from
  `file://` with the machine's network **physically disabled**, with a clean console.
- In each, verify by hand: rows render, virtual scroll works, search returns results, the
  chart draws, clicking a row fills the detail panel, and bookmarks/comments are disabled.
- Adding a new file under `static/logview/` requires no edit to `standalone.py`; prove it by
  adding a throwaway module, rebuilding, confirming it is present, then removing it.
- `test_standalone.py` fails if you temporarily add `<script src="https://…">` to
  `templates/standalone.html`. Verify the test actually catches it, then revert.

---

### Phase 10 — Documentation

`docs/dependencies.md` was written in Phase 1; review it now for accuracy against the
finished tree. Delete `docs/api-design.md` (v1, stale) and write:

**`docs/plugins.md`** — the plugin author guide. Sections:
1. What a plugin is; the directory layout.
2. The event contract (§4.1) — lead with this; it is the thing authors get wrong.
3. `LogType` reference (§4.2) — every method, what it receives, what it must return, whether
   it is required.
4. Page-data envelope (§4.4) with a full annotated example.
5. Frontend registration (§4.5): row component, detail summary, timeline views, chart types,
   time formatter. Include the row-component contract checklist verbatim.
6. CSS ownership and namespacing (§4.6).
7. Live sources (§4.7), including how to use `SampleReplaySource` during development.
8. Sample data requirements: at least one committed sample per plugin, small, feature-complete.
9. **Self-describing data (§4.9)** — how a file declares its type, the bundle envelope, how
   to write a good `detect()`, and the `row_id_base` rule with a worked cross-link example.
   Be explicit that a plugin never sees the envelope and never sets `log_type`.
10. **Row height (§4.10)** — the fixed-height-per-type rule, why it exists, how to set it in
    CSS, `rowHeightHint`, and the documented boundary: content-driven per-row heights are not
    supported, use a taller fixed row and clip.
11. **Constraints a plugin author must respect** — the "Consequences for plugin authors"
    list from §1.1: which bare specifiers a plugin module may import, no new Python
    dependencies, no runtime fetching, no npm, and how to request a new vendored library.
    State plainly that a plugin whose JS only exists as a bundler output cannot be accepted.
12. **Walkthrough: building `text_log` from scratch.** Every code block in this section must
    be copy-pasted from the real Phase-7 files, not paraphrased.
13. Checklist for a new plugin (entry point, id uniqueness, `detect()` strictness, fixed row
    height, `pyproject.toml` `package-data` globs, tests).

**`docs/architecture.md`** — one page: request flow for import, view, and live; the module
map from §3; where state lives on the frontend (signals in `state/*.js`, services in
`app-services.js`).

Update `AGENTS.md`. It currently says only "flask, waitress, htmx, splitjs, chartjs" — htmx
is not used anywhere and preact/htm/signals are missing. Replace with: the four hard
constraints from §1.1 stated up front, the accurate library list from `MANIFEST.json`, the
prime directive from §0.7, and "run `python -m pytest -q` before every commit".

Update `README.md`: install, `cinc serve`, `cinc log-types`, `cinc samples`,
`cinc build`, and a pointer to `docs/plugins.md`.

Also update `pyproject.toml` `[tool.setuptools.package-data]` to include `plugins/**/*.css`
(currently only `.json`, `.html`, `.js` are packaged, so plugin stylesheets would not ship).

---

### Phase 11 — Tests

Target `tests/`. Keep `conftest.py` as is.

| File | Contents |
|---|---|
| `test_contract.py` | `validate_events` accepts valid input; rejects a missing `row_id`, a missing `time`, an unparseable `time`, and duplicate `row_id`s — with the log type id in the message |
| `test_registry.py` | discovery finds both log types; duplicate id raises; unknown id returns `None`; **no aliasing** (`registry.get("core-event")` returns `None`) |
| `test_samples.py` | for **every** registered log type and **every** declared sample: the file parses, `build_page_data` succeeds, `validate_events` passes, the file is < 100 KB. Plus the core_event feature checklist from §2.2 (severities present, ≥3 matched pairs, one collapsed match, one unpaired set, one unpaired clear, ≥6 `pwr_bus` rows with numeric `data.bus.load_pct`, ≥4 systems) |
| `test_core_event.py` | port the existing `test_plugins.py` assertions to the new class and envelope: entity-field aliasing, `_search` arrays, channel catalog from `channels` and from `channelCount`, pairing durations, fault prefix `[PWR-Y-214]`, location string, `hasData` |
| `test_text_log.py` | import from JSON and from a raw `.log` upload; `normalize_events` output satisfies the contract; `search_config` and `log_summary` shapes |
| `test_page_data.py` | envelope shape for both log types: `apiVersion == 2`, required keys present, `logData` has **only** `events`, no `plugin` or top-level `logType` key, `logTypes` keyed by id, `view` holds only `scripts`/`styles`, both non-empty |
| `test_bundle.py` | **the multi-type suite.** Resolution order: bundle wins over `logType` key wins over `--log-type` wins over sniffing; unknown `logType` raises listing registered ids; no-match and ambiguous-match produce their specified messages; `detect` returns 0.0 for the other plugin's sample. `row_id` allocation: a two-section bundle yields globally unique, contiguous ids, **and every core_event `pairedChannels[].linkedRowId` / `matchSummary[].linkedRowId` still resolves to an event in the merged list** (the I17 regression). Merge order is `(time, row_id)` with the tiebreak verified on same-second rows. Building `--data a --data b` equals building the equivalent bundle. Every event carries `log_type`, and a plugin that tries to set it is rejected |
| `test_row_metrics.py` | `buildOffsets` / `indexAtOffset` — exercised via a small Node-free harness or ported to Python as a pure-function port; assert: monotonic offsets, `indexAtOffset(offsets, offsets[i]) === i`, boundary values at 0 and `offsets[n]`, single-type input reproduces `i * stride` exactly, and a 100k-row build stays under 10 ms |
| `test_store_types.py` | `log_record_types` membership: a mixed log is returned by `list_records` for **both** types and counted once under each; deleting the record cascades; `log_events.log_type` holds the per-event value, not the record's |
| `test_store.py` | keep, plus: `append_events` rejects an event with no `time`; a live record round-trips |
| `test_live.py` | `SampleReplaySource` is deterministic across two runs; `SessionManager` start→poll→stop→start again (regression for **D2**); replayed events are identical to imported events for the same sample apart from `row_id` (regression for I9) |
| `test_standalone.py` | the full self-containment matrix from Phase 9.3, run for every log type × every declared sample |
| `test_dependencies.py` | **the constraint guard.** (a) `pyproject.toml` `dependencies` equals exactly `{flask, jinja2, waitress, docopt}`; (b) no `package.json`, `node_modules`, or bundler config anywhere in the tree; (c) every file in `static/vendor/` appears in `MANIFEST.json` and vice versa, with no `version` field equal to `"unknown"`; (d) each manifest `sha256` matches the committed file; (e) the import map in `base.html` and `standalone.py`'s `BARE_MODULES`/`GLOBAL_SCRIPTS` agree with the manifest; (f) no source file under `src/cinc/` outside `static/vendor/` contains `cdn.jsdelivr`, `unpkg.com`, `cdnjs`, or `https://` in a `src`/`href` attribute |
| `test_isolation.py` | **the conformance test.** (a) no module under `src/cinc/` outside `plugins/` contains any of `core_event`, `core-event`, `set_clear`, `norm_time`, `utctime`, `subsystem`, `Flashing Red` — scan file text, including `static/` and `templates/`; (b) `plugins/text_log/**` never imports `plugins.core_event`; (c) `src/cinc/app.py` has no `cinc.plugins` import |

`test_isolation.py` is the guard rail that keeps this refactor from rotting. Write it early
(it can be added in Phase 3 with an `xfail` and flipped on in Phase 6) and never weaken it —
if a future change trips it, the change is wrong, not the test.

---

## 8. File-by-file change summary

| Path | Action |
|---|---|
| `src/cinc/log_generator.py` | **delete** |
| `src/cinc/plugin_manager.py` | **delete** |
| `src/cinc/plugins/base.py` | **delete** |
| `src/cinc/plugins/core_event/log_types.py` | **delete** (merged into `log_type.py`) |
| `src/cinc/plugins/core_event/live_monitor.py` | **delete** |
| `src/cinc/plugins/core_event/plugin.py` | **rename** → `log_type.py`, merge, rewrite class |
| `src/cinc/plugins/core_event/samples/dev-data*.json` | **delete** |
| `src/cinc/plugins/core_event/samples/sample-3ch.json` | **new** |
| `src/cinc/plugins/core_event/samples/sample-4ch.json` | **new** |
| `src/cinc/plugins/core_event/static/row.css` | **new** |
| `src/cinc/plugins/text_log/**` | **new** (whole plugin) |
| `src/cinc/logs/registry.py` | **delete** → `core/registry.py` |
| `src/cinc/core/__init__.py`, `core/contract.py`, `core/registry.py` | **new** |
| `src/cinc/core/bundle.py`, `core/assemble.py` | **new** — envelope parsing, type resolution, section merge |
| `src/cinc/samples/mixed.json` | **new** — core-level bundle sample spanning both plugins |
| `src/cinc/static/logview/row-metrics.js` | **new** — prefix-sum offsets, binary search, per-type measurement |
| `src/cinc/static/hooks/use-virtual-list.js` | optional `offsets` input; height-aware `scrollToIndex` |
| `src/cinc/static/logview/features/main/LogVirtualList.js` | offsets-based spacer; one hidden probe per present type |
| `src/cinc/static/logview/features/search/SearchResultsContent.js`, `SearchResultsPane.js` | same, for the search pane |
| `src/cinc/logs/types.py` | rewrite: `LogRecord`, `LogDocument`, `Sample`, `LogType` |
| `src/cinc/logs/store.py` | require `time`; delete `_event_time` |
| `src/cinc/logs/routes.py` | registry without aliases; `build_page_data`; attach `log`/`live` |
| `src/cinc/live/monitor.py` | **rename** → `live/session.py`; `LiveSource`, `SessionManager`, `LiveSessionRegistry` |
| `src/cinc/live/replay.py` | **new** |
| `src/cinc/live/routes.py` | per-log-type routes |
| `src/cinc/app.py` | app factory; zero plugin imports |
| `src/cinc/__main__.py` | `log-types`, `samples`, `build --log-type` |
| `src/cinc/standalone.py` | `CINC_PAGE_DATA`; auto-discovered modules |
| `src/cinc/templates/home.html` | **new** |
| `src/cinc/templates/live_view.html` | **delete** |
| `src/cinc/templates/live_sessions.html`, `log_view.html`, `logs_index.html` | de-leak |
| `src/cinc/static/runtime.js` | `Cinc`; renamed registrations; `registerDetailSummary`, `registerTimeFormatter` |
| `src/cinc/static/app.js` | `CINC_PAGE_DATA` |
| `src/cinc/static/shared.js` | add `eventTimeMs`, `formatInstant` |
| `src/cinc/static/services/app-services.js` | single identity; expose `search` |
| `src/cinc/static/services/search.js` | config-driven |
| `src/cinc/static/state/navigation.js` | `timeMs` |
| `src/cinc/static/state/activity.js` | live-safe id validation (**D1**) |
| `src/cinc/static/state/viewer-store.js` | pass `allEventsSignal` to activity state |
| `src/cinc/static/styles.css` | remove ~90 lines of core_event vocabulary |
| `src/cinc/static/logview/features/chart/LogMainChart.js` | time model; formatter hook |
| `src/cinc/static/logview/features/main/MainViewShell.js`, `LogVirtualList.js` | time model |
| `src/cinc/static/logview/features/rows/PluginLogRow.js` | single identity |
| `src/cinc/static/logview/features/detail/EventSummary.js` | pluggable |
| `src/cinc/static/logview/features/search/SearchHelpDialog.js`, `SearchPanel.js` | config-driven |
| `pyproject.toml` | deps trimmed to the C2 four; `dev` extra; entry-point group `cinc.log_types`; package `**/*.css` and `vendor/MANIFEST.json` |
| `requirements.txt` | **delete** (superseded by `pip install -e .`) |
| `src/cinc/static/vendor/htm.min.js`, `htm.mjs`, `preact.min.js`, `preact-hooks.min.js`, `preact-signals.min.js`, `preact-signals-core.min.js` | **delete** (**D13**) |
| `src/cinc/static/vendor/MANIFEST.json` | **new** |
| `tools/refresh_vendor.py` | **new** |
| `src/cinc/static/search_syntax.html`, `docs/search_syntax.rst` | **delete** (**D14**) |
| `docs/api-design.md`, `docs/preact-migration-plan.md`, `docs/transition-to-esm.md` | **delete** |
| `docs/plugins.md`, `docs/architecture.md`, `docs/dependencies.md` | **new** |
| `AGENTS.md`, `README.md`, `.gitignore` | update |
| `tests/test_plugins.py` | **replace** with the suite in Phase 11 |

---

## 9. Commit sequence

One commit per phase, in order. Suggested messages:

```
1  chore: trim dependencies to the minimum set and pin vendored CDN libraries
2  feat(core-event): replace generated samples with curated 3ch/4ch fixtures
3  refactor: collapse plugin and log-type into a single LogType interface
4  refactor: make ISO-8601 `time` the only temporal concept in core
5  refactor(live): generalize live capture behind LiveSource; add sample replay
6  refactor(ui): move core-event styling, search config, and detail summary into the plugin
7  feat(text-log): add a second reference plugin as an interface conformance test
8a feat: self-describing data — bundles, type detection, and multi-file build
8b refactor(ui): dispatch row rendering and search config per row log type
8c feat(ui): variable row heights via piecewise-uniform virtualization
9  refactor(standalone): auto-discover frontend modules and enforce self-containment
10 docs: plugin author guide and architecture overview
11 test: contract, registry, sample, bundle, isolation, and live-parity suites
```

---

## 10. Definition of done

All of the following must hold.

**The four hard constraints (§1.1) come first:**

- **C1** — for every log type × every declared sample, `cinc build` produces one HTML file
  that is fully functional from `file://` with the network disabled.
  `test_standalone.py` passes.
- **C2** — `pyproject.toml` `dependencies` is exactly `["flask", "jinja2", "waitress",
  "docopt"]`; `pytest`/`flake8`/`black` live only in the `dev` extra;
  `docs/dependencies.md` justifies each entry.
- **C3** — `find . -name package.json -o -name node_modules -o -name "vite.config.*" -o -name
  "webpack.config.*" -o -name "tsconfig.json" | grep -v .venv` is empty.
- **C4** — every file in `static/vendor/` is in `MANIFEST.json` with an exact version and a
  matching sha256; nothing outside `static/vendor/` references a CDN host.
  `test_dependencies.py` passes.

**Then the refactor goals:**

1. `python -m pytest -q` — all green, `test_isolation.py` and `test_dependencies.py`
   included and not skipped.
2. `grep -rn "core_event\|core-event\|set_clear\|norm_time\|utctime\|subsystem\|channels\|Flashing"
   src/cinc --include="*.py" --include="*.js" --include="*.css" --include="*.html"
   | grep -v "^src/cinc/plugins/"` returns **nothing**.
3. `grep -rn "EventLog2\|EVENTLOG2\|plugin_manager\|LogTypeDefinition\|EventLogSourcePlugin\|registerPlugin"
   src tests docs` returns **nothing**.
4. `src/cinc/plugins/` contains exactly two plugins; each ships at least one committed sample
   under 100 KB; there is no random or synthetic event generation anywhere in the tree.
5. Both log types work end-to-end: demo route, import (file and JSON body), view, search,
   bookmark, comment, chart, delete, live replay, standalone build.
6. **No command names a log type.** Every sample, and `mixed.json`, builds and imports with
   no `--log-type` flag. `grep -rn '\-\-plugin' src docs README.md` returns nothing.
7. **Mixed logs work.** `mixed.json` renders one time-ordered list with both row components
   at their own heights; the log appears under both types in `/logs`; search, bookmarks,
   cross-links, and chart jumps all behave.
8. `docs/plugins.md` is sufficient to build a third plugin without reading core source, and
   documents both the self-describing-data contract and the fixed-height-per-type rule.

---

## 11. Manual smoke test

Run after every phase.

```bash
pip install -e .
rm -rf ./cinc-data
cinc log-types                      # lists core_event (and text_log from Phase 7)
cinc samples                        # lists each log type's committed samples
cinc serve --port 8080
```

In the browser:

1. `/` lists each log type and its samples. Open one → viewer renders, rows visible.
2. `/logs` shows each log type with a count of 0.
3. Import a sample file → redirects to the viewer.
4. Click a row → detail panel populates; the data tree expands.
5. Bookmark a row → it appears under the Bookmarks tab and survives a reload.
6. Add a comment and a reply → both persist across a reload.
7. Search `level:ERROR` (text_log) / `color:Red` (core_event) → results filter; open the help
   dialog and confirm the examples match the *current* log type.
8. Promote a search to a filter → the main list narrows.
9. Click a chart marker → the list scrolls to that row and flashes.
10. Scroll the list → the chart cursor tracks.
11. Switch chart type and timeline view → both redraw.
12. `/live` → start a capture → rows stream in; "lock to bottom" follows them; bookmark a
    streamed row; stop → the record shows as completed and reopens intact.
13. **The C1 check — do this every phase, not just at the end.**
    ```bash
    cinc build --log-type core_event \
      --data src/cinc/plugins/core_event/samples/sample-4ch.json --output /tmp/out.html
    ```
    Then: turn the machine's Wi-Fi / network off, open `file:///tmp/out.html`, and confirm
    rows render, virtual scrolling is smooth, search returns results, the chart draws and is
    interactive, clicking a row fills the detail panel, the data tree expands, and
    bookmarks/comments are disabled. The console must be clean — in particular, no
    `net::ERR_INTERNET_DISCONNECTED` and no CORS or module-resolution errors.
    Repeat for `sample-3ch.json` and for `text_log` once Phase 7 lands.
14. Confirm the file is genuinely portable: copy it to a different directory (or a USB
    stick), open it there, and confirm it still works. A file that only works from its build
    directory is relying on a sibling asset and fails C1.
15. **The mixed-log check (from Phase 8 on).**
    ```bash
    cinc build --data src/cinc/samples/mixed.json --output /tmp/mixed.html
    cinc build --data src/cinc/plugins/core_event/samples/sample-4ch.json \
               --data src/cinc/plugins/text_log/samples/sample.json --output /tmp/two.html
    ```
    Neither command names a type. In `/tmp/mixed.html`: rows of both types interleave by
    timestamp, the two row styles are obviously different heights, scrolling is smooth with
    no jitter and a stable scrollbar, and jumping to a row (chart click, search result,
    core_event cross-link) centres the correct row. Scroll to the very bottom and confirm
    the last row is fully visible and there is no trailing gap — that is the classic
    prefix-sum off-by-one.
