# Audit fix plan

Fixes for the findings of the 2026-11-15 audit, in the order they land. Each numbered item is
one commit with its test.

Baseline: 98 tests pass, flake8 clean. None of the findings is covered by a test today, so every
item adds one.

## Context and decisions

Settled on 2026-11-15; the plan below follows them.

| Topic | Decision |
|---|---|
| Deployment | One person on localhost, served at the root. No URL-prefix work. |
| Server | `cinc serve` runs on waitress; Flask's server only with `--debug`. |
| POST guard | `POST /api/*` requires `Content-Type: application/json`. No Origin check. |
| Row identity | Events get a stable `row_key`; bookmarks and comments key by it. `API_VERSION` → 4. |
| Old bookmarks/comments | The legacy global localStorage keys are deleted on first load. |
| Notes storage | Stays in localStorage. Server-side storage is not part of this plan. |
| Python floor | 3.10 keeps working; a tolerant time parser replaces bare `fromisoformat`. |
| CLI parser | docopt stays. |
| Live scale | Tens of thousands of rows, posted one at a time, always in time order. All of phase 4 is needed. |
| Plugin API | `rows_independent` and `live_stream` are both public (API v4). |
| Augmenters | Per-row by default; a whole-log augmenter is opt-in and puts its type on the full re-derive path. |
| Browser live path | Measure with 50k rows first; fix only what the profile shows. |
| Plugin load failure | Warn and carry on. |
| Order | Phases 1 → 5 as written. |

## Phase 1 — broken now

### 1.1 Ship the `.mjs` vendor files

- `pyproject.toml`: add `"static/**/*.mjs"` to `[tool.setuptools.package-data]`.
- New `tests/test_packaging.py`: walk `src/cinc` and assert every file that is not `.py`/`.pyc`
  matches one of the `package-data` globs in `pyproject.toml`. Read it with `tomllib`, falling
  back to `tomli` if present; skip when neither exists (Python 3.10 without `tomli`).
- Manual check once: build a wheel, install it into a scratch venv, run `cinc build` on a fake
  log, open the page.

### 1.2 Adding a comment throws

- `static/state/activity.js:117`: `validIds.has(key)` → `validIds().has(key)`.
- `validIds()` builds a `Set` of every row id on each call, once per bookmark and comment. Make
  it a `computed` over `allEventsSignal`.
- Test: with 1.3 (same harness).

### 1.3 Bookmarks and comments per log

- `services/app-services.js` passes `pageData.server?.logId` into `createViewerStore`, which
  passes it to `createActivityState`.
- Storage keys become `loglayout.bookmarks.<logId>` and `loglayout.comments.<logId>`. No log id
  → activity disabled, as on standalone pages.
- Write a key only when it has content and remove it when it becomes empty.
- On load, remove the legacy `loglayout.bookmarks` and `loglayout.comments` keys.
- New `tests/test_activity.py`, following `tests/test_search.py` (Node, skipped without it): a
  runner with a `localStorage` stub and a resolve hook mapping the import-map names
  (`preact/signals`, …) to `static/vendor`. Cases: add a comment and read it back; a bookmark in
  log A is absent in log B; opening B leaves A's stored bookmarks intact; legacy keys are gone.

## Phase 2 — data correctness

### 2.1 Escape data embedded in `<script>`

- `standalone.py`: one helper, `script_json(value) -> str`: `json.dumps` (ASCII, compact), then
  `<` → `<`, `>` → `>`, `&` → `&`. Use it for `CINC_PAGE_DATA` (line 100),
  `CINC_MAP` (line 121) and the `esm-data` block (line 170).
- Vendored JS inlined as code (`GLOBAL_SCRIPTS`, the Leaflet text block at line 119, and the
  `replace` filter in `templates/standalone.html`) cannot be escaped that way. Make the
  `</script` replacement case-insensitive, and add a test asserting no vendored file contains
  `<!--` or a `</script` the replacement would miss.
- Tests in `tests/test_standalone.py`: a standalone page and a served view whose rows contain
  `</SCRIPT><img src=x onerror=1>` and `<!--<script>`. Parse with `html.parser`; the number of
  `<script>` elements is unchanged, no `<img>` appears, and the data round-trips.

### 2.2 One time parser, and order by instant

- `core/times.py`: `parse_time(value) -> datetime`. Accepts `Z`, any number of fraction digits
  (padded or cut to six before `fromisoformat`, so Python 3.10 behaves like 3.11+), and raises
  `ValueError` otherwise. A `require_zone` flag covers the contract's "must include a timezone".
- Use it in `core/contract.py` (`validate_events`), `core/track.py` (`time_ms`),
  `plugins/core_event/plugin.py` (`_parse_datetime`), `app.py` (`when`) and `logdir.py`.
- `core/assemble.py:86`: sort on `(parsed instant, row_id)`. `normalize_section` keeps the
  instants `validate_events` already parsed, so nothing is parsed twice. The `time` string a
  type produced is not rewritten.
- `logdir.py` `list_logs`: sort on the parsed `started`.
- Tests in `tests/test_contract.py`: `…05.500Z` after `…05Z`; `01:00:04+01:00` before
  `00:00:05Z`; `…05.5Z` and `…05.1234567Z` parse.

### 2.3 Stable `row_key` (API v4)

Row ids are `slot + 1 + index * step`, where `step` is the number of types in the log. When a
live log gains a type every row id changes, so anything stored by row id points elsewhere.

- `core/assemble.py` `normalize_section`: each event gets `row_key = "<type id>:<index>"`, the
  event's position in its type's output. Like `log_type`, a type or augmenter may not set or
  change it.
- `core/contract.py`: `row_key` is required and unique.
- `static/state/activity.js`: bookmarks and comments store and look up `row_key`. Callers
  (`DetailPanel.js`, the bookmark views under `features/search/`) pass the event rather than
  `event.row_id`.
- `API_VERSION` → 4 in `core/assemble.py` and `static/runtime.js`; `docs/plugins.md` lists
  `row_key` and says to store it, not `row_id`, for anything kept across page loads.
- Tests: `row_key` survives adding a second type to a log (`tests/test_live.py`); a bookmark
  set before the type was added is on the same row after (`tests/test_activity.py`).

### 2.4 Live order matches reload order

- `static/state/viewer-store.js` `applyLiveEvents`: append when the new event is not earlier
  than the last one; otherwise insert by `(Date.parse(time), row_id)` with a binary search.
  Senders post in time order, so the insert path is for the case of several types interleaving.
- Check the timeline helpers in `LogMainChart.js` that take the last element as the latest.
- Test: apply an out-of-order batch in the Node harness and compare with the order
  `assemble_page_data` gives for the same rows.

## Phase 3 — server robustness

### 3.1 Validate a header before saving it

- `live.py` `post`: when a header is supplied, run it through the type before `set_header` —
  `_normalize` with the existing rows (or none) plus `type_config(log_type, payload)`. A
  `ValueError` rejects the post with 400 and nothing is written.
- Test: header-only post of `{"severities": "nope"}` → 400; a following valid row post is 202.

### 3.2 One bad folder must not break the rest

- `logdir.py`: `meta()` catches `ValueError`/`OSError` from `log.json` and returns the fallback
  meta (folder name, mtime) with `"error": "<reason>"`.
- `list_logs` reads each log's meta once and returns it with the log, so `history` does not read
  it again.
- `templates/history.html`: an "unreadable log.json" chip when `meta.error` is set.
- Test: with a corrupt `log.json` in one folder, `/logs` and another log's page return 200.

### 3.3 Clean errors instead of 500s

- `app.py` `download_log`: the same `ValueError` handling as `view_log` (400 with the message),
  and 404 for a log with no types. One shared helper.
- `app.py:195`: `request.args.get("after", 0, type=int)`.
- Tests: unknown `<type>.jsonl` → download is 400; `?after=x` → 200.

### 3.4 Require JSON on POST

- `app.py`: a `before_request` returning 415 for `POST /api/*` unless the content type is
  `application/json`. `LiveClient` and `tests/fake_logs.py` already send it.
- `__main__.py`: a one-line warning when `--host` is not a loopback address (no authentication).
- README "Live API": state the content-type requirement and that there is no authentication.
- Test: a `text/plain` post to `/api/logs` → 415 and no log is created.

### 3.5 Serve with waitress

- `__main__.py`: `waitress.serve(app, host=…, port=…, threads=N)`; `app.run` only for `--debug`.
  New option `--threads` (default 16). Each open live tab holds one thread for its event stream.
- Verify by hand: events reach the browser as they are posted (no buffering), and Ctrl-C exits
  promptly with a viewer connected. Shorten the 20 s ping in `api_stream` if shutdown waits on it.
- README: mention `--threads`. `docs/dependencies.md` already describes waitress correctly.

## Phase 4 — live performance

Each post re-normalizes every row of the type and deep-compares old and new events, under the
hub lock: about 150 ms per post at 3,000 `core_event` rows, growing with the log. The target is
a post cost that does not depend on log size, at 50,000 rows.

### 4.1 Measure, and remove the avoidable work

- `tests/bench_live.py` (not collected by pytest): N single-row posts per type, printing time
  per post at 1k/5k/20k/50k rows. Put before/after numbers in each commit message of this phase.
- `live.py`: keep each type's header and the log's type list in memory instead of reading
  `<type>.json` and globbing `*.jsonl` on every post; invalidate on header change and `_start`.
- `page_data` for the current log builds from the in-memory sections instead of re-reading and
  re-normalizing from disk.

### 4.2 Per-row augmenters

- Contract (API v4): an augmenter computes each event from that event and the header only. In
  live mode it receives just the new and changed events.
- `ctx.extend(..., whole_log_augmenters={name: fn})` registers one that needs every event. A
  type with any whole-log augmenter always takes the full re-derive path (today's behaviour).
- `LogType.register_event_augmenter(name, fn, whole_log=False)` carries the flag.
- `docs/plugins.md`: the rule, the opt-in, and its cost. `examples/acme_plugin`'s `owner`
  augmenter is already per-row.
- Tests in `tests/test_extensions.py`: a per-row augmenter sees only the batch in live mode; a
  whole-log one sees everything and the result equals a full page load.

### 4.3 `rows_independent` types

- `LogType.rows_independent: bool = False` (public). When true, the hub normalizes only the new
  batch, passing `row_id_base = slot + stored_count * step` and the stored count as the
  `row_key` offset, and sends no `updates`.
- Set it on `TextLogType` and `TrackLogType`.
- Test: batch-by-batch output equals `assemble_page_data` for the whole log.

### 4.4 `live_stream`, and incremental `core_event`

- Public hook: `LogType.live_stream(header, *, row_id_base, row_id_step) -> stream | None`
  (default `None`). `stream.append(rows) -> (new_events, updated_events)`; it raises
  `cinc.NeedsRebuild` when a batch cannot be applied incrementally.
- Hub: on the first post after a start, a reload or a slot change, create the stream and replay
  the stored rows through `append`. On `NeedsRebuild`, run the full re-derive, log a warning
  naming the reason, and recreate the stream. `updates` come from the stream, so the
  deep comparison of all events goes away.
- `core_event` stream state: channel catalog, `fallback_start`, row count, and the `open_sets`
  queues from `_derive_core_events` (as `deque`s; also replace `queue.pop(0)` in the batch
  path). An appended CLEAR pops its SET; both are returned, the SET as an update, with
  `matchSummary` and `rowDisplay` rebuilt for those two only.
- `NeedsRebuild` cases: a row with `norm_time` earlier than the last one processed; a new
  channel on a log whose header declares none; a header change. Rows arrive in time order, so
  the first should not happen in practice.
- Public test helper `cinc.testing.assert_stream_matches(log_type, payload, batch_sizes)`:
  feeding the rows in batches gives exactly the events of a full normalization. Used for
  `core_event` with the fake-log generators and a randomized SET/CLEAR sequence, batch sizes
  1, 7 and all-at-once, plus a case that triggers each `NeedsRebuild` reason.
- `docs/plugins.md`: `rows_independent`, `live_stream`, `NeedsRebuild` and the test helper,
  under API v4.

### 4.5 Browser, measured

- Stream 50,000 rows one per post into an open viewer (`tests/fake_logs.py post`) and profile:
  time per message in `applyLiveEvents`, filter re-evaluation, and chart updates.
- Fix only what shows up. The expected fix is queuing stream messages in `app.js` and applying
  them once per animation frame, so the event array is copied once per frame, not per message.
- Record the numbers in the commit; if nothing is slow, close the item with the measurement.

## Phase 5 — housekeeping

- **`uv.lock`**: commit it.
- **Dependency bounds**: lower bounds matching `uv.lock` for flask, jinja2, waitress and docopt.
- **`API_VERSION` check**: `runtime.js` `setupPlugins` compares `pageData.apiVersion` with its
  own and logs one clear console error on a mismatch.
- **Plugin load failures**: `Registry.discover` catches an exception from `point.load()` or
  `setup`, records `(plugin id, error)` in `registry.failed`, prints a warning to stderr and
  carries on. `cinc plugins` lists the failures. Test: a plugin whose `setup` raises does not
  stop `cinc types`.
- **Channel case**: `_validate_raw_payload` in `plugins/core_event/log_type.py` normalizes both
  sides with `_normalize_channel_name` before comparing.
- **AGENTS.md**: data goes into a page only through `script_json`; times are parsed only with
  `parse_time`; a new asset extension needs a `package-data` glob (now tested).

## Not in this plan

- Saving bookmarks and comments in the log folder, or carrying them into standalone pages.
- Serving under a URL prefix.
- Replacing docopt.
- `LogMainChart.js`, `MapChart.js` and the acme example were not reviewed line by line in the
  audit; only 2.4's check of the timeline helpers touches them.
