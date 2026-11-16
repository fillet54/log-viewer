# Cinc — HTML Log Viewer

## Offline first

Cinc runs in an offline environment: no internet, no CDNs, no package
registries at runtime. Everything the app needs ships inside the Python
package.

- **Vendor every browser dependency.** Third-party JS/CSS goes in
  `src/cinc/static/vendor/` and is recorded in `vendor/MANIFEST.json` with its
  version and sha256. Never reference a CDN URL or a remote font, image, tile
  server or API.
- **Wire a new vendored library in both pages.** The served app loads it in
  `templates/base.html` (import map or `<script>`); the standalone single-file
  build inlines it via `standalone.py` (`GLOBAL_SCRIPTS` / `BARE_MODULES`).
- **No frontend build step.** Plain ES modules, with htm instead of JSX.
- **Ship data with the package.** Fonts live in `static/fonts/`, Map basemap
  data in `src/cinc/data/basemap/` (rebuilt with `scripts/build_basemap.py`;
  aviation layers with `scripts/build_aviation.py`).
  New asset types need a glob in `pyproject.toml` `package-data` or they won't
  be installed (`tests/test_packaging.py` fails until they have one).
- **Keep Python dependencies minimal** — they must be installable from an
  offline mirror. Ask before adding one.

## Rules that tests do not fully catch

- **Data goes into a page only through `standalone.script_json`**, which
  escapes `<`, `>` and `&`. Vendored code inlined as code goes through
  `inline_script_code`. Never put `json.dumps` output in a `<script>` directly.
- **Times are parsed only with `core.times.parse_time`.** It accepts `Z` and
  any number of fraction digits on Python 3.10, which bare `fromisoformat`
  does not. Order events by the parsed instant, never by the time string.
- **Store `row_key`, not `row_id`,** for anything kept across page loads.
- **Lower bounds in `pyproject.toml` match `uv.lock`**; update both together.

## Libraries in use

Python: flask, waitress, docopt (jinja2 via flask).

Browser (vendored; versions in `MANIFEST.json`):

* preact + hooks — UI components
* @preact/signals — reactive state
* htm — JSX-like templates without a build step
* split.js — resizable panes
* chart.js — charts
* leaflet — Map chart

## Tests

Run `python -m pytest -q`. The browser-module tests (`test_search.py`,
`test_activity.py`, `test_viewer_store.py`, via `tests/node_harness.py`) need
Node and are skipped without it. `python tests/bench_live.py` times live posts
against large logs.
