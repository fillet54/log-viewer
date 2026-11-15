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
  data in `src/cinc/data/basemap/` (rebuilt with `scripts/build_basemap.py`).
  New asset types need a glob in `pyproject.toml` `package-data` or they won't
  be installed.
- **Keep Python dependencies minimal** — they must be installable from an
  offline mirror. Ask before adding one.

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

Run `python -m pytest -q`. `tests/test_search.py` needs Node (skipped without it).
