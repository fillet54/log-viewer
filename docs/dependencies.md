# Dependencies

Runtime dependencies are Flask (HTTP application), Jinja2 (templates), Waitress (production WSGI serving), and docopt (CLI parsing). The standard library handles storage, parsing, discovery, and bundling. Development-only tools are pytest, flake8, and black.

JavaScript has no npm or bundler build. Browser libraries are pinned CDN distributions committed under `src/cinc/static/vendor/`; changes must update the manifest and its SHA-256 values. Standalone output must be opened from `file://` with networking disabled after every vendor change.

Leaflet (`static/vendor/leaflet/`, BSD-2-Clause, see its `LICENSE`) draws the Map chart. It is loaded
only when the Map chart is first opened, and embedded in a standalone page only when the log has
tracks (about 160 KB). Its default marker and layer-control images are not vendored; the chart
uses neither.

The Map chart's basemap is Natural Earth 5.1.2 (public domain), stored in `src/cinc/data/basemap/`
as gzipped, quantised layers (about 11 MB). `scripts/build_basemap.py` rebuilds them from the
Natural Earth GeoJSON files (github.com/nvkelso/natural-earth-vector, tag v5.1.2, `geojson/`).
