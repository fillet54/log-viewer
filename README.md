# Cinc

A log viewer. Two modes:

- **Standalone**: `cinc build PATH... -o out.html` writes one self-contained HTML page.
- **Live**: `cinc serve --data DIR` shows rows as they are posted, and saves every log to `DIR`.
  It runs on waitress; each open live viewer holds one server thread (`--threads`, default 16).

`cinc types` lists the registered log types (`core_event`, `text_log`, and the built-in `track`), and
`cinc plugins` lists the installed plugins and what each adds.

**Extending Cinc**: a project adds log types, charts, pages, commands and data helpers with a plugin
package. See [docs/plugins.md](docs/plugins.md) and the example in `examples/acme_plugin`.

## Log folder format

Each live log is saved as a plain folder. Any tool can write one by hand, and it will show up in the
live server's history. You can also pass it to `cinc build`.

```
<data>/<log-id>/
  log.json        {"name": "Run 42", "started": "2026-10-26T14:30:12+00:00"}   optional
  <type>.jsonl    raw rows for log type <type>, one JSON object per line
  <type>.json     header for that type, e.g. {"channels": ["A", "B"]}           optional
```

Each type receives the payload `{**header, "events": rows}`. The `<log-id>` can be any folder name
made of letters, digits, `.`, `_` and `-`.

`cinc build` accepts a log folder, a single `<type>.jsonl` file, or a JSON payload or bundle file.

## Tracks and the Map chart

The **Map** chart (Leaflet on a Natural Earth basemap, vendored and fully offline) shows position
tracks and follows the log: scrolling or selecting a row moves the map's clock, and scrubbing,
playing, or clicking a track jumps the log to that time. Clicking a track's marker or legend row
focuses it (and keeps it in view while playing). It appears in the chart picker when the log has
tracks. Toggles in the map's corner show or hide country/state borders and names, places, and roads.

Supply tracks as rows of the built-in `track` type. It adds nothing to the row list:

```
<data>/<log-id>/track.jsonl   {"time": "2026-01-01T08:00:05Z", "track": "UAV-1", "lat": 34.1, "lon": -117.2, "alt": 1200}
<data>/<log-id>/track.json    {"tracks": {"UAV-1": {"label": "Survey drone", "color": "#d1495b"}}}   optional
```

`alt` is metres above the WGS84 ellipsoid (default 0) and `track` defaults to `"track"`. Between
samples a track moves at constant heading and speed (a rhumb line), so one sample per heading or
speed change is enough. Live: `POST /api/events {"type": "track", "events": [...]}`. A plugin can
also supply tracks from its own rows by overriding `LogType.tracks(payload)` (see
`cinc/core/track.py`).

The basemap is a low-detail world plus 1:10m layers (coast, land, lakes, rivers, urban areas,
glaciers, roads, borders, place names) cut to the tracks' area with a margin. A standalone page
embeds just that area (a few hundred KB); the live server cuts it on request
(`GET /api/basemap?bbox=w,s,e,n`) and widens it as live tracks grow. The data is built from
Natural Earth by `scripts/build_basemap.py` into `src/cinc/data/basemap/`.

## Live API

| Endpoint | Body | Effect |
|---|---|---|
| `POST /api/logs` | `{"name": "..."}` (optional) | Starts a new log and makes it the live one. |
| `POST /api/events` | `{"type": "text_log", "events": [{...}]}` | Appends rows to the live log. If no log is live yet, one is started. `"event": {...}` sends a single row. |
| | `{"type": "core_event", "header": {...}}` | Sets that type's header. It can be sent together with `events`. |
| `GET /api/stream?log=ID&after=SEQ` | | Server-sent events that the viewer uses. |

Every `POST /api/*` must send `Content-Type: application/json` (anything else is a 415); `LiveClient`
does. There is no authentication: anyone who can reach the server can read and post logs, so
`cinc serve` binds to 127.0.0.1 by default and warns when `--host` is not a loopback address.

Pages: `/` shows the live log, `/logs` lists history, and `/logs/<id>` views one log
(`/logs/<id>/standalone.html` downloads it).

## Fake data

`tests/fake_logs.py` has small deterministic generators, used by the tests and for demos:

```
python tests/fake_logs.py write ./cinc-data 40         # write a log folder
python tests/fake_logs.py post http://127.0.0.1:8080   # stream rows into `cinc serve`
```
