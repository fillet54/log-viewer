# Eventlog2 MVP API Design

## Summary

Eventlog2 is a local HTML log viewer for core-event logs. The MVP is optimized for a working product rather than a broad plugin framework.

The durable abstraction is a **log type**. A log type owns import parsing, event normalization, row rendering, and chart registration. The current product ships one log type: `core_event`.

Storage is SQLite-backed and local to `CINC_DATA_DIR/eventlog2.sqlite`. Imported logs and live captures use the same tables and viewer route.

## Concepts

### Log Type

A log type describes one family of logs. It provides:

- an ID and display name
- import parsing
- event normalization
- viewer page-data construction
- optional custom import template
- frontend row and chart registrations

The public core-event log type ID is `core_event`. Older internal plugin IDs remain as compatibility shims while the app migrates away from plugin language.

### Log Record

A log record is one imported or captured log. It has:

- `id`
- `log_type`
- `name`
- `source`: `import`, `live`, or another simple source label
- `status`: `active`, `completed`, or `error`
- import/start/end timestamps
- JSON metadata such as channels and event count

### Log Event

Events are JSON maps. Core-event still preserves its existing fields, but every normalized event must include:

- `time`: canonical timestamp for storage/search

Compatibility fields such as `utctime`, `norm_time`, and `row_id` remain supported because the current viewer uses them.

### Live Capture

There is only one live capture at a time. Live capture creates a normal `core_event` log record with `source = "live"` and `status = "active"`. Stopping live marks that record `completed`.

Completed live captures appear in the normal logs list and can be found by searching for `live`.

## Backend APIs

### LogTypeDefinition

The MVP log type API is:

```python
class LogTypeDefinition:
    id: str
    name: str
    description: str

    def parse_import(self, *, file=None, json_data=None) -> tuple[str, dict]: ...
    def extract_metadata(self, payload: dict) -> dict: ...
    def normalize_events(self, payload: dict) -> list[dict]: ...
    def build_payload_from_events(self, record, events: list[dict]) -> dict: ...
    def build_view_page_data(self, record, payload: dict) -> dict: ...
    def get_import_template(self) -> str | None: ...
    def get_supported_charts(self) -> dict[str, list[str]]: ...
```

Only `parse_import` is required. Other methods have defaults or are overridden by `core_event`.

### LogStore

`LogStore` writes to SQLite and exposes:

- `create_log(...) -> LogRecord`
- `append_events(log_id, events, source="import", tags=())`
- `complete_log(log_id, status="completed")`
- `get_record(log_id)` or `get_record(log_type, log_id)`
- `get_events(log_id)`
- `list_records(log_type_id=None, q="", source=None, status=None, page=1, per_page=20)`
- `delete(log_id)`

SQLite tables:

- `log_records`
- `log_events`

The MVP intentionally does not include a dataset abstraction or full indexed search API.

## Frontend APIs

Log-type frontend extension points are:

```js
EventLog2.registerLogRowComponent(logTypeId, Component)
EventLog2.registerLogChartType(logTypeId, definition)
EventLog2.registerLogTimelineView(logTypeId, definition)
```

The old `registerPlugin...` names are compatibility aliases.

Page data includes:

```json
{
  "apiVersion": 1,
  "logType": { "id": "core_event", "name": "Core Event" },
  "logData": { "events": [] },
  "view": {
    "scripts": [],
    "styles": [],
    "rowSettings": {},
    "charts": ["systems"],
    "timelineViews": ["severity", "bus-load"]
  }
}
```

Search inside a viewed log remains the existing client-side JSON-map query language.

## Implementation Notes

- The current core-event parser normalizes raw events and adds canonical `time`.
- Live capture uses the same viewer route as imported logs.
- SQLite is the only durable storage path for new logs.
- Automatic migration from the previous JSON-directory storage is out of scope for the MVP.
