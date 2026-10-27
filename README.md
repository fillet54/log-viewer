# Cinc

A log viewer. Two modes:

- **Standalone**: `cinc build PATH... -o out.html` writes one self-contained HTML page.
- **Live**: `cinc serve --data DIR` shows rows as they are posted, and saves every log to `DIR`.

`cinc types` lists the registered log types (`core_event`, `text_log`).

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

## Live API

| Endpoint | Body | Effect |
|---|---|---|
| `POST /api/logs` | `{"name": "..."}` (optional) | Starts a new log and makes it the live one. |
| `POST /api/events` | `{"type": "text_log", "events": [{...}]}` | Appends rows to the live log. If no log is live yet, one is started. `"event": {...}` sends a single row. |
| | `{"type": "core_event", "header": {...}}` | Sets that type's header. It can be sent together with `events`. |
| `GET /api/stream?log=ID&after=SEQ` | | Server-sent events that the viewer uses. |

Pages: `/` shows the live log, `/logs` lists history, and `/logs/<id>` views one log
(`/logs/<id>/standalone.html` downloads it).

## Fake data

`tests/fake_logs.py` has small deterministic generators, used by the tests and for demos:

```
python tests/fake_logs.py write ./cinc-data 40         # write a log folder
python tests/fake_logs.py post http://127.0.0.1:8080   # stream rows into `cinc serve`
```
