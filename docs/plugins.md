# Extending Cinc with plugins

A project extends Cinc by shipping a small Python package (a **plugin**) next to it. `pip install` both,
and `cinc serve`, `cinc build` and the Python API pick the plugin up. The built-in `core_event` and
`text_log` types are plugins themselves (`src/cinc/plugins/`), so they show the API at work.

The common case is: logs use `core_event`, and the project adds its own charts, pages and tooling for
producing the data. `examples/acme_plugin` does exactly that and is the best place to start:

```bash
pip install -e examples/acme_plugin
cinc plugins             # what each installed plugin adds
cinc acme-demo out/      # a plugin command: writes a demo log folder and a standalone page
```

Page data and the browser API are versioned together as `API_VERSION` (currently 3):
`cinc.API_VERSION` in Python, `API_VERSION` from the `cinc` browser module.

## 1. Register the plugin

```toml
# the plugin's pyproject.toml
[project]
dependencies = ["cinc"]

[project.entry-points."cinc.plugins"]
acme = "acme_cinc:setup"

[tool.setuptools.package-data]
acme_cinc = ["static/*", "templates/acme/*"]     # browser files must be package data
```

```python
# acme_cinc/__init__.py
def setup(ctx):                    # ctx: cinc.PluginContext
    ctx.extend("core_event", scripts=["static/owners.js"], augmenters={"owner": add_owner})
```

Asset paths are relative to the package that defines `setup`, and are checked when the plugin loads.
To load plugins explicitly (tests, embedding), use `Registry.from_plugins({"acme": setup, ...})`
and pass it as `create_app(registry=...)`, `build_standalone(..., registry=...)`.

## 2. What `ctx` can add

| Call | Adds |
|---|---|
| `ctx.add_log_type(MyType)` | A log type (class or instance). |
| `ctx.extend(type_id, scripts=, styles=, config=, augmenters=)` | To an existing type, usually another plugin's: browser scripts set up for that type with `config`, stylesheets, and augmenters `fn(events, context) -> events` that add fields to its rows. |
| `ctx.add_script(path)` / `ctx.add_style(path)` | A browser script or stylesheet on every page, for charts not tied to a log type. |
| `ctx.add_blueprint(blueprint, **options)` | A Flask blueprint: pages and API routes in `cinc serve`. Templates can `{% extends "base.html" %}` for Cinc's frame; `blueprint.nav_label = "Acme"` (with an `index` view) adds it to the top bar. The app's data folder is `current_app.extensions["cinc"]["root"]`. |
| `ctx.add_command(name, fn, help)` | `cinc NAME ARGS...` calls `fn(args)`; its return value is the exit code. |

Extending a type that no installed plugin provides is an error at startup, not a silent no-op.

## 3. Log types (Python)

Subclass `cinc.LogType`. A type turns a payload, `{**header, "events": rows}`, into normalized
events: each needs `time` (ISO 8601 with a zone) and gets `row_id` (from the base class) and
`log_type` (added by Cinc).

| Member | Purpose |
|---|---|
| `id`, `name`, `description` | Identity; `id` is the `<type>` in `<type>.jsonl`. |
| `normalize_events(payload, row_id_base=, row_id_step=)` | Raw rows to events. |
| `detect(payload) -> 0..1` | Lets `cinc build` recognise an undeclared JSON file. |
| `view_config(payload) -> dict` | Settings for the type's browser scripts (`ctx.config`). |
| `search_config(payload)` | `fields` and `excludeFromBareTerms` for search. |
| `log_summary(payload) -> dict` | Key/value facts shown with the log. |
| `tracks(payload)` | Position tracks for the Map chart (see `cinc.core.track`). |
| `script_paths`, `style_paths`, `row_height` | Browser files (relative to the class's package) and row height. |

Script and style paths add up along the class hierarchy, each resolved against the package of the
class that lists it. So a project can reuse a type under its own id:

```python
from cinc.plugins.core_event import CoreEventLogType

class PumpEvents(CoreEventLogType):
    id = "pump_event"
    name = "Pump Events"
    defaults = {"severities": [{"name": "OK", "tone": "green"}, {"name": "Alarm", "tone": "red"}]}
```

`pump_event` logs then get core_event's rows, severity view and status board, with its own
severities. A type with no scripts still shows: rows fall back to a generic row (time, type, then the
`message`/`name`/`text` field or the row's JSON).

### core_event settings

Set in a log's `core_event.json` header (or a subclass's `defaults`); see
`cinc/plugins/core_event/config.py`:

```json
{"channels": ["A", "B"],
 "severities": [{"name": "Nominal", "tone": "green"}, {"name": "Warning", "tone": "red"}],
 "series": [{"id": "bus-load", "label": "Bus Load", "field": "data.bus.load_pct", "unit": "%",
             "where": {"system": "Power"}, "min": 0, "max": 100}]}
```

Tones are the viewer's severity colours: `green`, `yellow`, `red`, `flashing-red`. Each event gets
`tone` and `severity_rank`. Each `series` entry becomes a line view on the timeline.

## 4. Browser scripts

A script is an ES module whose default export is `setup(ctx)`. The viewer calls it once for each log
type that uses it, with that type's id and config, so nothing in the script names a type:

```js
import { html, signals, themeColor, TONE_COLORS } from "cinc";
import { renderBars } from "./bars.js";       // other files of the same package

export default function setup({ logTypeId, config, registerRow, registerDetail, registerChart, registerTimelineView }) {
  registerChart({
    id: "acme-owners",
    label: "Faults by Owner",
    renderPanel(panel, context) {             // context: logData, viewerStore, ...
      return { activate() {}, resize() {}, destroy() {} };
    },
  });
}
```

| `ctx` member | |
|---|---|
| `logTypeId`, `config` | The type being set up and its `view_config` (or, for an extension, the `config` given to `ctx.extend`). Both are null/empty for `add_script` scripts. |
| `registerRow(Component)` | The type's row component (Preact). |
| `registerDetail(Component)` | The summary at the top of the info pane. |
| `registerChart(definition)` | A chart: `{id, label, slots?, renderPanel(panel, context), buildCommands?(container, context), available?(services)}`. Global for `add_script` scripts. See *Where charts go* below. |
| `registerTimelineView(definition)` | A timeline view: `{id, label, kind: "histogram" | "line", stacked?, datasets: [{label, color, filter(event), value?(event)}], configureChart?(chart)}`. A line with `value` plots each event's value; otherwise it counts events per time bucket. |

The `cinc` module also exports `html` (htm bound to Preact), `hooks`, `signals`,
`themeColor("var(--token)")`, `onThemeChange(fn)` and `TONE_COLORS`. `context.viewerStore` carries
the shared state charts sync on: `selectedEvent`, `logScroll` and `setLogJump({timeMs | rowId})`.

### Where charts go

A chart can live in two **slots**: `"main"`, the wide chart area above the log, and `"side"`, the
right pane under the selected row's details (each side chart is a collapsible section there, with
its `buildCommands` in the section header). `slots` lists the ones a chart supports, the first
being where it starts; the default is `["main"]`. Users move a chart between its slots (a button in
the chart toolbar, or in the side section's header), and the choice is remembered per browser.

```js
registerChart({
  id: "systems",
  label: "Subsystem Status",
  slots: ["side", "main"],          // a list that grows with the systems: start in the side panel
  renderPanel(panel, context) {
    const vertical = context.slot === "side";   // lay out tall and narrow, or wide and short
    ...
    return { activate() {}, deactivate() {}, resize() {}, destroy() {} };
  },
});
```

The same controller contract applies in both slots; `resize()` is called when the section or pane
changes size. Timeline views always stay in the chart area.

Served pages load scripts from `/plugin-assets/<package>/<path>` (imported as
`cinc-plugins/<package>/<path>`); standalone pages inline them and every file they import. Use static
`import` statements for a plugin's own files so the standalone builder can find them.

## 5. Producing logs

```python
from datetime import datetime, timezone
from cinc import LogWriter, LiveClient, build_standalone, write_standalone, track_sample
from cinc.plugins.core_event import event

t0 = datetime(2026, 3, 1, 9, tzinfo=timezone.utc)
with LogWriter("cinc-data", "Run 42") as log:             # a log folder; cinc serve lists it
    log.header("core_event", channels=["A", "B"])
    log.write("core_event", event(t0, "Pump hot", system="Thermal", color="Yellow", channels=["A"]))
    log.write("track", track_sample(t0, 47.45, -122.3, 1500, track="UAV-1"))

write_standalone(log.path, "run42.html", "Run 42")        # or build_standalone(...) -> str

html = build_standalone({"core_event": {"channels": ["A"], "events": [...]}}, "In memory")

live = LiveClient("http://127.0.0.1:8080")                # a running `cinc serve`
live.start("Run 43")
live.post("core_event", [event(t0, "Pump hot", system="Thermal", channels=["A"])])
```

`build_standalone` accepts `{type_id: payload}`, a list of `(type_id, payload)`, a bundle
(`{"cinc": 1, "sections": [...]}`) or a log folder path. The page includes every installed plugin's
additions for the types it contains.
