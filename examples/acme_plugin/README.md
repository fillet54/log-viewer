# acme-cinc: an example Cinc plugin

A project-side plugin for the common case: logs use the built-in `core_event` type, and the project
adds its own knowledge, views and tooling on top. Install it next to Cinc:

```bash
pip install -e examples/acme_plugin
cinc plugins            # shows "acme" and what it adds
cinc acme-demo out/     # writes a demo log folder and a standalone page
cinc serve              # adds an "Acme" page to the top bar
```

What `setup(ctx)` in `src/acme_cinc/__init__.py` adds:

| Call | Effect |
|---|---|
| `ctx.extend("core_event", augmenters=...)` | Every core_event row gets an `owner` (the team for its system); search `owner:Thermal`. |
| `ctx.extend("core_event", scripts=..., config=...)` | Three charts for core_event logs (below). |
| `ctx.add_blueprint(...)` | `/acme/`: open faults per owner across all logs, and a button that writes a demo log. |
| `ctx.add_command("acme-demo", ...)` | `cinc acme-demo DIR`: a 30-minute demo flight built with `cinc.LogWriter` and `core_event.event`, plus its standalone page. |

The charts (pick them in the chart menu of a core_event log):

- **Overview** (`overview.js`): start, end and duration; rows per type; fault facts (worst severity,
  most open at once, longest, most frequent, still set at the end, open at the selected row); a
  per-system table; and distance and altitude per track. Values with a time jump the log there.
- **Fault Timeline** (`faults.js`): a section per system/subsystem with a summary and a strip of
  when any of its faults was set, then one row per fault: a bar in its severity colour while set,
  ▲ at each SET and ▽ at each CLEAR. A cursor follows the selected row. Click a mark to select that
  row, or empty track to move the log to that time. The toolbar picks one subsystem or collapses
  all sections.
- **Faults by Owner** (`owners.js` + `bars.js`): open faults per owning team at the selected row.

`model.js` turns core_event rows into fault intervals for the first two; both import it, which also
shows a plugin split across several files (standalone pages inline every imported file).

See `docs/plugins.md` in the Cinc repo for the full API.
