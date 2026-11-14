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
| `ctx.extend("core_event", scripts=..., config=...)` | `static/owners.js` adds a **Faults by Owner** chart to core_event logs. It imports `./bars.js`, a second file. |
| `ctx.add_blueprint(...)` | `/acme/`: open faults per owner across all logs, and a button that writes a demo log. |
| `ctx.add_command("acme-demo", ...)` | `cinc acme-demo DIR`: a demo log built with `cinc.LogWriter` and `core_event.event`. |

See `docs/plugins.md` in the Cinc repo for the full API.
