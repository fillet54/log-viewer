from __future__ import annotations

from pathlib import Path

from .app import app
from .plugin_manager import get_plugin, list_plugins
from .standalone import build_standalone_files

USAGE = """eventlog2

Usage:
  eventlog2 serve [--host HOST] [--port PORT] [--debug] [--waitress]
  eventlog2 build --plugin PLUGIN --data PATH [--output PATH] [--title TITLE]
  eventlog2 plugins
  eventlog2 [--host HOST] [--port PORT] [--debug] [--waitress]
  eventlog2 (-h | --help)
  eventlog2 --version

Options:
  --host HOST       Bind host. [default: 127.0.0.1]
  --port PORT       Bind port. [default: 8080]
  --debug           Run the Flask development server with debug enabled.
  --waitress        Run with Waitress instead of Flask's development server.
  --plugin PLUGIN   Log parser plugin id.
  --data PATH       Path to the input file for the selected plugin.
  --output PATH     Output HTML path. [default: standalone.html]
  --title TITLE     HTML document title. [default: HTML Log Viewer]
  -h --help         Show this screen.
  --version         Show version.
"""


def _apply_runtime_overrides(args: dict[str, object]) -> tuple[str, int, bool, bool]:
    host = str(args["--host"])
    try:
        port = int(args["--port"])
    except (TypeError, ValueError) as exc:
        raise SystemExit("--port must be an integer") from exc
    if port <= 0 or port > 65535:
        raise SystemExit("--port must be between 1 and 65535")

    debug = bool(args["--debug"])
    use_waitress = bool(args["--waitress"]) and not debug

    return host, port, debug, use_waitress


def _build_standalone(args: dict[str, object]) -> list[Path]:
    plugin_id = str(args["--plugin"])
    try:
        get_plugin(plugin_id)
    except LookupError as exc:
        raise SystemExit(str(exc)) from exc

    data_path = Path(str(args["--data"])).expanduser().resolve()
    if not data_path.is_file():
        raise SystemExit(f"--data file not found: {data_path}")

    output_path = Path(str(args["--output"])).expanduser().resolve()
    title = str(args["--title"])
    return build_standalone_files(
        plugin_id=plugin_id,
        data_path=data_path,
        output_path=output_path,
        title=title,
    )


def _print_plugins() -> None:
    for plugin in list_plugins():
        print(f"{plugin.plugin_id}\t{plugin.plugin_name}")


def main(argv: list[str] | None = None) -> None:
    try:
        from docopt import docopt
    except ImportError as exc:
        raise SystemExit("docopt is required for the eventlog2 CLI. Install project dependencies first.") from exc

    args = docopt(USAGE, argv=argv, version="eventlog2 0.1.0")

    if args["plugins"]:
        _print_plugins()
        return

    if args["build"]:
        output_paths = _build_standalone(args)
        if len(output_paths) == 1:
            print(f"Wrote standalone viewer: {output_paths[0]}")
        else:
            print(f"Wrote {len(output_paths)} standalone viewers:")
            for path in output_paths:
                print(path)
        return

    host, port, debug, use_waitress = _apply_runtime_overrides(args)

    if use_waitress:
        from waitress import serve
        serve(app, host=host, port=port)
        return

    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    main()
