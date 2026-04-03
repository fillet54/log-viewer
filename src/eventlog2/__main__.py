from __future__ import annotations

from .app import app

USAGE = """eventlog2

Usage:
  eventlog2 [--host HOST] [--port PORT] [--debug] [--waitress]
  eventlog2 (-h | --help)
  eventlog2 --version

Options:
  --host HOST       Bind host. [default: 127.0.0.1]
  --port PORT       Bind port. [default: 8080]
  --debug           Run the Flask development server with debug enabled.
  --waitress        Run with Waitress instead of Flask's development server.
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


def main(argv: list[str] | None = None) -> None:
    try:
        from docopt import docopt
    except ImportError as exc:
        raise SystemExit("docopt is required for the eventlog2 CLI. Install project dependencies first.") from exc

    args = docopt(USAGE, argv=argv, version="eventlog2 0.1.0")
    host, port, debug, use_waitress = _apply_runtime_overrides(args)

    if use_waitress:
        from waitress import serve
        serve(app, host=host, port=port)
        return

    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    main()
