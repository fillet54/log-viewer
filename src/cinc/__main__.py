from pathlib import Path

USAGE = """cinc — log viewer

Usage:
  cinc build PATH... [--log-type TYPE] [--output FILE] [--title TITLE]
  cinc serve [--data DIR] [--host HOST] [--port PORT] [--debug]
  cinc types

Commands:
  build   Write a self-contained HTML viewer. PATH is a log folder,
          a <type>.jsonl file, or a JSON payload/bundle file.
  serve   Run the live viewer. Logs are stored as folders under --data.
  types   List registered log types.

Options:
  --log-type TYPE  Log type for inputs that do not declare one.
  -o FILE, --output FILE  Output HTML path. [default: standalone.html]
  --title TITLE    Document title. [default: Log Viewer]
  --data DIR       Data directory. [default: ./cinc-data]
  --host HOST      Bind host. [default: 127.0.0.1]
  --port PORT      Bind port. [default: 8080]
  --debug          Enable Flask debug mode.
"""


def main(argv=None):
    from docopt import docopt

    args = docopt(USAGE, argv=argv)
    if args["types"]:
        from .core.registry import LogTypeRegistry

        for item in LogTypeRegistry.discover().all():
            print(f"{item.id}\t{item.name}")
        return
    if args["build"]:
        from .standalone import build_standalone_bundle

        paths = [Path(value).expanduser().resolve() for value in args["PATH"]]
        output = Path(args["--output"]).expanduser().resolve()
        build_standalone_bundle(
            paths, output, args["--title"], log_type_id=args["--log-type"]
        )
        print(f"Wrote standalone viewer: {output}")
        return
    from .app import create_app

    app = create_app(Path(args["--data"]).expanduser())
    app.run(
        host=args["--host"],
        port=int(args["--port"]),
        debug=bool(args["--debug"]),
        threaded=True,
    )


if __name__ == "__main__":
    main()
