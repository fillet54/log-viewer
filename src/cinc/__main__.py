from pathlib import Path

from .app import app
from .core.registry import LogTypeRegistry
from .standalone import build_standalone_bundle

USAGE = """cinc

Usage:
  cinc serve [--host HOST] [--port PORT] [--debug]
  cinc build [--log-type TYPE] --data PATH... [--output PATH] [--title TITLE]
  cinc log-types
  cinc samples

Options:
  --host HOST    Bind host. [default: 127.0.0.1]
  --port PORT    Bind port. [default: 8080]
  --debug        Enable Flask debug mode.
  --log-type TYPE  Override type for untyped input.
  --data PATH    Input file; may be repeated.
  --output PATH  Output HTML path. [default: standalone.html]
  --title TITLE  Document title. [default: HTML Log Viewer]
"""


def main(argv=None):
    from docopt import docopt

    args = docopt(USAGE, argv=argv)
    registry = LogTypeRegistry.discover()
    if args.get("log-types"):
        for item in registry.all():
            print(f"{item.id}\t{item.name}")
        return
    if args.get("samples"):
        for item in registry.all():
            for sample in item.samples():
                print(f"{item.id}\t{sample.slug}\t{sample.title}")
        return
    if args.get("build"):
        paths = [Path(value).expanduser().resolve() for value in args["--data"]]
        output = Path(args["--output"]).expanduser().resolve()
        build_standalone_bundle(paths, output, args["--title"])
        print(f"Wrote standalone viewer: {output}")
        return
    app.run(host=args["--host"], port=int(args["--port"]), debug=bool(args["--debug"]))


if __name__ == "__main__":
    main()
