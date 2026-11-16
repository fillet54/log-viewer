from pathlib import Path

USAGE = """cinc — log viewer

Usage:
  cinc build PATH... [--log-type TYPE] [--output FILE] [--title TITLE]
  cinc serve [--data DIR] [--host HOST] [--port PORT] [--threads N] [--debug]
  cinc types
  cinc plugins
  cinc COMMAND [ARGS...]

Commands:
  build    Write a self-contained HTML viewer. PATH is a log folder,
           a <type>.jsonl file, or a JSON payload/bundle file.
  serve    Run the live viewer. Logs are stored as folders under --data.
  types    List registered log types.
  plugins  List installed plugins and what they add, including commands.
  COMMAND  A command added by a plugin (see `cinc plugins`).

Options:
  --log-type TYPE  Log type for inputs that do not declare one.
  -o FILE, --output FILE  Output HTML path. [default: standalone.html]
  --title TITLE    Document title. [default: Log Viewer]
  --data DIR       Data directory. [default: ./cinc-data]
  --host HOST      Bind host. [default: 127.0.0.1]
  --port PORT      Bind port. [default: 8080]
  --threads N      Server threads; each open live viewer holds one.
                   [default: 16]
  --debug          Use Flask's debug server instead of waitress.
"""


BUILTIN = {"build", "serve", "types", "plugins"}


def _print_plugins(registry) -> None:
    for plugin_id, package in sorted(registry.plugins.items()):
        print(f"{plugin_id}  ({package})")
        for log_type in registry.all():
            if type(log_type).__module__.startswith(package):
                print(f"  log type  {log_type.id}  {log_type.name}")
        for type_id, extensions in sorted(registry.extensions().items()):
            for extension in extensions:
                if extension.plugin_id == plugin_id:
                    print(f"  extends   {type_id}")
        for asset in registry.scripts:
            if asset.package == package:
                print(f"  script    {asset.path}")
        for blueprint, _ in registry.blueprints:
            if blueprint.import_name.startswith(package):
                print(f"  pages     {blueprint.name}")
        for command in registry.commands.values():
            if command.plugin_id == plugin_id:
                print(f"  command   cinc {command.name}  {command.help}".rstrip())


def main(argv=None):
    import sys

    from docopt import docopt

    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and not argv[0].startswith("-") and argv[0] not in BUILTIN:
        from .core.registry import Registry

        command = Registry.discover().commands.get(argv[0])
        if command is None:
            sys.exit(f"cinc: unknown command {argv[0]!r} (see `cinc --help`)")
        return command.function(argv[1:])

    args = docopt(USAGE, argv=argv)
    if args["types"] or args["plugins"]:
        from .core.registry import Registry

        registry = Registry.discover()
        if args["plugins"]:
            _print_plugins(registry)
            return
        for item in registry.all():
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

    if not _is_loopback(args["--host"]):
        print(
            f"warning: serving on {args['--host']}; cinc has no authentication, "
            "so anyone who can reach it can read and post logs",
            file=sys.stderr,
        )
    app = create_app(Path(args["--data"]).expanduser())
    if args["--debug"]:
        app.run(host=args["--host"], port=int(args["--port"]), debug=True)
        return
    import signal

    from waitress import serve

    def stop(signum, frame):
        # Waitress waits for busy threads before it closes connections, and
        # each open live viewer holds a thread; end those streams first.
        app.extensions["cinc"]["hub"].close()
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    print(f"Serving on http://{args['--host']}:{args['--port']}/", file=sys.stderr)
    serve(
        app,
        host=args["--host"],
        port=int(args["--port"]),
        threads=int(args["--threads"]),
    )


def _is_loopback(host: str) -> bool:
    import ipaddress

    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


if __name__ == "__main__":
    raise SystemExit(main())
