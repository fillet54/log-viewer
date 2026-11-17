"""`cinc acme-demo DIR`: a plugin-provided CLI command."""

from __future__ import annotations

from pathlib import Path

from cinc import write_standalone

from .generate import write_demo_log


def demo_command(args: list[str]) -> int:
    if len(args) != 1 or args[0].startswith("-"):
        print("usage: cinc acme-demo DIR")
        return 2
    path = write_demo_log(Path(args[0]))
    page = write_standalone(path, path.with_suffix(".html"), "Acme demo flight")
    print(f"log folder: {path}\nstandalone: {page}")
    return 0
