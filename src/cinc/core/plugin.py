"""Plugins: how a package adds to Cinc.

A plugin is a function ``setup(ctx)`` named by a ``cinc.plugins`` entry point::

    # pyproject.toml of the plugin package
    [project.entry-points."cinc.plugins"]
    acme = "acme_cinc:setup"

``setup`` gets a :class:`PluginContext` and uses it to add log types, extend
existing ones (with scripts, styles, augmenters), add charts that are not tied
to a log type, Flask blueprints (pages and API routes) and ``cinc`` CLI
commands. Asset paths (``"static/chart.js"``) are relative to the package that
defines ``setup``. See docs/plugins.md.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from importlib.resources import files
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:  # pragma: no cover
    from .log_type import LogType
    from .registry import Registry

# Browser modules are imported as "cinc-plugins/<package>/<path>".
MODULE_PREFIX = "cinc-plugins/"


@dataclass(frozen=True)
class Asset:
    """A file shipped inside a Python package (a browser script or style)."""

    package: str
    path: str

    @property
    def module_id(self) -> str:
        return f"{MODULE_PREFIX}{self.package}/{self.path}"

    @classmethod
    def from_module_id(cls, module_id: str) -> "Asset":
        if not module_id.startswith(MODULE_PREFIX):
            raise ValueError(f"not a plugin module id: {module_id!r}")
        package, _, path = module_id[len(MODULE_PREFIX) :].partition("/")
        return cls(package, path)

    def exists(self) -> bool:
        try:
            return files(self.package).joinpath(self.path).is_file()
        except ModuleNotFoundError:
            return False

    def read_text(self) -> str:
        return files(self.package).joinpath(self.path).read_text(encoding="utf-8")

    def read_bytes(self) -> bytes:
        return files(self.package).joinpath(self.path).read_bytes()


@dataclass
class Extension:
    """What one plugin adds to a log type it does not own."""

    plugin_id: str
    scripts: list[Asset] = field(default_factory=list)
    styles: list[Asset] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)
    augmenters: list[tuple[str, Callable]] = field(default_factory=list)


@dataclass
class Command:
    name: str
    function: Callable[[list[str]], Any]
    help: str
    plugin_id: str


def package_of(obj: Any) -> str:
    """The package a function or class lives in (for resolving asset paths)."""
    module = inspect.getmodule(obj)
    if module is None:
        raise ValueError(f"cannot tell which package {obj!r} belongs to")
    if hasattr(module, "__path__"):  # a package's __init__
        return module.__name__
    return module.__package__ or module.__name__


class PluginContext:
    """Handed to a plugin's ``setup``; everything a plugin adds goes through it."""

    def __init__(self, registry: "Registry", plugin_id: str, package: str):
        self.registry = registry
        self.plugin_id = plugin_id
        self.package = package

    def asset(self, path: str) -> Asset:
        """An asset in this plugin's package; raises if the file is missing."""
        asset = Asset(self.package, str(path).lstrip("/"))
        if not asset.exists():
            raise ValueError(
                f"plugin {self.plugin_id!r}: {asset.path!r} not found in package "
                f"{self.package!r} (is it listed in the package data?)"
            )
        return asset

    def add_log_type(self, log_type: "LogType | type[LogType]") -> None:
        """Register a log type (an instance or a class to instantiate)."""
        self.registry.register(log_type() if isinstance(log_type, type) else log_type)

    def extend(
        self,
        type_id: str,
        *,
        scripts: tuple[str, ...] | list[str] = (),
        styles: tuple[str, ...] | list[str] = (),
        config: dict[str, Any] | None = None,
        augmenters: dict[str, Callable] | None = None,
    ) -> None:
        """Add to a log type, usually one from another plugin.

        ``scripts`` are browser modules whose ``setup`` is called for that type
        with ``config``; ``styles`` are CSS files; ``augmenters`` map a name to
        ``fn(events, context) -> events`` run after the type normalizes its
        rows (see ``LogType.register_event_augmenter``). The type must exist
        once all plugins are loaded."""
        if config is not None and not isinstance(config, dict):
            raise ValueError(f"plugin {self.plugin_id!r}: extend config must be a dict")
        self.registry.add_extension(
            str(type_id),
            Extension(
                plugin_id=self.plugin_id,
                scripts=[self.asset(path) for path in scripts],
                styles=[self.asset(path) for path in styles],
                config=dict(config or {}),
                augmenters=list((augmenters or {}).items()),
            ),
        )

    def add_script(self, path: str) -> None:
        """A browser module loaded on every page, for charts not tied to a log
        type. Its ``setup`` runs once with ``logTypeId`` null."""
        self.registry.scripts.append(self.asset(path))

    def add_style(self, path: str) -> None:
        """A stylesheet loaded on every page."""
        self.registry.styles.append(self.asset(path))

    def add_blueprint(self, blueprint: Any, **options: Any) -> None:
        """A Flask blueprint (pages, API routes) for ``cinc serve``. Its
        templates can extend ``base.html`` for Cinc's page frame."""
        self.registry.blueprints.append((blueprint, options))

    def add_command(
        self, name: str, function: Callable[[list[str]], Any], help: str = ""
    ) -> None:
        """A ``cinc <name> ARGS...`` command; ``function`` gets the ARGS list
        and may return an exit code."""
        name = str(name).strip()
        if not name or name in self.registry.commands or name in RESERVED_COMMANDS:
            raise ValueError(f"plugin {self.plugin_id!r}: command {name!r} is taken")
        self.registry.commands[name] = Command(name, function, help, self.plugin_id)


RESERVED_COMMANDS = {"build", "serve", "types", "plugins"}
