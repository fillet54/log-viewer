"""The registry: every log type, extension, script, page and command that
plugins added. ``Registry.discover()`` loads the installed plugins."""

from __future__ import annotations

from importlib.metadata import entry_points
from typing import Any, Callable

from .log_type import LogType
from .plugin import Asset, Command, Extension, PluginContext, package_of
from .track import TrackLogType

ENTRY_POINT_GROUP = "cinc.plugins"

# Log types that ship with the core and are always registered.
BUILTIN_TYPES = (TrackLogType,)


class Registry:
    def __init__(self) -> None:
        self._types: dict[str, LogType] = {}
        self._extensions: dict[str, list[Extension]] = {}
        self.plugins: dict[str, str] = {}  # plugin id -> package
        self.scripts: list[Asset] = []  # global browser modules
        self.styles: list[Asset] = []  # global stylesheets
        self.blueprints: list[tuple[Any, dict[str, Any]]] = []
        self.commands: dict[str, Command] = {}
        self._finished = False

    # -- log types --------------------------------------------------------

    def register(self, log_type: LogType) -> None:
        if not isinstance(log_type, LogType):
            raise TypeError("registered object must be a LogType")
        if log_type.id in self._types:
            raise ValueError(f"Duplicate log type id: {log_type.id}")
        self._types[log_type.id] = log_type

    def get(self, log_type_id: str) -> LogType | None:
        return self._types.get(log_type_id)

    def all(self) -> list[LogType]:
        return sorted(self._types.values(), key=lambda item: item.name)

    # -- extensions and assets --------------------------------------------

    def add_extension(self, type_id: str, extension: Extension) -> None:
        self._extensions.setdefault(type_id, []).append(extension)

    def extensions_for(self, type_id: str) -> list[Extension]:
        return list(self._extensions.get(type_id, []))

    def extensions(self) -> dict[str, list[Extension]]:
        """Type id -> the extensions plugins added to it."""
        return {type_id: list(items) for type_id, items in self._extensions.items()}

    def asset_packages(self) -> set[str]:
        """Packages whose files may be served as plugin assets."""
        packages = set(self.plugins.values())
        for log_type in self._types.values():
            packages.update(asset.package for asset in log_type.scripts())
            packages.update(asset.package for asset in log_type.styles())
        return packages

    # -- loading ------------------------------------------------------------

    def load_plugin(self, plugin_id: str, setup: Callable[[PluginContext], Any]):
        if plugin_id in self.plugins:
            raise ValueError(f"Duplicate plugin id: {plugin_id}")
        if not callable(setup):
            setup = getattr(setup, "setup", None)
        if not callable(setup):
            raise TypeError(f"plugin {plugin_id!r} must be a setup(ctx) function")
        package = package_of(setup)
        self.plugins[plugin_id] = package
        setup(PluginContext(self, plugin_id, package))

    def finish(self) -> "Registry":
        """Add the built-in types and apply extensions; call once after loading."""
        if self._finished:
            return self
        for builtin in BUILTIN_TYPES:
            if self.get(builtin.id) is None:
                self.register(builtin())
        for type_id, extensions in self._extensions.items():
            log_type = self.get(type_id)
            if log_type is None:
                plugins = ", ".join(sorted({e.plugin_id for e in extensions}))
                raise ValueError(
                    f"plugin(s) {plugins} extend log type {type_id!r}, which no "
                    f"installed plugin provides"
                )
            for extension in extensions:
                for name, function in extension.augmenters:
                    log_type.register_event_augmenter(
                        f"{extension.plugin_id}:{name}", function
                    )
                for name, function in extension.whole_log_augmenters:
                    log_type.register_event_augmenter(
                        f"{extension.plugin_id}:{name}", function, whole_log=True
                    )
        self._finished = True
        return self

    @classmethod
    def from_plugins(
        cls, plugins: dict[str, Callable[[PluginContext], Any]]
    ) -> "Registry":
        """A registry from explicit ``{plugin id: setup}`` (tests, embedding)."""
        registry = cls()
        for plugin_id, setup in plugins.items():
            registry.load_plugin(plugin_id, setup)
        return registry.finish()

    @classmethod
    def discover(
        cls, extra: dict[str, Callable[[PluginContext], Any]] | None = None
    ) -> "Registry":
        """The installed plugins (``cinc.plugins`` entry points), plus ``extra``."""
        plugins: dict[str, Callable] = {}
        for point in entry_points(group=ENTRY_POINT_GROUP):
            if point.name not in plugins:
                plugins[point.name] = point.load()
        plugins.update(extra or {})
        return cls.from_plugins(plugins)
