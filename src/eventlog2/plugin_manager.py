from __future__ import annotations

from importlib.metadata import entry_points
from pathlib import Path
from typing import Any

from .plugins import CoreEventPlugin, EventLogPlugin


def _built_in_plugins() -> list[EventLogPlugin]:
    return [CoreEventPlugin()]


def list_plugins() -> list[EventLogPlugin]:
    plugins: dict[str, EventLogPlugin] = {plugin.plugin_id: plugin for plugin in _built_in_plugins()}
    for entry_point in entry_points(group="eventlog2.plugins"):
        plugin = entry_point.load()()
        if not isinstance(plugin, EventLogPlugin):
            raise TypeError(f'Plugin "{entry_point.name}" must inherit EventLogPlugin.')
        plugins[plugin.plugin_id] = plugin
    return sorted(plugins.values(), key=lambda plugin: plugin.plugin_id)


def get_plugin(plugin_id: str) -> EventLogPlugin:
    normalized = str(plugin_id or "").strip().lower()
    for plugin in list_plugins():
        if plugin.plugin_id == normalized:
            return plugin
    available = ", ".join(plugin.plugin_id for plugin in list_plugins()) or "(none)"
    raise LookupError(f'Unknown event log plugin "{plugin_id}". Available plugins: {available}')


def build_page_data(plugin_id: str, payload: Any) -> dict[str, Any]:
    return get_plugin(plugin_id).build_page_data(payload)


def build_page_data_from_path(plugin_id: str, data_path: Path) -> dict[str, Any]:
    return get_plugin(plugin_id).parse_path(data_path)
