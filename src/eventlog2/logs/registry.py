from __future__ import annotations

from .types import LogTypeDefinition


class LogTypeRegistry:
    def __init__(self) -> None:
        self._types: dict[str, LogTypeDefinition] = {}
        self._aliases: dict[str, str] = {}

    def register(self, log_type: LogTypeDefinition) -> None:
        self._types[log_type.full_id] = log_type
        self._aliases[log_type.full_id.replace("-", "_")] = log_type.full_id
        legacy_id = f"{log_type.plugin_id}.{log_type.id}"
        self._aliases[legacy_id] = log_type.full_id
        self._aliases[legacy_id.replace("_", "-")] = log_type.full_id
        if log_type.full_id == "core_event":
            self._aliases["core-event.boot-log"] = log_type.full_id
            self._aliases["core_event.boot_log"] = log_type.full_id

    def register_plugin(self, plugin: object, **kwargs) -> None:
        fn = getattr(plugin, "get_log_types", None)
        if fn is None:
            return
        try:
            log_types = fn(**kwargs)
        except TypeError:
            log_types = fn()
        for log_type in log_types:
            self.register(log_type)

    def get(self, full_id: str) -> LogTypeDefinition | None:
        return self._types.get(full_id) or self._types.get(self._aliases.get(full_id, ""))

    def all(self) -> list[LogTypeDefinition]:
        return list(self._types.values())
