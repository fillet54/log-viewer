from importlib.metadata import entry_points
from cinc.logs.types import LogType


class LogTypeRegistry:
    def __init__(self) -> None:
        self._types: dict[str, LogType] = {}

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

    @classmethod
    def discover(cls) -> "LogTypeRegistry":
        registry = cls()
        loaded_points = set()
        for point in entry_points(group="cinc.log_types"):
            identity = (point.name, point.value)
            if identity in loaded_points:
                continue
            loaded_points.add(identity)
            loaded = point.load()
            obj = loaded() if isinstance(loaded, type) else loaded
            registry.register(obj)
        return registry
