from __future__ import annotations

from abc import ABC, abstractmethod
import json
from importlib import import_module
from importlib.resources import files
from pathlib import Path
import re
from typing import Any


class EventLogPlugin(ABC):
    plugin_id: str
    plugin_name: str
    asset_package: str | None = None
    row_template_path: str | None = None
    script_paths: tuple[str, ...] = ()
    row_settings: dict[str, Any] = {}

    def _load_json_text(self, text: str, data_path: Path) -> Any:
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        match = re.search(r"window\.EVENTLOG2_PAGE_DATA\s*=\s*", text)
        if match:
            decoder = json.JSONDecoder()
            try:
                page_data, _ = decoder.raw_decode(text[match.end() :].lstrip())
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"{data_path} looks like a JavaScript page-data wrapper, but the assigned value is not valid JSON."
                ) from exc

            if isinstance(page_data, dict):
                if isinstance(page_data.get("payload"), dict):
                    return page_data["payload"]
                if isinstance(page_data.get("logData"), dict):
                    return page_data["logData"]
                return page_data

        raise ValueError(
            f"{data_path} is not valid JSON. Pass raw plugin input JSON or a simple "
            'JavaScript assignment like `window.EVENTLOG2_PAGE_DATA = {...};`.'
        )

    def read_payload_file(self, data_path: Path) -> Any:
        return self._load_json_text(data_path.read_text(encoding="utf-8"), data_path)

    @abstractmethod
    def parse_payload(self, payload: Any) -> dict[str, Any]:
        raise NotImplementedError

    def parse_path(self, data_path: Path) -> dict[str, Any]:
        payload = self.read_payload_file(data_path)
        return self.build_page_data(payload)

    def get_asset_package(self) -> str:
        if self.asset_package:
            return self.asset_package
        module_name = self.__class__.__module__
        package_name, _, _ = module_name.rpartition(".")
        return package_name or module_name

    def read_asset_text(self, relative_path: str) -> str:
        package = import_module(self.get_asset_package())
        return files(package).joinpath(relative_path).read_text(encoding="utf-8")

    def get_row_template_html(self) -> str:
        if not self.row_template_path:
            return ""
        return self.read_asset_text(self.row_template_path)

    def get_inline_scripts(self) -> list[str]:
        return [self.read_asset_text(path) for path in self.script_paths]

    def get_view_config(self) -> dict[str, Any]:
        return {
            "rowTemplate": self.get_row_template_html(),
            "scripts": self.get_inline_scripts(),
            "rowSettings": dict(self.row_settings),
        }

    def build_page_data(self, payload: Any) -> dict[str, Any]:
        return {
            "plugin": {
                "id": self.plugin_id,
                "name": self.plugin_name,
            },
            "logData": self.parse_payload(payload),
            "view": self.get_view_config(),
        }
