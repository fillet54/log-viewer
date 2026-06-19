from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
import json
from importlib import import_module
from importlib.resources import files
from pathlib import Path
import re
from typing import Any


@dataclass(frozen=True)
class EventLogDocument:
    slug: str
    payload: Any
    title: str | None = None


class EventLogViewPlugin(ABC):
    plugin_id: str
    plugin_name: str
    asset_package: str | None = None
    script_paths: tuple[str, ...] = ()
    style_paths: tuple[str, ...] = ()
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

    @abstractmethod
    def parse_payload(self, payload: Any) -> dict[str, Any]:
        raise NotImplementedError

    def get_asset_package(self) -> str:
        if self.asset_package:
            return self.asset_package
        module_name = self.__class__.__module__
        package_name, _, _ = module_name.rpartition(".")
        return package_name or module_name

    def read_asset_text(self, relative_path: str) -> str:
        package = import_module(self.get_asset_package())
        return files(package).joinpath(relative_path).read_text(encoding="utf-8")

    def get_inline_scripts(self) -> list[str]:
        return [self.read_asset_text(path) for path in self.script_paths]

    def get_inline_styles(self) -> list[str]:
        return [self.read_asset_text(path) for path in self.style_paths]

    def get_view_config(self) -> dict[str, Any]:
        return {
            "scripts": self.get_inline_scripts(),
            "styles": self.get_inline_styles(),
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


class EventLogSourcePlugin(EventLogViewPlugin):
    def read_payload_file(self, data_path: Path) -> Any:
        return self._load_json_text(data_path.read_text(encoding="utf-8"), data_path)

    def read_source_path(self, data_path: Path) -> Any:
        return self.read_payload_file(data_path)

    def build_documents(self, source: Any, source_path: Path | None = None) -> list[EventLogDocument]:
        return [EventLogDocument(slug="report", payload=source)]

    def build_page_data_for_document(self, document: EventLogDocument) -> dict[str, Any]:
        return self.build_page_data(document.payload)

    def parse_path(self, data_path: Path) -> dict[str, Any]:
        return self.parse_documents_path(data_path)[0]["pageData"]

    def parse_documents_path(self, data_path: Path) -> list[dict[str, Any]]:
        source = self.read_source_path(data_path)
        documents = self.build_documents(source, source_path=data_path)
        results: list[dict[str, Any]] = []
        for index, document in enumerate(documents):
            slug = str(document.slug or f"report-{index + 1}").strip() or f"report-{index + 1}"
            results.append(
                {
                    "slug": slug,
                    "title": document.title,
                    "pageData": self.build_page_data_for_document(document),
                }
            )
        return results
