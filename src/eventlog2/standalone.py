from __future__ import annotations

from importlib.resources import files
from pathlib import Path
import json

from jinja2 import Environment, PackageLoader, select_autoescape

from .plugin_manager import build_page_data_documents_from_path, build_page_data_from_path

SCRIPT_PATHS = [
    "static/vendor/chart.umd.min.js",
    "static/vendor/split.min.js",
    "static/vendor/preact.min.js",
    "static/vendor/preact-hooks.min.js",
    "static/vendor/htm.min.js",
    "static/vendor/preact-signals-core.min.js",
    "static/vendor/preact-signals.min.js",
    "static/runtime.js",
    "static/context.js",
    "static/hooks/use-app-services.js",
    "static/hooks/use-bus.js",
    "static/hooks/use-local-storage.js",
    "static/hooks/use-split.js",
    "static/hooks/use-virtual-list.js",
    "static/state/viewer-store.js",
    "static/mount.js",
    "static/components/detail-panel.js",
    "static/components/layout-shell.js",
    "static/components/main-view-shell.js",
    "static/components/search-panel.js",
    "static/components/viewer-root.js",
    "static/services/search.js",
    "static/services/app-services.js",
    "static/components/log-main-chart.js",
    "static/app.js",
]

TEMPLATE_ENV = Environment(
    loader=PackageLoader("eventlog2"),
    autoescape=select_autoescape(["html", "xml"]),
)


def _read_package_text(relative_path: str) -> str:
    return files("eventlog2").joinpath(relative_path).read_text(encoding="utf-8")

def build_page_data_script(page_data: dict[str, object]) -> str:
    payload = json.dumps(page_data, separators=(",", ":"), sort_keys=True)
    safe_payload = payload.replace("</script", "<\\/script")
    return f"window.EVENTLOG2_PAGE_DATA = {safe_payload};"


def _build_inline_scripts(data_script: str) -> list[str]:
    blocks: list[str] = [data_script]
    for path in SCRIPT_PATHS:
        blocks.append(_read_package_text(path))
    return blocks


def build_standalone_html(data_script: str, title: str = "HTML Log Viewer") -> str:
    styles = _read_package_text("static/styles.css")
    row_template = _read_package_text("templates/components/shared/log_row_template.html")
    script_blocks = _build_inline_scripts(data_script)
    return TEMPLATE_ENV.get_template("standalone.html").render(
        title=title,
        styles=styles,
        row_template=row_template,
        script_blocks=script_blocks,
    )


def build_standalone_file(
    plugin_id: str,
    data_path: Path,
    output_path: Path,
    title: str = "HTML Log Viewer",
) -> Path:
    page_data = build_page_data_from_path(plugin_id, data_path)
    data_script = build_page_data_script(page_data)
    html = build_standalone_html(data_script=data_script, title=title)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    return output_path


def build_standalone_files(
    plugin_id: str,
    data_path: Path,
    output_path: Path,
    title: str = "HTML Log Viewer",
) -> list[Path]:
    documents = build_page_data_documents_from_path(plugin_id, data_path)
    if len(documents) <= 1:
        return [build_standalone_file(plugin_id=plugin_id, data_path=data_path, output_path=output_path, title=title)]

    output_dir = output_path if output_path.suffix == "" else output_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    base_stem = output_path.stem if output_path.suffix else "report"
    for index, document in enumerate(documents, start=1):
        page_data = document["pageData"]
        data_script = build_page_data_script(page_data)
        doc_title = str(document.get("title") or title)
        html = build_standalone_html(data_script=data_script, title=doc_title)
        slug = str(document.get("slug") or f"{base_stem}-{index}").strip() or f"{base_stem}-{index}"
        target = output_dir / f"{slug}.html"
        target.write_text(html, encoding="utf-8")
        written.append(target)
    return written
