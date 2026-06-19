from __future__ import annotations

from importlib.resources import files
from pathlib import Path
import json
import base64
import re
import os

from jinja2 import Environment, PackageLoader, select_autoescape

from .plugin_manager import build_page_data_documents_from_path, build_page_data_from_path

SCRIPT_PATHS = [
    "static/vendor/chart.umd.min.js",
    "static/vendor/split.min.js",
    "static/vendor/preact.mjs",
    "static/vendor/hooks.mjs",
    "static/vendor/signals-core.mjs",
    "static/vendor/signals.mjs",
    "static/vendor/htm-core.mjs",
    "static/vendor/htm-preact.mjs",
    "static/runtime.js",
    "static/shared.js",
    "static/context.js",
    "static/hooks/use-app-services.js",
    "static/hooks/use-split.js",
    "static/hooks/use-virtual-list.js",
    "static/state/storage.js",
    "static/state/navigation.js",
    "static/state/layout.js",
    "static/state/search.js",
    "static/state/chart.js",
    "static/state/activity.js",
    "static/state/viewer-store.js",
    "static/services/search.js",
    "static/services/app-services.js",
    "static/logview/lib.js",
    "static/logview/features/detail/DataTree.js",
    "static/logview/features/detail/CommentThread.js",
    "static/logview/features/detail/DetailPanelHeader.js",
    "static/logview/features/detail/EmptyDetailState.js",
    "static/logview/features/detail/EventSummary.js",
    "static/logview/features/detail/BookmarkSection.js",
    "static/logview/features/detail/CommentSection.js",
    "static/logview/features/detail/DataTreeSection.js",
    "static/logview/features/detail/DetailEventContent.js",
    "static/logview/features/detail/DetailPanel.js",
    "static/logview/features/search/SearchPanelHeader.js",
    "static/logview/features/search/SearchItemRow.js",
    "static/logview/features/search/FilterItemRow.js",
    "static/logview/features/search/ReadOnlyCommentThread.js",
    "static/logview/features/search/SearchHelpDialog.js",
    "static/logview/features/rows/PluginLogRow.js",
    "static/logview/features/search/RenderedRow.js",
    "static/logview/features/search/ActivityItem.js",
    "static/logview/features/search/SearchHistoryView.js",
    "static/logview/features/search/SearchFilterView.js",
    "static/logview/features/search/SearchBookmarksView.js",
    "static/logview/features/search/SearchSidebar.js",
    "static/logview/features/search/SearchControls.js",
    "static/logview/features/search/SearchResultsContent.js",
    "static/logview/features/search/SearchResultsPane.js",
    "static/logview/features/search/SearchPanel.js",
    "static/logview/features/chart/LogMainChart.js",
    "static/logview/features/main/LogVirtualList.js",
    "static/logview/features/main/MainViewToolbar.js",
    "static/logview/features/main/MainViewShell.js",
    "static/logview/features/layout/LayoutShell.js",
    "static/logview/ViewerRoot.js",
    "static/app.js",
]

GLOBAL_SCRIPTS = [
    "static/vendor/chart.umd.min.js",
    "static/vendor/split.min.js",
]

BARE_MODULES = {
    "preact": "static/vendor/preact.mjs",
    "preact/hooks": "static/vendor/hooks.mjs",
    "preact/signals": "static/vendor/signals.mjs",
    "@preact/signals-core": "static/vendor/signals-core.mjs",
    "htm": "static/vendor/htm-core.mjs",
    "htm/preact": "static/vendor/htm-preact.mjs",
    "logview/lib": "static/logview/lib.js",
}

ENTRY_POINT = "static/app.js"

TEMPLATE_ENV = Environment(
    loader=PackageLoader("eventlog2"),
    autoescape=select_autoescape(["html", "xml"]),
)


def _read_package_text(relative_path: str) -> str:
    return files("eventlog2").joinpath(relative_path).read_text(encoding="utf-8")

def _read_package_data_uri(relative_path: str, mime_type: str) -> str:
    data = files("eventlog2").joinpath(relative_path).read_bytes()
    encoded = base64.b64encode(data).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"

def _resolve_relative(current_path: str, rel_path: str) -> str:
    base_dir = os.path.dirname(current_path)
    resolved = os.path.normpath(os.path.join(base_dir, rel_path))
    return resolved.replace(os.sep, "/")

def _normalize_imports(path: str, content: str) -> str:
    def replace_import(match: re.Match) -> str:
        rel_path = match.group(2)
        if rel_path.startswith("."):
            resolved = _resolve_relative(path, rel_path)
            return f'{match.group(1)}"{resolved}"'
        return match.group(0)

    # Handle 'import ... from "..."' or 'export ... from "..."'
    # Use re.DOTALL to match newlines in the imports
    content = re.sub(
        r'((?:import|export)\s+.*?\s+from\s+)["\']([^"\']+)["\']',
        replace_import,
        content,
        flags=re.DOTALL
    )
    # Handle 'import "..."'
    content = re.sub(
        r'(import\s+)["\'](\.[^"\']+)["\']',
        replace_import,
        content,
        flags=re.DOTALL
    )
    return content

def build_page_data_script(page_data: dict[str, object]) -> str:
    payload = json.dumps(page_data, separators=(",", ":"), sort_keys=True)
    safe_payload = payload.replace("</script", "<\\/script")
    return f"window.EVENTLOG2_PAGE_DATA = {safe_payload};"


def build_standalone_html(data_script: str, title: str = "HTML Log Viewer") -> str:
    styles = _read_package_text("static/styles.css")
    
    global_scripts = [data_script]
    for path in GLOBAL_SCRIPTS:
        global_scripts.append(_read_package_text(path))
    
    module_data = {}
    
    # 1. Normalize and collect all modules
    for path in SCRIPT_PATHS:
        if path in GLOBAL_SCRIPTS:
            continue
        content = _read_package_text(path)
        module_data[path] = _normalize_imports(path, content)
    
    # 2. Add bare modules
    for name, path in BARE_MODULES.items():
        if name in module_data:
            continue
        content = _read_package_text(path)
        # Bare modules shouldn't have relative imports usually, but let's be safe
        module_data[name] = _normalize_imports(path, content)
            
    esm_data = json.dumps(module_data)

    logo_light = _read_package_data_uri("static/img/logo_lightmode.png", "image/png")
    logo_dark = _read_package_data_uri("static/img/logo_darkmode.png", "image/png")

    return TEMPLATE_ENV.get_template("standalone.html").render(
        title=title,
        styles=styles,
        global_scripts=global_scripts,
        esm_data=esm_data,
        entry_point_id=ENTRY_POINT,
        logo_light=logo_light,
        logo_dark=logo_dark,
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
