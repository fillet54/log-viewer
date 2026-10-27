from __future__ import annotations

from importlib.resources import files
from pathlib import Path
import json
import base64
import re
import os

from jinja2 import Environment, PackageLoader, select_autoescape

from .core.bundle import build_from_paths
from .core.registry import LogTypeRegistry

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


def _discover_frontend_modules() -> list[str]:
    root = files("cinc").joinpath("static")
    modules = []
    for item in root.rglob("*"):
        if item.suffix in {".js", ".mjs"} and "vendor" not in item.parts:
            modules.append(str(item.relative_to(files("cinc"))).replace("\\", "/"))
    return sorted(set(modules + list(BARE_MODULES.values())))


ENTRY_POINT = "static/app.js"

TEMPLATE_ENV = Environment(
    loader=PackageLoader("cinc"),
    autoescape=select_autoescape(["html", "xml"]),
)


def _read_package_text(relative_path: str) -> str:
    return files("cinc").joinpath(relative_path).read_text(encoding="utf-8")


def _read_package_data_uri(relative_path: str, mime_type: str) -> str:
    data = files("cinc").joinpath(relative_path).read_bytes()
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
        flags=re.DOTALL,
    )
    # Handle 'import "..."'
    content = re.sub(
        r'(import\s+)["\'](\.[^"\']+)["\']',
        replace_import,
        content,
        flags=re.DOTALL,
    )
    return content


def build_page_data_script(page_data: dict[str, object]) -> str:
    payload = json.dumps(page_data, separators=(",", ":"), sort_keys=True)
    safe_payload = payload.replace("</script", "<\\/script")
    return f"window.CINC_PAGE_DATA = {safe_payload};"


def build_standalone_html(data_script: str, title: str = "HTML Log Viewer") -> str:
    styles = _read_package_text("static/styles.css")

    global_scripts = [data_script]
    for path in GLOBAL_SCRIPTS:
        global_scripts.append(_read_package_text(path))

    module_data = {}

    # 1. Normalize and collect all modules
    for path in _discover_frontend_modules():
        if path in GLOBAL_SCRIPTS:
            continue
        content = _read_package_text(path)
        module_data[path] = _normalize_imports(path, content)

    # 2. Add bare modules
    for name, path in BARE_MODULES.items():
        if name in module_data:
            continue
        content = _read_package_text(path)
        # Normalize defensively.
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


def build_standalone_bundle(
    data_paths: list[Path],
    output_path: Path,
    title: str = "HTML Log Viewer",
    *,
    log_type_id: str | None = None,
) -> Path:
    page_data = build_from_paths(
        data_paths, LogTypeRegistry.discover(), log_type_id=log_type_id
    )
    html = build_standalone_html(build_page_data_script(page_data), title=title)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    return output_path
