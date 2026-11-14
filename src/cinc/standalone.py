from __future__ import annotations

from importlib.resources import files
from pathlib import Path
import json
import base64
import re
import os

from jinja2 import Environment, PackageLoader, select_autoescape

from .core.basemap import basemap_for_tracks
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
    return f"data:{mime_type};base64,{base64.b64encode(data).decode('ascii')}"


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


LEAFLET_SCRIPT = "static/vendor/leaflet/leaflet.js"
LEAFLET_STYLE = "static/vendor/leaflet/leaflet.css"


def page_tracks(page_data: dict[str, object]) -> list:
    log_data = page_data.get("logData") if isinstance(page_data, dict) else None
    tracks = log_data.get("tracks") if isinstance(log_data, dict) else None
    return tracks if isinstance(tracks, list) else []


def _map_assets(tracks: list) -> dict[str, str]:
    """Leaflet (kept as inert text; the Map chart loads it on first use) and the
    basemap cut to the tracks' area, for a standalone page with tracks."""
    config = json.dumps({"basemap": basemap_for_tracks(tracks)}, separators=(",", ":"))
    return {
        "source": _read_package_text(LEAFLET_SCRIPT).replace("</script", "<\\/script"),
        "style": _read_package_text(LEAFLET_STYLE),
        "config": "window.CINC_MAP = " + config.replace("</", "<\\/") + ";",
    }


def build_standalone_html(
    data_script: str, title: str = "HTML Log Viewer", *, tracks: list | None = None
) -> str:
    styles = _read_package_text("static/styles.css").replace(
        '@import url("fonts.css");', ""
    )

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

    return TEMPLATE_ENV.get_template("standalone.html").render(
        title=title,
        styles=_inline_fonts(_read_package_text("static/fonts.css")) + "\n" + styles,
        global_scripts=global_scripts,
        esm_data=esm_data,
        entry_point_id=ENTRY_POINT,
        logo_light=_read_package_data_uri("static/img/logo_lightmode.png", "image/png"),
        logo_dark=_read_package_data_uri("static/img/logo_darkmode.png", "image/png"),
        map=_map_assets(tracks) if tracks else None,
    )


def _inline_fonts(fonts_css: str) -> str:
    """fonts.css with each url("fonts/...") replaced by a data URI."""

    def inline(match: re.Match) -> str:
        uri = _read_package_data_uri(f"static/fonts/{match.group(1)}", "font/woff2")
        return f'url("{uri}")'

    return re.sub(r'url\("fonts/([^"]+)"\)', inline, fonts_css)


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
    html = build_standalone_html(
        build_page_data_script(page_data), title=title, tracks=page_tracks(page_data)
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    return output_path
