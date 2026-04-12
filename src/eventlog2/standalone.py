from __future__ import annotations

from html import escape
from importlib.resources import files
from pathlib import Path
import json
from urllib.parse import quote

from .plugin_manager import build_page_data_from_path

SCRIPT_PATHS = [
    "static/vendor/chart.umd.min.js",
    "static/services/search.js",
    "static/shared/log-row-helper.js",
    "static/services/app-services.js",
    "static/app.js",
    "static/components/log-layout.js",
    "static/components/log-main-chart.js",
    "static/components/log-main-view.js",
    "static/components/log-detail-panel.js",
    "static/components/log-search-panel.js",
]


def _read_package_text(relative_path: str) -> str:
    return files("eventlog2").joinpath(relative_path).read_text(encoding="utf-8")


def _build_search_help_data_url() -> str:
    help_html = _read_package_text("static/search_syntax.html")
    encoded = quote(help_html, safe="")
    return f"data:text/html;charset=utf-8,{encoded}"


def _escape_inline_script(text: str) -> str:
    return text.replace("</script", "<\\/script")


def build_page_data_script(page_data: dict[str, object]) -> str:
    payload = json.dumps(page_data, separators=(",", ":"), sort_keys=True)
    return f"window.EVENTLOG2_PAGE_DATA = {_escape_inline_script(payload)};"


def _build_inline_scripts(data_script: str) -> str:
    blocks: list[str] = [data_script]
    for path in SCRIPT_PATHS:
        blocks.append(_read_package_text(path))
    script_blocks = []
    for block in blocks:
        safe_block = _escape_inline_script(block)
        script_blocks.append(f"<script>\n{safe_block}\n</script>")
    return "\n".join(script_blocks)


def build_standalone_html(data_script: str, title: str = "HTML Log Viewer") -> str:
    styles = _read_package_text("static/styles.css")
    row_template = _read_package_text("templates/components/shared/log_row_template.html")
    help_url = _build_search_help_data_url()
    body_scripts = _build_inline_scripts(data_script)
    escaped_title = escape(title)

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{escaped_title}</title>
    <script>
      (function () {{
        const storageKey = "eventlog2-theme";
        const storedTheme = localStorage.getItem(storageKey);
        const preferredTheme = window.matchMedia("(prefers-color-scheme: dark)").matches
          ? "dark"
          : "light";
        document.documentElement.dataset.theme = storedTheme || preferredTheme;
      }})();
    </script>
    <style>
{styles}
    </style>
  </head>
  <body class="app-body app-body-viewer">
    <div class="app-shell">
      <header class="topbar">
        <div class="topbar-inner">
          <span class="brand">HTML Log Viewer</span>
          <div class="topbar-actions">
            <button type="button" id="theme-toggle" class="button button-ghost button-xs" aria-label="Toggle theme">
              Theme
            </button>
            <span class="user-state">Standalone Page</span>
          </div>
        </div>
      </header>
      <main class="app-content app-content-viewer">
        <div class="viewer-page">
          <log-viewer-app>
            <log-layout>
              <log-main-view>
                {row_template}
              </log-main-view>
              <log-detail-panel></log-detail-panel>
              <log-search-panel search-help-url="{help_url}">
                {row_template}
              </log-search-panel>
            </log-layout>
          </log-viewer-app>
        </div>
      </main>
    </div>
    <script>
      (function () {{
        const storageKey = "eventlog2-theme";
        const button = document.getElementById("theme-toggle");
        if (!button) return;
        const root = document.documentElement;
        const syncLabel = () => {{
          const theme = root.dataset.theme === "dark" ? "Dark" : "Light";
          button.textContent = theme;
          button.setAttribute("aria-label", `Switch theme. Current theme: ${{theme}}`);
        }};
        button.addEventListener("click", () => {{
          root.dataset.theme = root.dataset.theme === "dark" ? "light" : "dark";
          localStorage.setItem(storageKey, root.dataset.theme);
          syncLabel();
        }});
        syncLabel();
      }})();
    </script>
{body_scripts}
  </body>
</html>
"""


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
