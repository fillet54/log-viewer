from __future__ import annotations

import json
import os
from pathlib import Path

from flask import Flask, render_template

from .plugin_manager import get_plugin
from .standalone import build_page_data_script
from .live.monitor import SessionManager
from .live.routes import create_live_blueprint
from .logs.registry import LogTypeRegistry
from .logs.store import LogStore
from .logs.routes import create_logs_blueprint
from .plugins.core_event.live_monitor import CoreEventLiveMonitor

app = Flask(__name__)

_plugin = get_plugin("core-event")

_data_root = Path(os.environ.get("CINC_DATA_DIR", "./cinc-data"))

_log_registry = LogTypeRegistry()
_log_registry.register_plugin(_plugin)
_core_event_log_type = _log_registry.get("core_event")
if _core_event_log_type is None:
    raise RuntimeError("Core event log type was not registered")

_log_store = LogStore(_data_root / "eventlog2.sqlite")
_manager = SessionManager(
    store=_log_store,
    monitor=CoreEventLiveMonitor(),
    log_type=_core_event_log_type,
)
app.register_blueprint(create_live_blueprint(_manager))

app.register_blueprint(create_logs_blueprint(_log_registry, _log_store))


@app.route("/")
def index():
    payload = json.loads(_plugin.read_asset_text("samples/dev-data.json"))
    page_data = _plugin.build_page_data(payload)
    return render_template("index.html", page_data_script=build_page_data_script(page_data))


if __name__ == "__main__":
    app.run(debug=True, port=8080)
