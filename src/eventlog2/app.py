from __future__ import annotations

import json
import os
from pathlib import Path

from flask import Flask, render_template

from .plugin_manager import get_plugin
from .standalone import build_page_data_script
from .live.storage import SessionStore
from .live.monitor import SessionManager
from .live.routes import create_live_blueprint
from .plugins.core_event.live_monitor import CoreEventLiveMonitor

app = Flask(__name__)

_plugin = get_plugin("core-event")

_sessions_dir = Path(os.environ.get("CINC_DATA_DIR", "./cinc-data")) / "sessions"
_store = SessionStore(_sessions_dir)
_manager = SessionManager(
    store=_store,
    monitor=CoreEventLiveMonitor(),
    plugin_id=_plugin.plugin_id,
    build_page_data=_plugin.build_page_data,
    normalize_events=_plugin.normalize_stream_events,
)
app.register_blueprint(create_live_blueprint(_manager, _store))


@app.route("/")
def index():
    payload = json.loads(_plugin.read_asset_text("samples/dev-data.json"))
    page_data = _plugin.build_page_data(payload)
    return render_template("index.html", page_data_script=build_page_data_script(page_data))


if __name__ == "__main__":
    app.run(debug=True, port=8080)
