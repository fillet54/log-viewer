from __future__ import annotations

import json
import os
from importlib.resources import files
from pathlib import Path

from flask import Flask, abort, render_template

from .core.registry import LogTypeRegistry
from .logs.store import LogStore
from .logs.routes import create_logs_blueprint
from .live.session import LiveSessionRegistry
from .live.routes import create_live_blueprint
from .standalone import build_page_data_script


def create_app(data_dir: Path | None = None) -> Flask:
    registry = LogTypeRegistry.discover()
    root = data_dir or Path(os.environ.get("CINC_DATA_DIR", "./cinc-data"))
    store = LogStore(root / "cinc.sqlite")
    live = LiveSessionRegistry(registry, store)
    app = Flask(__name__)
    app.extensions["cinc"] = {"registry": registry, "store": store, "live": live}
    app.register_blueprint(create_live_blueprint(live, registry))
    app.register_blueprint(create_logs_blueprint(registry, store))

    @app.get("/")
    def home():
        entries = []
        for log_type in registry.all():
            entries.append(
                {
                    "log_type": log_type,
                    "count": store.count(log_type.id),
                    "samples": log_type.samples(),
                }
            )
        return render_template("home.html", entries=entries)

    @app.get("/demo/<log_type_id>/<sample_slug>")
    def demo(log_type_id: str, sample_slug: str):
        log_type = registry.get(log_type_id)
        if log_type is None:
            abort(404)
        sample = next(
            (item for item in log_type.samples() if item.slug == sample_slug), None
        )
        if sample is None:
            abort(404)
        package = log_type.__class__.__module__.rsplit(".", 1)[0]
        payload = json.loads(
            files(package).joinpath(sample.path).read_text(encoding="utf-8")
        )
        page_data = log_type.build_page_data(payload)
        return render_template(
            "index.html", page_data_script=build_page_data_script(page_data)
        )

    return app


app = create_app()
