from __future__ import annotations

import json
import os
import queue
from datetime import datetime, timezone
from pathlib import Path

from flask import (
    Flask,
    Response,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    stream_with_context,
    url_for,
)

from .core.registry import LogTypeRegistry
from .live import LiveHub
from .logdir import LogDir, is_log_dir, list_logs, valid_id
from .standalone import build_page_data_script, build_standalone_html


def when(value) -> str:
    """'2026-10-27 05:19:39Z' for an ISO time; anything else as it is."""
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return str(value or "")
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.strftime("%Y-%m-%d %H:%M:%SZ")


def create_app(data_dir: Path | None = None) -> Flask:
    root = Path(data_dir or os.environ.get("CINC_DATA_DIR", "./cinc-data"))
    root.mkdir(parents=True, exist_ok=True)
    registry = LogTypeRegistry.discover()
    hub = LiveHub(root, registry)
    app = Flask(__name__)
    app.extensions["cinc"] = {"registry": registry, "hub": hub, "root": root}
    app.jinja_env.filters["when"] = when

    def find_log(log_id: str) -> LogDir:
        path = root / log_id
        if not valid_id(log_id) or not is_log_dir(path):
            abort(404)
        return LogDir(path)

    def is_current(log: LogDir) -> bool:
        return hub.current is not None and hub.current.id == log.id

    # -- pages -------------------------------------------------------------

    @app.get("/")
    def live():
        if hub.current is None:
            return render_template("waiting.html", log=None)
        return redirect(url_for("view_log", log_id=hub.current.id))

    @app.get("/logs")
    def history():
        logs = [
            {"log": log, "meta": log.meta(), "types": log.types()}
            for log in list_logs(root)
        ]
        current_id = hub.current.id if hub.current else None
        return render_template("history.html", logs=logs, current_id=current_id)

    @app.get("/logs/<log_id>")
    def view_log(log_id: str):
        log = find_log(log_id)
        live_now = is_current(log)
        if not log.types():
            return render_template("waiting.html", log=log, live=live_now)
        try:
            page = hub.page_data(log)
        except ValueError as exc:
            return f"Cannot open log {log_id}: {exc}", 400
        ids = [item.id for item in list_logs(root)]
        index = ids.index(log.id) if log.id in ids else -1
        return render_template(
            "viewer.html",
            log=log,
            meta=log.meta(),
            live=live_now,
            newer=ids[index - 1] if index > 0 else None,
            older=ids[index + 1] if 0 <= index < len(ids) - 1 else None,
            page_data_script=build_page_data_script(page),
        )

    @app.get("/logs/<log_id>/standalone.html")
    def download_log(log_id: str):
        log = find_log(log_id)
        page = hub.page_data(log)
        page.pop("live", None)
        html = build_standalone_html(
            build_page_data_script(page), title=log.meta()["name"]
        )
        return Response(
            html,
            mimetype="text/html",
            headers={"Content-Disposition": f'attachment; filename="{log.id}.html"'},
        )

    # -- API ---------------------------------------------------------------

    @app.post("/api/logs")
    def api_start_log():
        body = request.get_json(silent=True) or {}
        log = hub.start(str(body["name"]) if body.get("name") else None)
        return jsonify({"id": log.id, "url": url_for("view_log", log_id=log.id)}), 201

    @app.post("/api/events")
    def api_post_events():
        body = request.get_json(silent=True)
        if not isinstance(body, dict) or not body.get("type"):
            return (
                jsonify({"error": 'Body must be {"type": ..., "events": [...]}'}),
                400,
            )
        rows = body.get("events", [body["event"]] if "event" in body else [])
        try:
            log = hub.post(str(body["type"]), rows, body.get("header"))
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        return jsonify({"log": log.id, "count": len(rows)}), 202

    @app.get("/api/stream")
    def api_stream():
        log_id = request.args.get("log") or None
        subscriber = hub.subscribe(log_id, int(request.args.get("after", 0)))

        @stream_with_context
        def generate():
            try:
                while True:
                    try:
                        message = subscriber.get(timeout=20)
                    except queue.Empty:
                        yield ": ping\n\n"
                        continue
                    yield f"data: {json.dumps(message)}\n\n"
            finally:
                hub.unsubscribe(subscriber)

        return Response(
            generate(),
            mimetype="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    return app
