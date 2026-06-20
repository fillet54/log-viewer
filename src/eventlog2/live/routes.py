from __future__ import annotations

import json
import queue

from flask import Blueprint, Response, redirect, render_template, stream_with_context, url_for

from .monitor import SessionManager


def create_live_blueprint(manager: SessionManager) -> Blueprint:
    bp = Blueprint("live", __name__)

    @bp.route("/live")
    def sessions():
        return render_template("live_sessions.html", active_session_id=manager.active_session_id)

    @bp.route("/live/start", methods=["POST"])
    def start():
        try:
            log_id = manager.start()
        except RuntimeError as exc:
            return render_template(
                "live_sessions.html",
                active_session_id=manager.active_session_id,
                error=str(exc),
            )
        return redirect(url_for("logs.view_log", log_type_id=manager.log_type_id, log_id=log_id))

    @bp.route("/live/stop", methods=["POST"])
    def stop():
        active_id = manager.active_session_id
        manager.stop()
        if active_id:
            return redirect(url_for("logs.view_log", log_type_id=manager.log_type_id, log_id=active_id))
        return redirect(url_for("live.sessions"))

    @bp.route("/live/<session_id>")
    def session_view(session_id):
        return redirect(url_for("logs.view_log", log_type_id=manager.log_type_id, log_id=session_id))

    @bp.route("/live/<session_id>/stream")
    def event_stream(session_id):
        if manager.active_session_id != session_id:
            return "Not an active live capture", 404

        q = manager.subscribe()

        @stream_with_context
        def generate():
            try:
                while True:
                    try:
                        payload = q.get(timeout=25)
                    except queue.Empty:
                        yield "data: {\"type\":\"ping\"}\n\n"
                        continue
                    if payload is None:
                        yield "data: {\"type\":\"done\"}\n\n"
                        return
                    yield f"data: {json.dumps(payload)}\n\n"
            finally:
                manager.unsubscribe(q)

        return Response(
            generate(),
            mimetype="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    return bp
