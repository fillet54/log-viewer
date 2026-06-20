from __future__ import annotations

import json
import queue

from flask import Blueprint, Response, redirect, render_template, stream_with_context, url_for

from ..standalone import build_page_data_script
from .monitor import SessionManager
from .storage import SessionStore


def create_live_blueprint(manager: SessionManager, store: SessionStore) -> Blueprint:
    bp = Blueprint("live", __name__)

    @bp.route("/live")
    def sessions():
        all_sessions = store.list_sessions()
        active_id = manager.active_session_id
        return render_template("live_sessions.html", sessions=all_sessions, active_session_id=active_id)

    @bp.route("/live/start", methods=["POST"])
    def start():
        try:
            session_id = manager.start()
        except RuntimeError as exc:
            all_sessions = store.list_sessions()
            return render_template(
                "live_sessions.html",
                sessions=all_sessions,
                active_session_id=None,
                error=str(exc),
            )
        return redirect(url_for("live.session_view", session_id=session_id))

    @bp.route("/live/stop", methods=["POST"])
    def stop():
        manager.stop()
        return redirect(url_for("live.sessions"))

    @bp.route("/live/<session_id>")
    def session_view(session_id):
        meta = store.get_meta(session_id)
        if not meta:
            return "Session not found", 404
        page_data = manager.get_session_page_data(session_id) or {}
        is_live = manager.active_session_id == session_id
        if is_live:
            page_data["live"] = {"sessionId": session_id}
        script = build_page_data_script(page_data)
        return render_template(
            "live_view.html",
            meta=meta,
            page_data_script=script,
            is_live=is_live,
        )

    @bp.route("/live/<session_id>/stream")
    def event_stream(session_id):
        if manager.active_session_id != session_id:
            return "Not an active session", 404

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
