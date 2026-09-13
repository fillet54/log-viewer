from __future__ import annotations

import json
import queue

from flask import Blueprint, Response, jsonify, render_template, stream_with_context


def create_live_blueprint(live, registry):
    bp = Blueprint("live", __name__)

    @bp.get("/live")
    def sessions():
        return render_template("live_sessions.html", live=live, registry=registry)

    @bp.post("/live/<log_type_id>/start")
    def start(log_type_id):
        manager = live.get(log_type_id)
        if manager is None:
            return jsonify({"error": "Live capture is unavailable"}), 404
        try:
            return jsonify({"sessionId": manager.start()})
        except RuntimeError as exc:
            return jsonify({"error": str(exc)}), 409

    @bp.post("/live/<log_type_id>/stop")
    def stop(log_type_id):
        manager = live.get(log_type_id)
        if manager is None:
            return jsonify({"error": "Live capture is unavailable"}), 404
        manager.stop()
        return jsonify({"ok": True})

    @bp.get("/live/<session_id>/stream")
    def event_stream(session_id):
        manager = next(
            (
                item
                for item in live.managers().values()
                if item.active_session_id == session_id
            ),
            None,
        )
        if manager is None:
            return "Not an active live capture", 404
        subscriber = manager.subscribe()

        @stream_with_context
        def generate():
            try:
                while True:
                    try:
                        payload = subscriber.get(timeout=25)
                    except queue.Empty:
                        yield 'data: {"type":"ping"}\n\n'
                        continue
                    if payload is None:
                        yield 'data: {"type":"done"}\n\n'
                        return
                    yield f"data: {json.dumps(payload)}\n\n"
            finally:
                manager.unsubscribe(subscriber)

        return Response(
            generate(),
            mimetype="text/event-stream",
            headers={"Cache-Control": "no-cache"},
        )

    return bp
