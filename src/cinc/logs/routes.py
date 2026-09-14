from __future__ import annotations

import json
from flask import Blueprint, jsonify, redirect, render_template, request, url_for

from cinc.core.bundle import resolve_sections
from cinc.standalone import build_page_data_script


def create_logs_blueprint(registry, store):
    bp = Blueprint("logs", __name__)

    @bp.get("/logs")
    def index():
        return render_template(
            "logs_index.html",
            entries=[
                {"log_type": item, "count": store.count(item.id)}
                for item in registry.all()
            ],
        )

    @bp.get("/logs/<log_type_id>")
    def list_logs(log_type_id):
        log_type = registry.get(log_type_id)
        if log_type is None:
            return "Log type not found", 404
        rows, total = store.list_records(log_type_id)
        return render_template(
            "logs_list.html",
            log_type=log_type,
            rows=[(row, {}) for row in rows],
            columns=[],
            page=1,
            pages=1,
            total=total,
            search="",
        )

    @bp.post("/logs/import")
    def import_logs():
        files = [item for item in request.files.getlist("file") if item.filename]
        if files:
            inputs = [(item.filename, json.load(item)) for item in files]
        else:
            body = request.get_json(silent=True)
            if not isinstance(body, dict):
                return (
                    jsonify({"error": "Provide at least one file or a JSON body."}),
                    400,
                )
            inputs = [(str(body.get("source") or "json import"), body)]
        sections = []
        try:
            for name, payload in inputs:
                sections.extend(resolve_sections(payload, registry, path=name))
            page_data = __import__(
                "cinc.core.assemble", fromlist=["assemble_page_data"]
            ).assemble_page_data(sections)
        except (ValueError, json.JSONDecodeError) as exc:
            return jsonify({"error": str(exc)}), 400
        primary = sections[0][0]
        record = store.create_log(
            primary.id,
            "",
            str(request.form.get("name") or request.args.get("name") or "Imported log"),
            {"payload_header": {"apiVersion": page_data.get("apiVersion", 2)}},
        )
        for log_type, _ in sections[1:]:
            store.add_record_type(record.id, log_type.id)
        store.append_events(record.id, page_data["logData"]["events"])
        return (
            jsonify(
                {
                    "recordId": record.id,
                    "pageData": page_data,
                    "viewUrl": url_for(
                        "logs.view_log", log_type_id=primary.id, log_id=record.id
                    ),
                }
            ),
            201,
        )

    @bp.get("/logs/<log_type_id>/<log_id>")
    def view_log(log_type_id, log_id):
        log_type = registry.get(log_type_id)
        record = store.get_record(log_type_id, log_id) if log_type else None
        if record is None:
            return "Log not found", 404
        payload = log_type.build_payload_from_events(
            record, store.get_events(record.id)
        )
        return render_template(
            "log_view.html",
            record=record,
            log_type=log_type,
            page_data_script=build_page_data_script(log_type.build_page_data(payload)),
        )

    @bp.post("/logs/<log_type_id>/<log_id>/delete")
    def delete_log(log_type_id, log_id):
        store.delete(log_id)
        return redirect(url_for("logs.list_logs", log_type_id=log_type_id))

    return bp
