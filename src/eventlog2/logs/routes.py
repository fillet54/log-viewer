from __future__ import annotations

from flask import (
    Blueprint,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

from ..standalone import build_page_data_script
from .registry import LogTypeRegistry
from .store import LogStore

_PER_PAGE = 20


def create_logs_blueprint(registry: LogTypeRegistry, store: LogStore) -> Blueprint:
    bp = Blueprint("logs", __name__)

    @bp.route("/logs")
    def index():
        entries = []
        for log_type in registry.all():
            entries.append({"log_type": log_type, "count": store.count(log_type.full_id)})
        return render_template("logs_index.html", entries=entries)

    @bp.route("/logs/<path:log_type_id>")
    def list_logs(log_type_id: str):
        log_type = registry.get(log_type_id)
        if not log_type:
            return "Log type not found", 404
        page = max(1, int(request.args.get("page", 1)))
        search = request.args.get("q", "").strip()
        store_records, _ = store.list_records(log_type_id, page=1, per_page=99999, search=search)
        extra = log_type.extra_records(search=search)
        all_records = sorted(
            store_records + extra,
            key=lambda r: r.imported_at,
            reverse=True,
        )
        total = len(all_records)
        start = (page - 1) * _PER_PAGE
        records = all_records[start : start + _PER_PAGE]
        pages = max(1, (total + _PER_PAGE - 1) // _PER_PAGE)
        columns = log_type.get_list_columns()
        rows = [(r, log_type.format_list_row(r)) for r in records]
        return render_template(
            "logs_list.html",
            log_type=log_type,
            rows=rows,
            columns=columns,
            page=page,
            pages=pages,
            total=total,
            search=search,
        )

    @bp.route("/logs/<path:log_type_id>/import", methods=["GET", "POST"])
    def import_log(log_type_id: str):
        log_type = registry.get(log_type_id)
        if not log_type:
            return "Log type not found", 404

        if request.method == "GET":
            return render_template("logs_import.html", log_type=log_type, error=None)

        is_api = request.is_json or request.headers.get("Accept") == "application/json"
        try:
            file = request.files.get("file") if request.files else None
            json_data: dict | None = None
            if not (file and file.filename):
                file = None
                json_data = request.get_json(silent=True)
                if json_data is None:
                    raise ValueError("Provide a file upload or a JSON body.")

            name, payload = log_type.parse_import(file=file, json_data=json_data)

            name_override = (request.form.get("name") or "").strip()
            if not name_override and json_data:
                name_override = str(json_data.get("name") or "").strip()
            if name_override:
                name = name_override

            metadata = log_type.extract_metadata(payload)
            record = store.store(log_type.full_id, log_type.plugin_id, name, metadata, payload)

            if is_api:
                return jsonify({"id": record.id, "name": record.name, "log_type_id": log_type_id})
            return redirect(url_for("logs.view_log", log_type_id=log_type_id, log_id=record.id))

        except ValueError as exc:
            if is_api:
                return jsonify({"error": str(exc)}), 400
            return render_template("logs_import.html", log_type=log_type, error=str(exc))

    @bp.route("/logs/<path:log_type_id>/<log_id>")
    def view_log(log_type_id: str, log_id: str):
        log_type = registry.get(log_type_id)
        if not log_type:
            return "Log type not found", 404
        record = store.get_record(log_type_id, log_id)
        payload = store.get_payload(log_type_id, log_id) if record else None
        if record is None or payload is None:
            # Try secondary sources (e.g. live-session records)
            extra = {r.id: r for r in log_type.extra_records()}
            record = extra.get(log_id)
            if not record:
                return "Log not found", 404
            payload = log_type.get_extra_payload(log_id)
            if payload is None:
                return "Log data missing", 404
        page_data = log_type.build_view_page_data(record, payload)
        script = build_page_data_script(page_data)
        return render_template(
            "log_view.html",
            record=record,
            log_type=log_type,
            page_data_script=script,
        )

    @bp.route("/logs/<path:log_type_id>/<log_id>/delete", methods=["POST"])
    def delete_log(log_type_id: str, log_id: str):
        # Only delete records owned by the log store; extra records (e.g. live
        # sessions) are managed by their own storage layer.
        if store.get_record(log_type_id, log_id):
            store.delete(log_type_id, log_id)
        return redirect(url_for("logs.list_logs", log_type_id=log_type_id))

    return bp
