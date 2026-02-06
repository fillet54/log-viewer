from datetime import datetime, timedelta
import os
from typing import Any, Optional, Dict
from flask import (
    Flask,
    Response,
    flash,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from log_generator import generate_logs
from storage import (
    close_db,
    consume_login_token,
    create_dataset,
    delete_dataset,
    get_boot_details,
    get_boot_meta,
    get_dataset,
    get_dataset_by_name,
    get_or_create_user,
    get_user,
    get_first_dataset,
    init_db,
    insert_events_into_dataset,
    issue_login_token,
    list_bookmarks_for_user,
    list_comments_for_boot,
    list_boots_for_dataset,
    list_datasets,
    load_log_data_from_dataset,
    parse_events_from_upload,
    create_comment,
    set_bookmark,
    update_boot_metadata,
    update_user_last_seen,
    update_user_name,
)

app = Flask(__name__)
app.secret_key = os.environ.get("LOG_VIEWER_SECRET", "dev-secret-key-change-me")
app.permanent_session_lifetime = timedelta(days=30)
app.config["DATABASE"] = os.environ.get(
    "LOG_VIEWER_DB", os.path.join(app.root_path, "log_viewer.db")
)
app.config["DATASET_ROOT"] = os.environ.get(
    "LOG_VIEWER_DATASETS", os.path.join(app.root_path, "data", "datasets")
)


def login_user(user_id: int) -> None:
    session.clear()
    session.permanent = True
    now = datetime.utcnow().isoformat()
    session["user_id"] = user_id
    session["last_seen"] = now
    update_user_last_seen(user_id, datetime.fromisoformat(now))


@app.teardown_appcontext
def teardown_db(error: Optional[BaseException] = None) -> None:
    close_db(error)


@app.context_processor
def inject_user() -> Dict[str, Any]:
    return {"current_user": getattr(g, "current_user", None)}


@app.before_request
def load_user() -> None:
    init_db()
    g.current_user = None
    user_id = session.get("user_id")
    last_seen_str = session.get("last_seen")
    if not user_id:
        return
    now = datetime.utcnow()
    last_seen = None
    try:
        last_seen = datetime.fromisoformat(last_seen_str) if last_seen_str else None
    except (TypeError, ValueError):
        last_seen = None

    if last_seen and now - last_seen > timedelta(days=30):
        session.clear()
        return

    user = get_user(int(user_id))
    if not user:
        session.clear()
        return

    g.current_user = user
    needs_refresh = last_seen is None or now - last_seen >= timedelta(hours=12)
    if needs_refresh:
        session["last_seen"] = now.isoformat()
        session.modified = True
        update_user_last_seen(user["id"], now)


def _require_login_redirect():
    if g.current_user:
        return None
    flash("Please log in to continue.")
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if g.current_user:
        return redirect(url_for("index"))
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        if not email:
            flash("Email is required.")
            return redirect(url_for("login"))
        user = get_or_create_user(email)
        token = issue_login_token(user["id"])
        login_link = url_for("consume_token_route", token=token, _external=True)
        # TODO: send email with login_link; kept commented for now while SMTP is unavailable.
        # send_login_email(email, login_link)
        flash("Check your email for the magic link. Redirecting for development.")
        return redirect(login_link)
    return render_template("login.html")


@app.route("/auth/<token>")
def consume_token_route(token: str):
    user_id = consume_login_token(token)
    if not user_id:
        flash("That login link is invalid or expired. Request a new one.")
        return redirect(url_for("login"))
    login_user(user_id)
    user = get_user(user_id)
    if user and not user.get("name"):
        return redirect(url_for("profile", first_login=1))
    return redirect(url_for("index"))


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    flash("You have been logged out.")
    return redirect(url_for("index"))


@app.route("/profile", methods=["GET", "POST"])
def profile():
    redirect_response = _require_login_redirect()
    if redirect_response:
        return redirect_response
    user = g.current_user
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        update_user_name(user["id"], name)
        g.current_user["name"] = name
        flash("Profile updated.")
        return redirect(url_for("profile"))
    return render_template("profile.html", user=user, first_login=request.args.get("first_login"))


@app.route("/")
def index():
    current_user_id = g.current_user["id"] if g.current_user else None
    dataset_id = request.args.get("dataset", type=int)
    dataset = None
    if dataset_id:
        dataset = get_dataset(dataset_id)
    else:
        dataset = get_first_dataset(current_user_id)

    boot_id = request.args.get("boot")
    log_data = None
    if dataset:
        log_data = load_log_data_from_dataset(dataset, boot_id)

    if not log_data:
        hours = request.args.get("hours", default="4")
        seed = request.args.get("seed")
        try:
            hours_value = max(0.25, min(48.0, float(hours)))
        except ValueError:
            hours_value = 4.0
        log_data = generate_logs(hours_value, seed)
        dataset = None

    datasets = list_datasets(current_user_id)
    boots_for_dataset = list_boots_for_dataset(dataset["id"]) if dataset else []

    return render_template(
        "index.html",
        log_data=log_data,
        current_dataset=dataset,
        datasets=datasets,
        boots=boots_for_dataset,
    )


@app.route("/app.js")
def app_js():
    return Response(render_template("app.js"), mimetype="application/javascript")


@app.route("/upload", methods=["GET", "POST"])
def upload_logs():
    current_user_id = g.current_user["id"] if g.current_user else None
    datasets = list_datasets(current_user_id)
    selected_dataset_id = request.form.get("dataset_id", type=int)
    if request.method == "POST":
        dataset_name_new = (request.form.get("dataset_name_new") or "").strip()
        dataset_description_new = (request.form.get("dataset_description_new") or "").strip()
        is_personal = bool(request.form.get("dataset_is_personal"))
        file = request.files.get("log_file")
        gen_hours = request.form.get("hours", type=float)
        gen_seed = request.form.get("seed")

        dataset_obj = None
        if selected_dataset_id:
            dataset_obj = get_dataset(selected_dataset_id)
        if not dataset_obj and dataset_name_new:
            owner_user_id = current_user_id if is_personal else None
            if is_personal and not current_user_id:
                flash("Log in to create a personal dataset.")
                return redirect(url_for("login"))
            dataset_obj = get_dataset_by_name(dataset_name_new, owner_user_id) or create_dataset(
                dataset_name_new, dataset_description_new, owner_user_id
            )
        if not dataset_obj:
            flash("Select an existing dataset or provide a new dataset name.")
            return redirect(url_for("upload_logs"))

        events = []
        if file and file.filename:
            events = parse_events_from_upload(file)
            if not events:
                flash("No events found in upload. Ensure JSON is an array or has an 'events' list.")
                return redirect(url_for("upload_logs"))
        else:
            try:
                hours_value = max(0.25, min(48.0, float(gen_hours))) if gen_hours else 4.0
            except (TypeError, ValueError):
                hours_value = 4.0
            generated = generate_logs(hours_value, gen_seed or None)
            events = generated.get("events", [])
            for ev in events:
                ev.setdefault("event_id", "")
                ev.setdefault("tags", [])

        insert_events_into_dataset(dataset_obj, events)
        flash(f"Imported {len(events)} events into dataset '{dataset_obj['name']}'.")
        return redirect(url_for("index", dataset=dataset_obj["id"]))

    return render_template(
        "upload.html",
        datasets=datasets,
        selected_dataset_id=selected_dataset_id,
        current_user_id=current_user_id,
    )


@app.route("/logs")
def logs_index():
    current_user_id = g.current_user["id"] if g.current_user else None
    datasets = list_datasets(current_user_id)
    dataset_id = request.args.get("dataset_id", type=int)
    if dataset_id:
        datasets = [d for d in datasets if d["id"] == dataset_id]

    boots = []
    for dataset in datasets:
        for boot in list_boots_for_dataset(dataset["id"]):
            boots.append(
                {
                    "boot_id": boot["boot_id"],
                    "event_count": boot["event_count"],
                    "created_at": boot["created_at"],
                    "dataset_id": dataset["id"],
                    "dataset_name": dataset["name"],
                }
            )
    boots.sort(key=lambda b: b["created_at"], reverse=True)
    boots.sort(key=lambda b: b["dataset_name"].lower())

    boot_meta_map = {}
    for boot in boots:
        dataset = get_dataset(boot["dataset_id"])
        details = get_boot_details(dataset, boot["boot_id"]) if dataset else {}
        meta_key = f"{boot['dataset_id']}:{boot['boot_id']}"
        boot["meta_key"] = meta_key
        boot_meta_map[meta_key] = {
            "system": details.get("system", "") if details else "",
            "event_id": details.get("event_id", "") if details else "",
            "tags": details.get("tags", "") if details else "",
        }

    return render_template(
        "logs.html",
        datasets=datasets,
        boots=boots,
        selected_dataset_id=dataset_id,
        boot_meta_map=boot_meta_map,
    )


@app.route("/datasets/<int:dataset_id>/delete", methods=["POST"])
def delete_dataset_route(dataset_id: int):
    redirect_response = _require_login_redirect()
    if redirect_response:
        return redirect_response
    dataset = get_dataset(dataset_id)
    if not dataset:
        flash("Dataset not found.")
        return redirect(url_for("logs_index"))
    if dataset.get("owner_user_id") != g.current_user["id"]:
        flash("Only the owning user can delete this dataset.")
        return redirect(url_for("logs_index"))
    delete_dataset(dataset_id)
    flash("Dataset deleted.")
    return redirect(url_for("logs_index"))


@app.route("/boots/<int:dataset_id>/<boot_id>/edit", methods=["GET", "POST"])
def edit_boot(dataset_id: int, boot_id: str):
    dataset = get_dataset(dataset_id)
    if not dataset:
        flash("Dataset not found.")
        return redirect(url_for("logs_index"))
    boot = get_boot_meta(dataset_id, boot_id)
    if not boot:
        flash("Boot not found.")
        return redirect(url_for("logs_index"))

    details = get_boot_details(dataset, boot_id)
    system_val = details.get("system", "") if details else ""
    event_id_val = details.get("event_id", "") if details else ""
    tags_val = details.get("tags", "") if details else ""

    if request.method == "POST":
        system = (request.form.get("system") or "").strip()
        event_id = (request.form.get("event_id") or "").strip()
        tags_raw = request.form.get("tags") or ""
        tags = [t.strip() for t in tags_raw.split(",") if t.strip()]
        update_boot_metadata(dataset, boot_id, system, event_id, tags)
        flash("Boot metadata updated.")
        return redirect(url_for("logs_index"))

    return render_template(
        "boot_edit.html",
        dataset=dataset,
        boot=boot,
        system=system_val,
        event_id=event_id_val,
        tags=tags_val,
    )


@app.route("/api/bookmarks", methods=["GET", "POST"])
def bookmarks_api():
    if not g.current_user:
        return jsonify({"error": "login_required"}), 401
    user_id = g.current_user["id"]
    if request.method == "GET":
        dataset_id = request.args.get("dataset_id", type=int)
        boot_id = request.args.get("boot_id", type=str)
        if not dataset_id or not boot_id:
            return jsonify({"error": "missing_params"}), 400
        bookmarks = list_bookmarks_for_user(user_id, dataset_id, boot_id)
        return jsonify({"bookmarks": bookmarks})

    payload = request.get_json(silent=True) or {}
    dataset_id = payload.get("dataset_id")
    boot_id = payload.get("boot_id")
    row_id = payload.get("row_id")
    color_index = payload.get("color_index")
    if not dataset_id or not boot_id or row_id is None or color_index is None:
        return jsonify({"error": "missing_params"}), 400
    try:
        dataset_id = int(dataset_id)
        row_id = int(row_id)
        color_index = int(color_index)
    except (TypeError, ValueError):
        return jsonify({"error": "invalid_params"}), 400
    set_bookmark(user_id, dataset_id, str(boot_id), row_id, color_index)
    return jsonify({"row_id": row_id, "color_index": color_index})


@app.route("/api/comments", methods=["GET", "POST"])
def comments_api():
    if request.method == "GET":
        dataset_id = request.args.get("dataset_id", type=int)
        boot_id = request.args.get("boot_id", type=str)
        if not dataset_id or not boot_id:
            return jsonify({"error": "missing_params"}), 400
        comments = list_comments_for_boot(dataset_id, boot_id)
        return jsonify({"comments": comments})

    if not g.current_user:
        return jsonify({"error": "login_required"}), 401
    payload = request.get_json(silent=True) or {}
    dataset_id = payload.get("dataset_id")
    boot_id = payload.get("boot_id")
    row_id = payload.get("row_id")
    body = (payload.get("body") or "").strip()
    parent_id = payload.get("parent_id")
    if not dataset_id or not boot_id or row_id is None or not body:
        return jsonify({"error": "missing_params"}), 400
    try:
        dataset_id = int(dataset_id)
        row_id = int(row_id)
        parent_id = int(parent_id) if parent_id is not None else None
    except (TypeError, ValueError):
        return jsonify({"error": "invalid_params"}), 400
    if len(body) > 2000:
        return jsonify({"error": "body_too_long"}), 400
    try:
        comment = create_comment(
            g.current_user["id"], dataset_id, str(boot_id), row_id, body, parent_id
        )
    except ValueError:
        return jsonify({"error": "invalid_parent"}), 400
    return jsonify({"comment": comment})


if __name__ == "__main__":
    app.run(debug=True, port=8080)
