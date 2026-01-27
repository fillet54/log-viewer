from datetime import datetime, timedelta
import os
from typing import Any, Optional, Dict
from flask import (
    Flask,
    Response,
    flash,
    g,
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
    create_shard,
    delete_shard,
    get_boot_details,
    get_boot_meta,
    get_db,
    get_dataset,
    get_dataset_by_name,
    get_first_shard,
    get_or_create_user,
    get_shard,
    get_shards_for_dataset,
    get_user,
    init_db,
    insert_events_into_shard,
    issue_login_token,
    list_boots_for_shard,
    list_datasets,
    load_log_data_from_shard,
    parse_events_from_upload,
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
app.config["SHARD_ROOT"] = os.environ.get(
    "LOG_VIEWER_SHARDS", os.path.join(app.root_path, "data", "shards")
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
    shard_id = request.args.get("shard", type=int)
    dataset_id = request.args.get("dataset", type=int)
    shard = None
    if shard_id:
        shard = get_shard(shard_id)
    elif dataset_id:
        shards = get_shards_for_dataset(dataset_id)
        shard = shards[0] if shards else None
    else:
        shard = get_first_shard()

    boot_id = request.args.get("boot")
    log_data = None
    if shard:
        log_data = load_log_data_from_shard(shard, boot_id)

    if not log_data:
        hours = request.args.get("hours", default="4")
        seed = request.args.get("seed")
        try:
            hours_value = max(0.25, min(48.0, float(hours)))
        except ValueError:
            hours_value = 4.0
        log_data = generate_logs(hours_value, seed)
        shard = None

    datasets = list_datasets()
    shards_for_dataset = get_shards_for_dataset(shard["dataset_id"]) if shard else []
    boots_for_shard = list_boots_for_shard(shard["id"]) if shard else []

    return render_template(
        "index.html",
        log_data=log_data,
        current_shard=shard,
        datasets=datasets,
        dataset_shards=shards_for_dataset,
        boots=boots_for_shard,
    )


@app.route("/app.js")
def app_js():
    return Response(render_template("app.js"), mimetype="application/javascript")


@app.route("/upload", methods=["GET", "POST"])
def upload_logs():
    datasets = list_datasets()
    selected_dataset_id = request.form.get("dataset_id", type=int)
    if request.method == "POST":
        dataset_name_new = (request.form.get("dataset_name_new") or "").strip()
        shard_name_new = (request.form.get("shard_name_new") or "").strip()
        shard_id = request.form.get("shard_id", type=int)
        file = request.files.get("log_file")
        gen_hours = request.form.get("hours", type=float)
        gen_seed = request.form.get("seed")

        dataset_obj = None
        if selected_dataset_id:
            dataset_obj = get_dataset(selected_dataset_id)
        if not dataset_obj and dataset_name_new:
            dataset_obj = get_dataset_by_name(dataset_name_new) or create_dataset(dataset_name_new)
        if not dataset_obj:
            flash("Select an existing dataset or provide a new dataset name.")
            return redirect(url_for("upload_logs"))

        shard_obj = None
        if shard_id:
            shard_obj = get_shard(shard_id)
        if (not shard_obj or shard_obj["dataset_id"] != dataset_obj["id"]) and shard_name_new:
            shard_obj = create_shard(dataset_obj["id"], shard_name_new)
        if not shard_obj or shard_obj["dataset_id"] != dataset_obj["id"]:
            flash("Select an existing shard or create a new shard for the chosen dataset.")
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

        insert_events_into_shard(shard_obj, events)
        flash(f"Imported {len(events)} events into shard '{shard_obj['name']}'.")
        return redirect(url_for("index", shard=shard_obj["id"]))

    shards_map = {d["id"]: get_shards_for_dataset(d["id"]) for d in datasets}
    return render_template(
        "upload.html",
        datasets=datasets,
        shards_map=shards_map,
        selected_dataset_id=selected_dataset_id,
    )


@app.route("/logs")
def logs_index():
    datasets = list_datasets()
    dataset_id = request.args.get("dataset_id", type=int)
    db = get_db()
    params = []
    where_clause = ""
    if dataset_id:
        where_clause = "WHERE shards.dataset_id = ?"
        params.append(dataset_id)
    rows = db.execute(
        f"""
        SELECT boots.boot_id, boots.event_count, boots.created_at,
               shards.id AS shard_id, shards.name AS shard_name, shards.dataset_id,
               datasets.name AS dataset_name
        FROM boots
        JOIN shards ON shards.id = boots.shard_id
        JOIN datasets ON datasets.id = shards.dataset_id
        {where_clause}
        ORDER BY datasets.name, shards.name, datetime(boots.created_at) DESC
    """,
        tuple(params),
    ).fetchall()

    boots = [
        {
            "boot_id": r["boot_id"],
            "event_count": r["event_count"],
            "created_at": r["created_at"],
            "shard_id": r["shard_id"],
            "shard_name": r["shard_name"],
            "dataset_id": r["dataset_id"],
            "dataset_name": r["dataset_name"],
        }
        for r in rows
    ]

    boot_meta_map = {}
    for boot in boots:
        shard = get_shard(boot["shard_id"])
        details = get_boot_details(shard, boot["boot_id"]) if shard else {}
        boot_meta_map[boot["boot_id"]] = {
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


@app.route("/shards/<int:shard_id>/delete", methods=["POST"])
def delete_shard_route(shard_id: int):
    delete_shard(shard_id)
    flash("Shard deleted.")
    return redirect(url_for("logs_index"))


@app.route("/boots/<int:shard_id>/<boot_id>/edit", methods=["GET", "POST"])
def edit_boot(shard_id: int, boot_id: str):
    shard = get_shard(shard_id)
    if not shard:
        flash("Shard not found.")
        return redirect(url_for("logs_index"))
    boot = get_boot_meta(shard_id, boot_id)
    if not boot:
        flash("Boot not found.")
        return redirect(url_for("logs_index"))

    details = get_boot_details(shard, boot_id)
    system_val = details.get("system", "") if details else ""
    event_id_val = details.get("event_id", "") if details else ""
    tags_val = details.get("tags", "") if details else ""

    if request.method == "POST":
        system = (request.form.get("system") or "").strip()
        event_id = (request.form.get("event_id") or "").strip()
        tags_raw = request.form.get("tags") or ""
        tags = [t.strip() for t in tags_raw.split(",") if t.strip()]
        update_boot_metadata(shard, boot_id, system, event_id, tags)
        flash("Boot metadata updated.")
        return redirect(url_for("logs_index"))

    return render_template(
        "boot_edit.html",
        shard=shard,
        boot=boot,
        system=system_val,
        event_id=event_id_val,
        tags=tags_val,
    )


if __name__ == "__main__":
    app.run(debug=True, port=8080)
