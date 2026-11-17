"""`/acme/`: a plugin page inside `cinc serve`, using Cinc's page frame, and
`/acme/api/`: a JSON endpoint the plugin's browser script calls on served pages."""

from __future__ import annotations

from collections import Counter

from flask import (
    Blueprint,
    abort,
    current_app,
    jsonify,
    redirect,
    render_template,
    url_for,
)

from cinc.logdir import list_logs

from .generate import TEAMS, owner_of, write_demo_log

blueprint = Blueprint("acme", __name__, url_prefix="/acme", template_folder="templates")
blueprint.nav_label = "Acme"  # shows the page in Cinc's top bar


def open_faults(log) -> Counter:
    """Faults still SET at the end of a log, counted per owning team."""
    open_keys = {}
    for row in log.rows("core_event"):
        key = (row.get("system"), row.get("code"))
        if row.get("set_clear") == "clear":
            open_keys.pop(key, None)
        else:
            open_keys[key] = owner_of(row.get("system"))
    return Counter(open_keys.values())


@blueprint.get("/")
def index():
    root = current_app.extensions["cinc"]["root"]
    logs = [
        {"log": log, "meta": log.meta(), "open": open_faults(log)}
        for log in list_logs(root)
        if "core_event" in log.types()
    ]
    return render_template("acme/index.html", logs=logs)


@blueprint.post("/demo")
def demo():
    path = write_demo_log(current_app.extensions["cinc"]["root"])
    return redirect(url_for("view_log", log_id=path.name))


def find_log(log_id: str):
    for log in list_logs(current_app.extensions["cinc"]["root"]):
        if log.id == log_id:
            return log
    abort(404)


@blueprint.get("/api/logs/<log_id>/owners")
def owners(log_id):
    """Each team's contacts (server-side data a standalone page can't have) and
    its faults still set at the end of the log. Reads the folder on every call,
    so live logs stay current."""
    log = find_log(log_id)
    still_set = open_faults(log)
    return jsonify(
        {
            "logId": log.id,
            "owners": {
                team: {**contacts, "setAtEnd": still_set.get(team, 0)}
                for team, contacts in sorted(TEAMS.items())
            },
        }
    )
