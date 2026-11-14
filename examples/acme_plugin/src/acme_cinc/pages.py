"""`/acme/`: a plugin page inside `cinc serve`, using Cinc's page frame."""

from __future__ import annotations

from collections import Counter

from flask import Blueprint, current_app, redirect, render_template, url_for

from cinc.logdir import list_logs

from .generate import owner_of, write_demo_log

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
