"""Example Cinc plugin. ``setup`` is named by the ``cinc.plugins`` entry point."""

from .generate import OWNERS, owner_of, write_demo_log


def add_owner(events, context):
    """Augmenter: tag each core_event row with the team that owns its system."""
    return [{**event, "owner": owner_of(event.get("system"))} for event in events]


def setup(ctx):
    from .commands import demo_command
    from .pages import blueprint

    ctx.extend(
        "core_event",
        scripts=["static/owners.js"],
        styles=["static/owners.css"],
        config={"owners": sorted(set(OWNERS.values()))},
        augmenters={"owner": add_owner},
    )
    ctx.add_blueprint(blueprint)
    ctx.add_command("acme-demo", demo_command, "Write a demo log (and page) to DIR")


__all__ = ["setup", "write_demo_log"]
