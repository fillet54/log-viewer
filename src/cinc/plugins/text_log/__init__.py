"""The text_log log type: plain timestamped application log lines."""

from .log_type import TextLogType

__all__ = ["TextLogType", "setup"]


def setup(ctx):
    ctx.add_log_type(TextLogType)
