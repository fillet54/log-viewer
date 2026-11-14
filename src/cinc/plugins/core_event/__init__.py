"""The core_event log type: system events with SET/CLEAR pairing."""

from .config import DEFAULT_SEVERITIES, TONES
from .log_type import CoreEventLogType
from .rows import event

__all__ = ["CoreEventLogType", "DEFAULT_SEVERITIES", "TONES", "event", "setup"]


def setup(ctx):
    ctx.add_log_type(CoreEventLogType)
