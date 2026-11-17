"""Cinc: a log viewer you can extend with plugins.

The public Python API (see docs/plugins.md)::

    from cinc import LogType, PluginContext, Registry          # plugins
    from cinc import LogWriter, LiveClient                       # producing logs
    from cinc import build_standalone, write_standalone          # standalone pages
    from cinc import build_tracks, track_sample, API_VERSION
    from cinc import NeedsRebuild                                # live streams
    from cinc.testing import assert_stream_matches               # plugin tests
    from cinc.plugins.core_event import event                    # core_event rows

Names load on first use, so ``import cinc`` stays cheap.
"""

__all__ = [
    "API_VERSION",
    "LiveClient",
    "LogType",
    "LogWriter",
    "NeedsRebuild",
    "PluginContext",
    "Registry",
    "build_standalone",
    "build_tracks",
    "create_app",
    "track_sample",
    "write_standalone",
]

_EXPORTS = {
    "API_VERSION": ("cinc.core.assemble", "API_VERSION"),
    "LiveClient": ("cinc.writer", "LiveClient"),
    "LogType": ("cinc.core.log_type", "LogType"),
    "LogWriter": ("cinc.writer", "LogWriter"),
    "NeedsRebuild": ("cinc.core.log_type", "NeedsRebuild"),
    "PluginContext": ("cinc.core.plugin", "PluginContext"),
    "Registry": ("cinc.core.registry", "Registry"),
    "build_standalone": ("cinc.standalone", "build_standalone"),
    "build_tracks": ("cinc.core.track", "build_tracks"),
    "create_app": ("cinc.app", "create_app"),
    "track_sample": ("cinc.core.track", "sample"),
    "write_standalone": ("cinc.standalone", "write_standalone"),
}


def __getattr__(name: str):
    if name in _EXPORTS:
        from importlib import import_module

        module, attribute = _EXPORTS[name]
        return getattr(import_module(module), attribute)
    raise AttributeError(name)
