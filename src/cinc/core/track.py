"""Track data: positions over time, drawn by the core Map chart.

Any log type can supply tracks by overriding ``LogType.tracks``. The built-in
``track`` log type does nothing else: its rows are position samples and it adds
no rows to the log list. A sample row is::

    {"time": "2026-01-01T08:00:05Z", "track": "UAV-1", "lat": 34.1, "lon": -117.2,
     "alt": 1200}

``track`` defaults to ``"track"`` and ``alt`` (metres above the ellipsoid) to 0.
Longitudes are stored in [-180, 180). The viewer moves between samples at
constant heading and speed (a rhumb line), so a sample per heading or speed
change is enough.
The type's header (``track.json``) may name and colour tracks::

    {"tracks": {"UAV-1": {"label": "Survey drone", "color": "#d1495b"}}}

A row's own ``label`` / ``color`` work too; the header wins.

Tracks reach the viewer as ``logData.tracks``::

    [{"id": "UAV-1", "label": "Survey drone", "color": "#d1495b",
      "samples": [[time_ms, lat, lon, alt], ...]}]

with samples sorted by time.
"""

from __future__ import annotations

import math
from typing import Any

from .log_type import LogType
from .times import iso_time, parse_time

_DEFAULT_TRACK = "track"


def time_ms(value: Any) -> float:
    """Epoch milliseconds for an ISO time; a time without a zone is UTC."""
    if not isinstance(value, str):
        raise ValueError(f"invalid time {value!r}")
    return parse_time(value).timestamp() * 1000


def sample(
    time: Any,
    lat: float,
    lon: float,
    alt: float = 0.0,
    *,
    track: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """One ``track`` row. ``time`` is an ISO string or a datetime."""
    row: dict[str, Any] = {"time": iso_time(time), "lat": lat, "lon": lon, "alt": alt}
    if track is not None:
        row["track"] = track
    row.update(extra)
    return row


def _number(row: dict[str, Any], key: str, default: float | None = None) -> float:
    value = row.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{key} must be a number")
    if not math.isfinite(value):
        raise ValueError(f"{key} must be finite")
    return float(value)


def build_tracks(
    rows: list[dict[str, Any]], header: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    """Group sample rows into tracks. Raises ValueError on a bad row."""
    meta = (header or {}).get("tracks") or {}
    if not isinstance(meta, dict):
        raise ValueError('header "tracks" must be an object of track id -> info')
    tracks: dict[str, dict[str, Any]] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"track row {index} is not an object")
        try:
            sample = [
                time_ms(row.get("time")),
                _number(row, "lat"),
                _number(row, "lon"),
                _number(row, "alt", 0.0),
            ]
            if not -90 <= sample[1] <= 90 or not -180 <= sample[2] <= 360:
                raise ValueError("lat/lon out of range")
            if sample[2] >= 180:
                sample[2] -= 360
        except ValueError as exc:
            raise ValueError(f"track row {index}: {exc}") from None
        track_id = str(row.get("track") or row.get("id") or _DEFAULT_TRACK)
        track = tracks.get(track_id)
        if track is None:
            info = meta.get(track_id) if isinstance(meta.get(track_id), dict) else {}
            track = tracks[track_id] = {
                "id": track_id,
                "label": str(info.get("label") or row.get("label") or track_id),
                "color": info.get("color") or row.get("color"),
                "samples": [],
            }
        track["samples"].append(sample)
    for track in tracks.values():
        track["samples"].sort(key=lambda sample: sample[0])
    return list(tracks.values())


def merge_tracks(groups: list[list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """Merge track lists from several log types; same id means same track."""
    merged: dict[str, dict[str, Any]] = {}
    for group in groups:
        for track in group:
            existing = merged.get(track["id"])
            if existing is None:
                merged[track["id"]] = {**track, "samples": list(track["samples"])}
                continue
            existing["samples"].extend(track["samples"])
            existing["samples"].sort(key=lambda sample: sample[0])
            existing["color"] = existing.get("color") or track.get("color")
    return list(merged.values())


class TrackLogType(LogType):
    """Position samples for the Map chart; adds no rows to the log list."""

    id = "track"
    name = "Track"
    description = "Positions over time (lat/lon/alt), drawn on the Map chart."
    rows_independent = True

    def normalize_events(self, payload, *, row_id_base=0, row_id_step=1):
        self.tracks(payload)  # reject bad rows here too
        return []

    def detect(self, payload):
        events = payload.get("events") if isinstance(payload, dict) else None
        if not isinstance(events, list) or not events:
            return 0.0
        first = events[0]
        if isinstance(first, dict) and {"time", "lat", "lon"} <= set(first):
            return 0.8
        return 0.0

    def tracks(self, payload):
        rows = payload.get("events") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            raise ValueError("track: payload must contain an events list")
        return build_tracks(rows, payload)

    def log_summary(self, payload):
        tracks = self.tracks(payload)
        return {"Tracks": str(len(tracks))}
