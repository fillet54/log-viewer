"""Basemap for the Map chart: Natural Earth and aviation layers cut to the tracks' area.

The data lives in ``cinc/data/basemap`` (built by ``scripts/build_basemap.py``;
the airports, runways, navaids and airspace by ``scripts/build_aviation.py``).
Detailed 1:10m layers are cut to an area around the tracks; a 1:110m world and
the country names are always included, so zooming out still shows context.

The viewer gets::

    {"scale": 10000, "bounds": [west, south, east, north],
     "layers": {name: {"kind": "polygon" | "line" | "point", "items": [...]}}}

Polygon and line items are flat integer coordinate lists ``[x0, y0, x1, y1, ...]``
(degrees * scale); point items are ``[x, y, name, min_zoom]``, aviation points
add a fifth field, their kind.
"""

from __future__ import annotations

import gzip
import json
import math
from array import array
from functools import lru_cache
from importlib.resources import files
from typing import Any, Iterable

WORLD_LAYERS = ("world_land", "world_borders", "country_labels")
DETAIL_LAYERS = (
    "land",
    "glaciers",
    "urban",
    "lakes",
    "coastline",
    "rivers",
    "roads",
    "borders",
    "state_borders",
    "state_labels",
    "places",
    # aviation (scripts/build_aviation.py)
    "runways",
    "airspace_b",
    "airspace_c",
    "airspace_d",
    "airports",
    "navaids",
)
MIN_MARGIN_DEG = 1.5  # around the tracks, at least this much on each side
MARGIN_FRACTION = 0.5  # ... or this fraction of the tracks' extent
MAX_LAT = 85.0  # Web Mercator stops here

Box = tuple[int, int, int, int]


@lru_cache(maxsize=None)
def _layer(name: str) -> tuple[str, int, list]:
    """(kind, scale, items); polygon/line items are (bbox, array of absolute
    coordinates), point items are kept as stored."""
    raw = files("cinc").joinpath(f"data/basemap/{name}.json.gz").read_bytes()
    data = json.loads(gzip.decompress(raw))
    kind, scale = data["kind"], data["scale"]
    if kind == "point":
        return kind, scale, data["items"]
    items = []
    for item in data["items"]:
        coords = array("i", item[4:])
        for i in range(2, len(coords)):
            coords[i] += coords[i - 2]  # undo the delta encoding
        items.append((tuple(item[:4]), coords))
    return kind, scale, items


# -- clipping (integer coordinates, axis-aligned box) ---------------------------


def _clip_segment(ax, ay, bx, by, box: Box):
    """Liang-Barsky: the part of a-b inside the box, or None."""
    x0, y0, x1, y1 = box
    dx, dy = bx - ax, by - ay
    t0, t1 = 0.0, 1.0
    for p, q in ((-dx, ax - x0), (dx, x1 - ax), (-dy, ay - y0), (dy, y1 - ay)):
        if p == 0:
            if q < 0:
                return None
            continue
        t = q / p
        if p < 0:
            if t > t1:
                return None
            t0 = max(t0, t)
        else:
            if t < t0:
                return None
            t1 = min(t1, t)
    start = (ax, ay) if t0 == 0 else (round(ax + t0 * dx), round(ay + t0 * dy))
    end = (bx, by) if t1 == 1 else (round(ax + t1 * dx), round(ay + t1 * dy))
    return start, end


def clip_line(coords, box: Box) -> list[list[int]]:
    """Cut a line to the box; may split it into several parts."""
    parts: list[list[int]] = []
    current: list[int] = []
    for i in range(0, len(coords) - 2, 2):
        a = (coords[i], coords[i + 1])
        b = (coords[i + 2], coords[i + 3])
        segment = _clip_segment(*a, *b, box)
        if segment is None:
            if len(current) >= 4:
                parts.append(current)
            current = []
            continue
        start, end = segment
        if not current or (current[-2], current[-1]) != start:
            if len(current) >= 4:
                parts.append(current)
            current = [*start]
        current += end
        if end != b:  # left the box
            parts.append(current)
            current = []
    if len(current) >= 4:
        parts.append(current)
    return parts


def clip_ring(coords, box: Box) -> list[int]:
    """Sutherland-Hodgman: cut a polygon ring to the box (for filling)."""
    points = [(coords[i], coords[i + 1]) for i in range(0, len(coords), 2)]
    x0, y0, x1, y1 = box
    edges = (
        (lambda p: p[0] >= x0, lambda a, b: _at_x(a, b, x0)),
        (lambda p: p[0] <= x1, lambda a, b: _at_x(a, b, x1)),
        (lambda p: p[1] >= y0, lambda a, b: _at_y(a, b, y0)),
        (lambda p: p[1] <= y1, lambda a, b: _at_y(a, b, y1)),
    )
    for inside, cross in edges:
        if not points:
            break
        result = []
        previous = points[-1]
        for point in points:
            if inside(point):
                if not inside(previous):
                    result.append(cross(previous, point))
                result.append(point)
            elif inside(previous):
                result.append(cross(previous, point))
            previous = point
        points = result
    return [value for point in points for value in point] if len(points) >= 3 else []


def _at_x(a, b, x):
    t = (x - a[0]) / (b[0] - a[0])
    return (x, round(a[1] + t * (b[1] - a[1])))


def _at_y(a, b, y):
    t = (y - a[1]) / (b[1] - a[1])
    return (round(a[0] + t * (b[0] - a[0])), y)


def _within(bbox, box: Box) -> bool:
    return (
        bbox[0] >= box[0]
        and bbox[1] >= box[1]
        and bbox[2] <= box[2]
        and bbox[3] <= box[3]
    )


def _overlaps(bbox, box: Box) -> bool:
    return (
        bbox[0] <= box[2]
        and bbox[2] >= box[0]
        and bbox[1] <= box[3]
        and bbox[3] >= box[1]
    )


def cut_layer(name: str, box: Box | None) -> dict[str, Any]:
    """One layer, cut to ``box`` (integer coordinates); None keeps all of it."""
    kind, _scale, items = _layer(name)
    out: list = []
    if kind == "point":
        out = [
            item
            for item in items
            if box is None
            or (box[0] <= item[0] <= box[2] and box[1] <= item[1] <= box[3])
        ]
        return {"kind": kind, "items": out}
    for bbox, coords in items:
        if box is None or _within(bbox, box):
            out.append(coords.tolist())
        elif _overlaps(bbox, box):
            if kind == "line":
                out.extend(clip_line(coords, box))
            else:
                ring = clip_ring(coords, box)
                if ring:
                    out.append(ring)
    return {"kind": kind, "items": out}


# -- areas ----------------------------------------------------------------------


def track_bounds(tracks: Iterable[dict[str, Any]]) -> list[float] | None:
    """[west, south, east, north] of all track samples, or None."""
    lats: list[float] = []
    lons: list[float] = []
    for track in tracks:
        for sample in track.get("samples") or []:
            lats.append(sample[1])
            lons.append(sample[2])
    if not lats:
        return None
    west, east = min(lons), max(lons)
    if east - west > 180:  # crosses the antimeridian: take the whole width
        west, east = -180.0, 180.0
    return [west, min(lats), east, max(lats)]


def map_area(bounds: list[float]) -> list[float]:
    """The tracks' bounds plus a margin, kept on the map."""
    west, south, east, north = bounds
    extent = max(east - west, north - south)
    margin = max(MIN_MARGIN_DEG, extent * MARGIN_FRACTION)
    return [
        max(-180.0, west - margin),
        max(-MAX_LAT, south - margin),
        min(180.0, east + margin),
        min(MAX_LAT, north + margin),
    ]


def parse_bounds(text: str) -> list[float]:
    """ "west,south,east,north" in degrees; raises ValueError."""
    values = [float(part) for part in text.split(",")]
    if len(values) != 4 or not all(math.isfinite(v) for v in values):
        raise ValueError("bbox must be west,south,east,north")
    west, south, east, north = values
    if not (-180 <= west <= east <= 180 and -90 <= south <= north <= 90):
        raise ValueError("bbox is out of range")
    return values


def basemap(bounds: list[float] | None, *, world: bool = True) -> dict[str, Any]:
    """Basemap for tracks within ``bounds`` (see the module docstring).

    With no bounds there is no detailed area, only the world layers."""
    scale = _layer("world_land")[1]
    layers: dict[str, Any] = {}
    if world:
        layers.update({name: cut_layer(name, None) for name in WORLD_LAYERS})
    area = map_area(bounds) if bounds else None
    if area:
        box = tuple(round(value * scale) for value in area)
        layers.update({name: cut_layer(name, box) for name in DETAIL_LAYERS})
    return {"scale": scale, "bounds": area, "layers": layers}


def basemap_for_tracks(tracks: list[dict[str, Any]]) -> dict[str, Any]:
    return basemap(track_bounds(tracks))
