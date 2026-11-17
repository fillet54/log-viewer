"""Build the Map chart's basemap data from Natural Earth GeoJSON.

Usage::

    python scripts/build_basemap.py NE_GEOJSON_DIR [OUT_DIR]

NE_GEOJSON_DIR holds the Natural Earth 5.1.2 GeoJSON files named in LAYERS
(from github.com/nvkelso/natural-earth-vector, folder ``geojson/``). OUT_DIR
defaults to ``src/cinc/data/basemap``. Natural Earth is public domain.

Each output file is gzipped JSON ``{"kind", "scale", "items"}`` with
coordinates as integers (degrees * scale, about 11 m at 1e4):

- ``polygon``: items are rings, ``[minx, miny, maxx, maxy, x0, y0, dx1, dy1, ...]``
  where each later point is a delta from the previous one (feature structure is
  dropped; rings are drawn with the even-odd rule).
- ``line``: items are line parts in the same form.
- ``point``: items are ``[x, y, name, min_zoom]``.

``cinc.core.basemap`` cuts these to an area for the viewer.
"""

from __future__ import annotations

import gzip
import json
import sys
from pathlib import Path

SCALE = 10_000

# output name -> (source file, kind, options)
LAYERS = {
    # whole world, low detail: the backdrop outside the detailed area
    "world_land": ("ne_110m_land", "polygon", {}),
    "world_borders": ("ne_110m_admin_0_boundary_lines_land", "line", {}),
    "country_labels": (
        "ne_10m_admin_0_countries",
        "point",
        {"x": "LABEL_X", "y": "LABEL_Y", "name": "NAME", "zoom": "MIN_LABEL"},
    ),
    # 1:10m detail, cut to the track area
    "land": ("ne_10m_land", "polygon", {}),
    "glaciers": ("ne_10m_glaciated_areas", "polygon", {}),
    "urban": ("ne_10m_urban_areas", "polygon", {}),
    "lakes": ("ne_10m_lakes", "polygon", {}),
    "coastline": ("ne_10m_coastline", "line", {}),
    "rivers": ("ne_10m_rivers_lake_centerlines", "line", {}),
    "roads": ("ne_10m_roads", "line", {"skip_type": "Ferry"}),
    "borders": ("ne_10m_admin_0_boundary_lines_land", "line", {}),
    "state_borders": ("ne_10m_admin_1_states_provinces_lines", "line", {}),
    "state_labels": (
        "ne_10m_admin_1_states_provinces",
        "point",
        {"x": "longitude", "y": "latitude", "name": "name", "zoom": "min_label"},
    ),
    "places": (
        "ne_10m_populated_places_simple",
        "point",
        {"name": "name", "zoom": "min_zoom"},
    ),
}


def _quantize(coords) -> list[int]:
    flat: list[int] = []
    for lon, lat, *_ in coords:
        x, y = round(lon * SCALE), round(lat * SCALE)
        if flat and flat[-2] == x and flat[-1] == y:
            continue
        flat += [x, y]
    return flat


def _with_bbox(flat: list[int]) -> list[int]:
    """bbox, first point, then each point as a delta from the one before
    (deltas compress far better than absolute coordinates)."""
    xs, ys = flat[0::2], flat[1::2]
    deltas = [flat[0], flat[1]]
    for i in range(2, len(flat), 2):
        deltas += [flat[i] - flat[i - 2], flat[i + 1] - flat[i - 1]]
    return [min(xs), min(ys), max(xs), max(ys), *deltas]


def _parts(geometry, kind: str):
    gtype, coords = geometry["type"], geometry["coordinates"]
    if kind == "line":
        lines = [coords] if gtype == "LineString" else coords
        for line in lines:
            flat = _quantize(line)
            if len(flat) >= 4:
                yield _with_bbox(flat)
    else:
        polygons = [coords] if gtype == "Polygon" else coords
        for polygon in polygons:
            for ring in polygon:
                flat = _quantize(ring)
                if len(flat) >= 6:
                    yield _with_bbox(flat)


def build_layer(source: Path, kind: str, options: dict) -> dict:
    features = json.loads(source.read_text(encoding="utf-8"))["features"]
    items: list = []
    for feature in features:
        props, geometry = feature["properties"], feature["geometry"]
        if not geometry:
            continue
        if options.get("skip_type") and options["skip_type"] in str(
            props.get("type", "")
        ):
            continue
        if kind == "point":
            if "x" in options:
                lon, lat = props[options["x"]], props[options["y"]]
            else:
                lon, lat = geometry["coordinates"][:2]
            name = props.get(options["name"])
            if not name or lon is None or lat is None:
                continue
            zoom = props.get(options["zoom"])
            zoom = round(float(zoom), 1) if zoom is not None else 0
            items.append([round(lon * SCALE), round(lat * SCALE), name, zoom])
        else:
            items.extend(_parts(geometry, kind))
    return {"kind": kind, "scale": SCALE, "items": items}


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 2
    source_dir = Path(argv[0])
    out_dir = (
        Path(argv[1])
        if len(argv) > 1
        else Path(__file__).resolve().parents[1] / "src/cinc/data/basemap"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, (source, kind, options) in LAYERS.items():
        layer = build_layer(source_dir / f"{source}.geojson", kind, options)
        payload = json.dumps(layer, separators=(",", ":"), ensure_ascii=False)
        target = out_dir / f"{name}.json.gz"
        # mtime=0 keeps the output byte-identical between runs
        target.write_bytes(gzip.compress(payload.encode("utf-8"), 9, mtime=0))
        print(
            f"{target.name}: {len(layer['items'])} items, {target.stat().st_size:,} B"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
