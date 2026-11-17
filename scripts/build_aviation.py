"""Build the Map chart's aviation layers: airports, runways, navaids, airspace.

Usage::

    python scripts/build_aviation.py SOURCE_DIR [OUT_DIR]

SOURCE_DIR holds:

- ``airports.csv``, ``runways.csv``, ``navaids.csv`` from OurAirports
  (davidmegginson.github.io/ourairports-data; public domain).
- ``airspace_*.geojson``: FAA Class B/C/D airspace (US only; public open data)
  from the FAA ADDS ``Class_Airspace`` feature service, fetched in pages of
  1000 with ``where=CLASS IN ('B','C','D')``, ``outSR=4326``, ``f=geojson``.

OUT_DIR defaults to ``src/cinc/data/basemap``. The output uses the same format
as ``build_basemap.py``; point items carry a fifth field, the feature kind:

- ``airports`` (point): ``[x, y, ident, min_zoom, kind]``, kind one of
  large, medium, small, heliport, seaplane. Fields with only an OurAirports
  placeholder ident (``US-1234``) are left out.
- ``runways`` (line): one segment per runway with both ends known.
- ``navaids`` (point): ``[x, y, "IDENT freq", min_zoom, kind]``, kind one of
  vor, dme, ndb.
- ``airspace_b`` / ``airspace_c`` / ``airspace_d`` (line): boundary rings,
  simplified to about 20 m.
"""

from __future__ import annotations

import csv
import gzip
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_basemap import SCALE, _quantize, _with_bbox  # noqa: E402

SIMPLIFY = 2  # Douglas-Peucker tolerance in scaled units (2e-4 deg, ~20 m)

# OurAirports type -> (kind, min zoom)
AIRPORT_TYPES = {
    "large_airport": ("large", 4),
    "medium_airport": ("medium", 7),
    "small_airport": ("small", 9.5),
    "seaplane_base": ("seaplane", 10.5),
    "heliport": ("heliport", 11),
}
# OurAirports navaid type -> (kind, min zoom)
NAVAID_TYPES = {
    "VOR": ("vor", 6),
    "VOR-DME": ("vor", 6),
    "VORTAC": ("vor", 6),
    "TACAN": ("dme", 8),
    "DME": ("dme", 8),
    "NDB": ("ndb", 8),
    "NDB-DME": ("ndb", 8),
}


def _float(value: str) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _point(lon: float, lat: float) -> list[int]:
    return [round(lon * SCALE), round(lat * SCALE)]


def build_airports(source: Path) -> dict:
    items = []
    with source.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            kind = AIRPORT_TYPES.get(row["type"])
            lat, lon = _float(row["latitude_deg"]), _float(row["longitude_deg"])
            if not kind or lat is None or lon is None:
                continue
            ident = row["icao_code"] or row["gps_code"] or row["local_code"] or row["ident"]
            if "-" in ident:
                continue  # OurAirports' own placeholder (e.g. US-1234): no official code
            items.append([*_point(lon, lat), ident, kind[1], kind[0]])
    # Most important first, so a cut keeps a stable order.
    items.sort(key=lambda item: (item[3], item[2]))
    return {"kind": "point", "scale": SCALE, "items": items}


def build_runways(source: Path, airports: Path) -> dict:
    open_airports = set()
    with airports.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["type"] in AIRPORT_TYPES:
                open_airports.add(row["id"])
    items = []
    with source.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["closed"] == "1" or row["airport_ref"] not in open_airports:
                continue
            ends = [
                (_float(row["le_longitude_deg"]), _float(row["le_latitude_deg"])),
                (_float(row["he_longitude_deg"]), _float(row["he_latitude_deg"])),
            ]
            if any(value is None for end in ends for value in end):
                continue
            flat = _quantize(ends)
            if len(flat) == 4:
                items.append(_with_bbox(flat))
    return {"kind": "line", "scale": SCALE, "items": items}


def _frequency(kind: str, khz: str) -> str:
    value = _float(khz)
    if value is None:
        return ""
    if kind == "ndb":
        return f"{value:g}"
    return f"{value / 1000:.2f}"


def build_navaids(source: Path) -> dict:
    items = []
    with source.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            kind = NAVAID_TYPES.get(row["type"])
            lat, lon = _float(row["latitude_deg"]), _float(row["longitude_deg"])
            if not kind or lat is None or lon is None or not row["ident"]:
                continue
            label = " ".join(filter(None, [row["ident"], _frequency(kind[0], row["frequency_khz"])]))
            items.append([*_point(lon, lat), label, kind[1], kind[0]])
    items.sort(key=lambda item: (item[3], item[2]))
    return {"kind": "point", "scale": SCALE, "items": items}


def _simplify(points: list[tuple[int, int]], tolerance: float) -> list[tuple[int, int]]:
    """Douglas-Peucker on integer points (iterative)."""
    if len(points) < 3:
        return points
    keep = [False] * len(points)
    keep[0] = keep[-1] = True
    stack = [(0, len(points) - 1)]
    while stack:
        start, end = stack.pop()
        ax, ay = points[start]
        bx, by = points[end]
        dx, dy = bx - ax, by - ay
        length = (dx * dx + dy * dy) ** 0.5
        best, best_index = -1.0, -1
        for i in range(start + 1, end):
            px, py = points[i]
            if length == 0:
                distance = ((px - ax) ** 2 + (py - ay) ** 2) ** 0.5
            else:
                distance = abs(dy * (px - ax) - dx * (py - ay)) / length
            if distance > best:
                best, best_index = distance, i
        if best > tolerance:
            keep[best_index] = True
            stack += [(start, best_index), (best_index, end)]
    return [point for point, kept in zip(points, keep) if kept]


def build_airspace(sources: list[Path]) -> dict[str, dict]:
    layers = {name: [] for name in ("B", "C", "D")}
    for source in sources:
        for feature in json.loads(source.read_text(encoding="utf-8"))["features"]:
            geometry = feature.get("geometry")
            airspace_class = (feature.get("properties") or {}).get("CLASS")
            if not geometry or airspace_class not in layers:
                continue
            polygons = (
                [geometry["coordinates"]]
                if geometry["type"] == "Polygon"
                else geometry["coordinates"]
            )
            for polygon in polygons:
                for ring in polygon:
                    flat = _quantize(ring)
                    points = list(zip(flat[0::2], flat[1::2]))
                    # Split the closed ring in two so the end points stay put.
                    half = len(points) // 2
                    simplified = (
                        _simplify(points[: half + 1], SIMPLIFY)[:-1]
                        + _simplify(points[half:], SIMPLIFY)
                    )
                    if len(simplified) >= 3:
                        layers[airspace_class].append(
                            _with_bbox([value for point in simplified for value in point])
                        )
    return {
        f"airspace_{name.lower()}": {"kind": "line", "scale": SCALE, "items": items}
        for name, items in layers.items()
    }


def write_layer(out_dir: Path, name: str, layer: dict) -> None:
    payload = json.dumps(layer, separators=(",", ":"), ensure_ascii=False)
    target = out_dir / f"{name}.json.gz"
    # mtime=0 keeps the output byte-identical between runs
    target.write_bytes(gzip.compress(payload.encode("utf-8"), 9, mtime=0))
    print(f"{target.name}: {len(layer['items'])} items, {target.stat().st_size:,} B")


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
    airports = source_dir / "airports.csv"
    write_layer(out_dir, "airports", build_airports(airports))
    write_layer(out_dir, "runways", build_runways(source_dir / "runways.csv", airports))
    write_layer(out_dir, "navaids", build_navaids(source_dir / "navaids.csv"))
    airspace_sources = sorted(source_dir.glob("airspace_*.geojson"))
    for name, layer in build_airspace(airspace_sources).items():
        write_layer(out_dir, name, layer)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
