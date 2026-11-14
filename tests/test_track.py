import pytest

from cinc.app import create_app
from cinc.core.assemble import assemble_page_data
from cinc.core.registry import Registry
from cinc.core.basemap import basemap, clip_line, clip_ring, map_area, track_bounds
from cinc.core.track import TrackLogType, build_tracks, merge_tracks
from cinc.standalone import build_standalone_bundle
from fake_logs import TRACKS, text_rows, track_rows, write_log


def test_rows_group_into_sorted_tracks_with_header_labels():
    rows = [
        {"time": "2026-01-01T00:00:02Z", "track": "A", "lat": 1, "lon": 2, "alt": 30},
        {"time": "2026-01-01T00:00:01Z", "track": "A", "lat": 0, "lon": 1},
        {"time": "2026-01-01T00:00:01", "lat": 5, "lon": 6, "color": "red"},
    ]
    tracks = build_tracks(rows, {"tracks": {"A": {"label": "Alpha"}}})
    alpha, default = tracks
    assert alpha["label"] == "Alpha" and alpha["color"] is None
    assert [s[0] for s in alpha["samples"]] == sorted(s[0] for s in alpha["samples"])
    assert alpha["samples"][0][1:] == [0.0, 1.0, 0.0]
    assert default["id"] == "track" and default["color"] == "red"


@pytest.mark.parametrize(
    "row",
    [
        {"lat": 1, "lon": 2},
        {"time": "nope", "lat": 1, "lon": 2},
        {"time": "2026-01-01T00:00:00Z", "lat": "1", "lon": 2},
        {"time": "2026-01-01T00:00:00Z", "lat": 95, "lon": 2},
    ],
)
def test_bad_rows_are_rejected(row):
    with pytest.raises(ValueError):
        build_tracks([row])


def test_merge_joins_tracks_with_the_same_id():
    a = build_tracks([{"time": "2026-01-01T00:00:02Z", "lat": 1, "lon": 1}])
    b = build_tracks([{"time": "2026-01-01T00:00:01Z", "lat": 2, "lon": 2}])
    (merged,) = merge_tracks([a, b])
    assert [s[1] for s in merged["samples"]] == [2.0, 1.0]


def test_track_type_is_builtin_and_adds_no_rows():
    registry = Registry.discover()
    log_type = registry.get("track")
    assert isinstance(log_type, TrackLogType)
    page = assemble_page_data([(log_type, {"tracks": TRACKS, "events": track_rows(2)})])
    assert page["logData"]["events"] == []
    ids = {track["id"] for track in page["logData"]["tracks"]}
    assert ids == set(TRACKS)
    assert page["logData"]["tracks"][0]["label"] == "Survey drone"


def test_longitudes_are_stored_in_minus_180_to_180():
    (track,) = build_tracks([{"time": "2026-01-01T00:00:00Z", "lat": 0, "lon": 190}])
    assert track["samples"][0][2] == -170.0


def test_log_folder_with_tracks_serves_and_downloads_with_leaflet(tmp_path):
    write_log(tmp_path, rows=4)
    client = create_app(tmp_path).test_client()
    page = client.get("/logs/fake-20260101").data
    assert b'"tracks":[{' in page and b"vendor/leaflet/leaflet.js" in page
    download = client.get("/logs/fake-20260101/standalone.html").data.decode()
    assert (
        'id="cinc-leaflet-source"' in download and 'id="cinc-leaflet-style"' in download
    )
    assert '"basemap":{' in download and '"coastline":' in download


def test_standalone_without_tracks_does_not_embed_leaflet(tmp_path):
    path = write_log(tmp_path, rows=4)
    output = build_standalone_bundle([path / "text_log.jsonl"], tmp_path / "t.html")
    html = output.read_text()
    assert '<script id="cinc-leaflet-source"' not in html
    assert "window.CINC_MAP =" not in html


def test_live_posts_stream_new_track_samples(tmp_path):
    app = create_app(tmp_path)
    client, hub = app.test_client(), app.extensions["cinc"]["hub"]
    client.post("/api/events", json={"type": "text_log", "events": text_rows(1)})
    client.post("/api/events", json={"type": "track", "events": track_rows(1, 0)})
    subscriber = hub.subscribe(hub.current.id, after=hub._seq)
    response = client.post(
        "/api/events", json={"type": "track", "events": track_rows(1, 1)}
    )
    assert response.status_code == 202
    message = subscriber.get_nowait()
    assert message["type"] == "events" and message["events"] == []
    samples = {t["id"]: len(t["samples"]) for t in message["tracks"]}
    assert samples == {"UAV-1": 5}  # AC-7 is only sampled when it turns
    page = hub.page_data(hub.current)
    counts = {t["id"]: len(t["samples"]) for t in page["logData"]["tracks"]}
    assert counts == {"UAV-1": 10, "AC-7": 1}


def test_live_rejects_bad_track_rows(tmp_path):
    client = create_app(tmp_path).test_client()
    bad = client.post("/api/events", json={"type": "track", "events": [{"lat": 1}]})
    assert bad.status_code == 400 and "time" in bad.get_json()["error"]


def test_clip_line_splits_at_the_box_edges():
    box = (0, 0, 10, 10)
    # in, out across the right edge, back in, and out again
    assert clip_line([5, 5, 15, 5, 15, 8, 5, 8], box) == [[5, 5, 10, 5], [10, 8, 5, 8]]
    assert clip_line([20, 20, 30, 30], box) == []


def test_clip_ring_cuts_a_polygon_to_the_box():
    ring = clip_ring([-5, -5, 5, -5, 5, 5, -5, 5], (0, 0, 10, 10))
    points = {(ring[i], ring[i + 1]) for i in range(0, len(ring), 2)}
    assert points == {(0, 0), (5, 0), (5, 5), (0, 5)}


def test_basemap_covers_the_tracks_with_detail_and_the_world():
    tracks = build_tracks(track_rows(20), {"tracks": TRACKS})
    bounds = track_bounds(tracks)
    data = basemap(bounds)
    west, south, east, north = data["bounds"]
    assert (
        west < bounds[0]
        and east > bounds[2]
        and south < bounds[1]
        and north > bounds[3]
    )
    assert data["bounds"] == map_area(bounds)
    layers = data["layers"]
    assert layers["coastline"]["items"] and layers["land"]["items"]
    assert any(item[2] == "Los Angeles" for item in layers["places"]["items"])
    assert len(layers["country_labels"]["items"]) > 200  # the whole world
    scale, (w, s, e, n) = data["scale"], data["bounds"]
    for item in layers["roads"]["items"]:
        xs, ys = item[0::2], item[1::2]
        assert w * scale - 1 <= min(xs) and max(xs) <= e * scale + 1
        assert s * scale - 1 <= min(ys) and max(ys) <= n * scale + 1


def test_track_bounds_across_the_antimeridian_take_the_whole_width():
    tracks = build_tracks(
        [
            {"time": "2026-01-01T00:00:00Z", "lat": 0, "lon": 179},
            {"time": "2026-01-01T00:01:00Z", "lat": 1, "lon": -179},
        ]
    )
    assert track_bounds(tracks) == [-180.0, 0.0, 180.0, 1.0]


def test_basemap_api(tmp_path):
    client = create_app(tmp_path).test_client()
    response = client.get("/api/basemap?bbox=-118,33.9,-117,34.4&world=0")
    assert response.status_code == 200
    data = response.get_json()
    assert "world_land" not in data["layers"] and data["layers"]["coastline"]["items"]
    assert "land" not in client.get("/api/basemap").get_json()["layers"]
    assert client.get("/api/basemap?bbox=1,2,3").status_code == 400
