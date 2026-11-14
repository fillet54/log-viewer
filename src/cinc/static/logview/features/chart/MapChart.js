// Map chart: position tracks on a 2D Leaflet map, synced with the log's timeline.
//
// Tracks come from `logData.tracks` (and live batches via viewerStore.tracksSignal), shaped
// [{ id, label, color, samples: [[timeMs, lat, lon, alt], ...] }] — see cinc/core/track.py.
// Between samples a track moves at constant heading and speed (a rhumb line, which is a straight
// line on this Mercator map), so sparse data such as one sample per heading change is enough.
//
// Time sync: scrolling or selecting in the log moves the map's clock; scrubbing, playing or
// clicking a track jumps the log to that time.
//
// Leaflet is vendored (static/vendor/leaflet) and loaded on first use. The basemap is Natural
// Earth (cinc/core/basemap.py): a low-detail world plus 1:10m layers cut to the tracks' area.
// Standalone pages embed both; served pages fetch the basemap and widen it as live tracks grow.
import { themeColor, onThemeChange } from "../../../theme.js";
import { signals } from "logview/lib";

const LogMainViewChart = window.LogMainViewChart;
const signalEffect = signals?.effect || null;

const TRACK_COLORS = ["--series-1", "--series-3", "--series-5", "--series-6", "--series-4", "--series-2"];
const SPEEDS = [1, 5, 20, 60, 300];
const INCOMING_QUIET_MS = 700; // ignore log -> map sync this long after the map drove the log
const OUTGOING_JUMP_MS = 150; // throttle map -> log jumps
const PICK_RADIUS_PX = 10;
const LAYER_STORAGE_KEY = "cinc.map.layers";
const OVERLAYS = [
  { id: "political", label: "Borders & names" },
  { id: "places", label: "Places" },
  { id: "roads", label: "Roads" },
];

// -- loading ------------------------------------------------------------------------------------

let leafletPromise = null;
const loadLeaflet = () => {
  if (window.L?.map) return Promise.resolve(window.L);
  if (leafletPromise) return leafletPromise;
  const config = window.CINC_MAP || {};
  const style = new Promise((resolve) => {
    // Standalone pages inline the stylesheet; served pages link the vendored file.
    if (document.getElementById("cinc-leaflet-style") || !config.leafletCssUrl) {
      resolve();
      return;
    }
    const link = document.createElement("link");
    link.rel = "stylesheet";
    link.href = config.leafletCssUrl;
    link.onload = link.onerror = () => resolve();
    document.head.appendChild(link);
  });
  const script = new Promise((resolve, reject) => {
    // Standalone pages carry Leaflet as inert text; served pages point at the vendored file.
    const inline = document.getElementById("cinc-leaflet-source");
    const src = inline
      ? URL.createObjectURL(new Blob([inline.textContent], { type: "text/javascript" }))
      : config.leafletUrl;
    if (!src) {
      reject(new Error("Leaflet is not available on this page."));
      return;
    }
    const tag = document.createElement("script");
    tag.src = src;
    tag.onload = () => {
      if (inline) URL.revokeObjectURL(src);
      if (window.L?.map) resolve(window.L);
      else reject(new Error("Leaflet did not initialise."));
    };
    tag.onerror = () => reject(new Error("Could not load Leaflet."));
    document.head.appendChild(tag);
  });
  leafletPromise = Promise.all([script, style]).then(([L]) => L);
  leafletPromise.catch(() => {
    leafletPromise = null;
  });
  return leafletPromise;
};

// Served pages ask the server for the basemap around `bounds`; standalone pages carry it.
const fetchBasemap = async (bounds, { world = true } = {}) => {
  const config = window.CINC_MAP || {};
  if (config.basemap) return config.basemap;
  if (!config.basemapUrl) return null;
  const params = new URLSearchParams();
  if (bounds) params.set("bbox", bounds.map((value) => value.toFixed(4)).join(","));
  if (!world) params.set("world", "0");
  const response = await fetch(`${config.basemapUrl}?${params}`);
  if (!response.ok) throw new Error(`basemap request failed (${response.status})`);
  return response.json();
};

// -- geometry -----------------------------------------------------------------------------------

const D2R = Math.PI / 180;
const EARTH_RADIUS_M = 6371008.8;
const MS_TO_KT = 1.943844;
const mercY = (lat) => Math.log(Math.tan(Math.PI / 4 + (Math.max(-89.9, Math.min(89.9, lat)) * D2R) / 2));
const invMercY = (y) => (2 * Math.atan(Math.exp(y)) - Math.PI / 2) / D2R;
const wrapLon = (lon) => ((((lon + 180) % 360) + 360) % 360) - 180;
const wrapDelta = (delta) => (delta > 180 ? delta - 360 : delta < -180 ? delta + 360 : delta);

// A leg from sample i to i+1 along a rhumb line: heading (deg), ground speed (m/s).
const legBetween = (lat0, lon0, lat1, lon1, dtMs) => {
  const dPsi = mercY(lat1) - mercY(lat0);
  const dLat = (lat1 - lat0) * D2R;
  const dLon = (lon1 - lon0) * D2R;
  const q = Math.abs(dPsi) > 1e-12 ? dLat / dPsi : Math.cos(lat0 * D2R);
  const distance = EARTH_RADIUS_M * Math.sqrt(dLat * dLat + q * q * dLon * dLon);
  const heading = distance > 0.01 ? ((Math.atan2(dLon, dPsi) / D2R) + 360) % 360 : null;
  return { dPsi, heading, speed: dtMs > 0 ? distance / (dtMs / 1000) : 0 };
};

// Index of the last sample at or before `ms` (-1 if before the first).
const sampleIndexAt = (samples, ms) => {
  let lo = 0;
  let hi = samples.length - 1;
  if (hi < 0 || ms < samples[0][0]) return -1;
  while (lo < hi) {
    const mid = (lo + hi + 1) >> 1;
    if (samples[mid][0] <= ms) lo = mid;
    else hi = mid - 1;
  }
  return lo;
};

// Lat at fraction `f` of a leg, and the matching (unwrapped) lon on the rhumb line.
const along = (lat0, lon0, lat1, lon1, dPsi, f) => {
  const lat = lat0 + (lat1 - lat0) * f;
  const g = Math.abs(dPsi) > 1e-12 ? (mercY(lat) - mercY(lat0)) / dPsi : f;
  return [lat, lon0 + (lon1 - lon0) * g];
};

const trackBounds = (tracks) => {
  let west = Infinity;
  let east = -Infinity;
  let south = Infinity;
  let north = -Infinity;
  tracks.forEach((track) =>
    (track.samples || []).forEach((sample) => {
      south = Math.min(south, sample[1]);
      north = Math.max(north, sample[1]);
      west = Math.min(west, sample[2]);
      east = Math.max(east, sample[2]);
    })
  );
  if (!Number.isFinite(west)) return null;
  if (east - west > 180) return [-180, south, 180, north];
  return [west, south, east, north];
};

const contains = (outer, inner) =>
  !!outer && !!inner && inner[0] >= outer[0] && inner[1] >= outer[1] && inner[2] <= outer[2] && inner[3] <= outer[3];

// -- helpers ------------------------------------------------------------------------------------

const pad = (value, size = 2) => String(value).padStart(size, "0");
const formatClock = (ms) => {
  if (!Number.isFinite(ms)) return "—";
  const d = new Date(ms);
  return `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}.${pad(d.getUTCMilliseconds(), 3)}Z`;
};
const formatAlt = (meters) =>
  Math.abs(meters) >= 10000 ? `${(meters / 1000).toFixed(1)} km` : `${Math.round(meters)} m`;

const timeDomain = (tracks, events) => {
  let start = Infinity;
  let end = -Infinity;
  tracks.forEach((track) => {
    const samples = track.samples || [];
    if (!samples.length) return;
    start = Math.min(start, samples[0][0]);
    end = Math.max(end, samples[samples.length - 1][0]);
  });
  if (events.length) {
    const first = Date.parse(events[0]?.time || "");
    const last = Date.parse(events[events.length - 1]?.time || "");
    if (Number.isFinite(first)) start = Math.min(start, first);
    if (Number.isFinite(last)) end = Math.max(end, last);
  }
  if (!Number.isFinite(start)) return { start: 0, end: 1 };
  return { start, end: Math.max(end, start + 1) };
};

const element = (tag, className, text) => {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text != null) node.textContent = text;
  return node;
};

const readOverlayPrefs = () => {
  const prefs = { political: true, places: true, roads: true };
  try {
    Object.assign(prefs, JSON.parse(localStorage.getItem(LAYER_STORAGE_KEY) || "{}"));
  } catch {
    /* storage unavailable: defaults */
  }
  return prefs;
};
const saveOverlayPrefs = (prefs) => {
  try {
    localStorage.setItem(LAYER_STORAGE_KEY, JSON.stringify(prefs));
  } catch {
    /* storage unavailable */
  }
};

// Basemap styles, from the theme's --map-* tokens.
const mapPalette = () => {
  const color = (name) => themeColor(`var(${name})`);
  return {
    land: color("--map-land"),
    water: color("--map-water"),
    coast: color("--map-coast"),
    urban: color("--map-urban"),
    glacier: color("--map-glacier"),
    road: color("--map-road"),
    border: color("--map-border"),
    stateBorder: color("--map-state-border"),
  };
};
const BASEMAP_STYLES = {
  world_land: (p) => ({ fillColor: p.land }),
  land: (p) => ({ fillColor: p.land }),
  glaciers: (p) => ({ fillColor: p.glacier }),
  urban: (p) => ({ fillColor: p.urban }),
  lakes: (p) => ({ fillColor: p.water }),
  coastline: (p) => ({ color: p.coast, weight: 0.8 }),
  rivers: (p) => ({ color: p.coast, weight: 0.8, opacity: 0.8 }),
  roads: (p) => ({ color: p.road, weight: 0.9 }),
  world_borders: (p) => ({ color: p.border, weight: 1 }),
  borders: (p) => ({ color: p.border, weight: 1.2 }),
  state_borders: (p) => ({ color: p.stateBorder, weight: 0.8, dashArray: "4 3" }),
};
// layer -> [pane, overlay group (null: always shown)]
const BASEMAP_PLACEMENT = {
  world_land: ["cinc-world", null],
  world_borders: ["cinc-world", "political"],
  land: ["cinc-detail", null],
  glaciers: ["cinc-detail", null],
  urban: ["cinc-detail", null],
  lakes: ["cinc-detail", null],
  rivers: ["cinc-detail", null],
  coastline: ["cinc-detail", null],
  roads: ["cinc-detail", "roads"],
  state_borders: ["cinc-political", "political"],
  borders: ["cinc-political", "political"],
};
const LABEL_LAYERS = {
  country_labels: { group: "political", className: "map-label map-label-country", priority: 0 },
  state_labels: { group: "political", className: "map-label map-label-state", priority: 1 },
  places: { group: "places", className: "map-label map-label-place", priority: 2 },
};
const PANES = [
  ["cinc-world", 200],
  ["cinc-detail", 210],
  ["cinc-political", 380],
  ["cinc-tracks", 420],
  ["cinc-labels", 450],
];

// -- the chart ----------------------------------------------------------------------------------

const createMapController = (panel, context) => {
  const store = context.viewerStore || null;
  const readTracks = () =>
    store?.tracksSignal?.value || (Array.isArray(context.logData?.tracks) ? context.logData.tracks : []);
  const readEvents = () => (Array.isArray(context.logData?.events) ? context.logData.events : []);

  panel.innerHTML = "";
  const root = element("div", "map-panel");
  const host = element("div", "map-canvas-host");
  const legend = element("div", "map-legend");
  const status = element("div", "map-status", "Loading map…");
  const scrubber = element("div", "map-scrubber");
  const slider = element("input", "map-slider");
  slider.type = "range";
  slider.min = "0";
  slider.step = "any";
  slider.setAttribute("aria-label", "Map time");
  const clockLabel = element("span", "map-clock");
  scrubber.append(slider, clockLabel);
  const stage = element("div", "map-stage");
  stage.append(host, legend, status);
  root.append(stage, scrubber);
  panel.appendChild(root);

  let L = null;
  let map = null;
  let destroyed = false;
  let fitted = false;
  let domain = timeDomain(readTracks(), readEvents());
  let currentMs = domain.start;
  let playing = false;
  let speedIndex = 2;
  let frameHandle = 0;
  let quietUntil = 0;
  let jumpTimer = null;
  let pendingJumpMs = null;
  let focusId = null;
  let commandRefresh = null;
  let palette = null;
  let detailBounds = null; // area the detailed basemap covers
  let basemapRequest = null;
  const overlayPrefs = readOverlayPrefs();
  const renderers = {};
  const basemapLayers = new Map(); // layer name -> Leaflet layer
  let mask = null;
  const labels = []; // { marker, zoom, group, priority, text, shown, key }
  const models = new Map(); // track id -> model
  const off = [];

  // -- outgoing sync (map -> log)
  const driveLog = (ms) => {
    quietUntil = performance.now() + INCOMING_QUIET_MS;
    pendingJumpMs = ms;
    if (jumpTimer) return;
    jumpTimer = setTimeout(() => {
      jumpTimer = null;
      quietUntil = performance.now() + INCOMING_QUIET_MS;
      store?.setLogJump?.({ timeMs: pendingJumpMs });
    }, OUTGOING_JUMP_MS);
  };

  // -- basemap
  const overlayOn = (group) => group == null || overlayPrefs[group] !== false;

  const styleBasemap = () => {
    palette = mapPalette();
    basemapLayers.forEach((layer, name) => layer.setStyle(BASEMAP_STYLES[name](palette)));
    mask?.setStyle({ fillColor: palette.water });
  };

  const placeLayer = (name, layer) => {
    const previous = basemapLayers.get(name);
    if (previous) previous.remove();
    basemapLayers.set(name, layer);
    const group = BASEMAP_PLACEMENT[name][1];
    if (overlayOn(group)) layer.addTo(map);
  };

  const toLatLngs = (flat, scale) => {
    const out = new Array(flat.length / 2);
    for (let i = 0, j = 0; i < flat.length; i += 2, j += 1) out[j] = [flat[i + 1] / scale, flat[i] / scale];
    return out;
  };

  const addBasemap = (data) => {
    if (!data || !map) return;
    const scale = data.scale || 1;
    // The world backdrop is coarse; hide it where the detailed area takes over.
    if (data.bounds) {
      const [w, s, e, n] = data.bounds;
      detailBounds = data.bounds;
      mask?.remove();
      mask = L.rectangle(
        [
          [s, w],
          [n, e],
        ],
        { pane: "cinc-detail", renderer: renderers["cinc-detail"], stroke: false, fillOpacity: 1, interactive: false }
      ).addTo(map);
      mask.bringToBack();
    }
    Object.entries(data.layers || {}).forEach(([name, layer]) => {
      if (LABEL_LAYERS[name]) {
        addLabels(name, layer.items, scale);
        return;
      }
      const placement = BASEMAP_PLACEMENT[name];
      if (!placement) return;
      const [pane] = placement;
      const options = { pane, renderer: renderers[pane], interactive: false, smoothFactor: 1 };
      const items = layer.items.map((item) => toLatLngs(item, scale));
      if (!items.length) {
        basemapLayers.get(name)?.remove();
        basemapLayers.delete(name);
        return;
      }
      const leafletLayer =
        layer.kind === "polygon"
          ? L.polygon(items, { ...options, stroke: false, fillOpacity: 1 })
          : L.polyline(items, { ...options, fill: false });
      placeLayer(name, leafletLayer);
    });
    // Keep the detail pane's drawing order: mask, land, then features on top.
    ["land", "glaciers", "urban", "lakes", "rivers", "coastline", "roads"].forEach((name) =>
      basemapLayers.get(name)?.bringToFront()
    );
    styleBasemap();
    layoutLabels();
  };

  const addLabels = (name, items, scale) => {
    const config = LABEL_LAYERS[name];
    for (let i = labels.length - 1; i >= 0; i -= 1) {
      if (labels[i].layer === name) {
        labels[i].marker.remove();
        labels.splice(i, 1);
      }
    }
    items.forEach(([x, y, text, zoom]) => {
      const marker = L.marker([y / scale, x / scale], {
        pane: "cinc-labels",
        interactive: false,
        keyboard: false,
        icon: L.divIcon({ className: config.className, html: `<span></span>`, iconSize: null }),
      });
      labels.push({ layer: name, marker, text, zoom: Number(zoom) || 0, ...config, shown: false });
    });
    labels.sort((a, b) => a.priority - b.priority || a.zoom - b.zoom);
  };

  // Show labels allowed at this zoom (Natural Earth's min zoom), most important first,
  // skipping any that would overlap one already shown.
  const layoutLabels = () => {
    if (!map) return;
    const zoom = map.getZoom();
    const size = map.getSize();
    const placed = [];
    labels.forEach((label) => {
      let show = overlayOn(label.group) && zoom >= label.zoom - 1;
      let box = null;
      if (show) {
        const point = map.latLngToContainerPoint(label.marker.getLatLng());
        const width = label.text.length * (label.priority === 0 ? 7.5 : 6.5) + 10;
        const left = label.priority === 2 ? point.x - 4 : point.x - width / 2;
        box = [left, point.y - 8, left + width, point.y + 8];
        show =
          box[2] > 0 &&
          box[0] < size.x &&
          box[3] > 0 &&
          box[1] < size.y &&
          !placed.some((other) => box[0] < other[2] && box[2] > other[0] && box[1] < other[3] && box[3] > other[1]);
      }
      if (show) {
        placed.push(box);
        if (!label.shown) {
          label.marker.addTo(map);
          label.marker.getElement().firstChild.textContent = label.text;
          label.shown = true;
        }
      } else if (label.shown) {
        label.marker.remove();
        label.shown = false;
      }
    });
  };

  const setOverlay = (group, on) => {
    overlayPrefs[group] = on;
    saveOverlayPrefs(overlayPrefs);
    basemapLayers.forEach((layer, name) => {
      if (BASEMAP_PLACEMENT[name][1] !== group) return;
      if (on) layer.addTo(map);
      else layer.remove();
    });
    layoutLabels();
  };

  // Live mode on a served page: widen the detailed area when tracks leave it.
  const ensureDetailCovers = () => {
    const config = window.CINC_MAP || {};
    if (config.basemap || !config.basemapUrl || basemapRequest) return;
    const bounds = trackBounds(readTracks());
    if (!bounds || contains(detailBounds, bounds)) return;
    basemapRequest = fetchBasemap(bounds, { world: false })
      .then((data) => {
        if (!destroyed) addBasemap(data);
      })
      .catch((error) => console.warn("Map: could not load the basemap", error))
      .finally(() => {
        basemapRequest = null;
      });
  };

  // -- tracks
  const trackColor = (track, index) =>
    track.color || themeColor(`var(${TRACK_COLORS[index % TRACK_COLORS.length]})`);

  const buildGeometry = (model) => {
    const samples = model.track.samples || [];
    const lons = new Array(samples.length);
    const latlngs = new Array(samples.length);
    const legs = new Array(Math.max(0, samples.length - 1));
    samples.forEach((sample, i) => {
      // Unwrapped longitudes keep a track continuous across the antimeridian.
      lons[i] = i === 0 ? sample[2] : lons[i - 1] + wrapDelta(sample[2] - samples[i - 1][2]);
      latlngs[i] = [sample[1], lons[i]];
      if (i > 0) {
        const a = samples[i - 1];
        legs[i - 1] = legBetween(a[1], lons[i - 1], sample[1], lons[i], sample[0] - a[0]);
      }
    });
    // A leg with no movement keeps the heading it arrived with.
    let heading = null;
    legs.forEach((leg) => {
      if (leg.heading == null) leg.heading = heading;
      else heading = leg.heading;
    });
    for (let i = legs.length - 1, next = null; i >= 0; i -= 1) {
      if (legs[i].heading == null) legs[i].heading = next;
      else next = legs[i].heading;
    }
    model.lons = lons;
    model.latlngs = latlngs;
    model.legs = legs;
    model.route.setLatLngs(latlngs);
    model.flownIndex = -2;
  };

  const styleModel = (model) => {
    const focused = focusId == null || focusId === model.track.id;
    model.route.setStyle({ color: model.color, opacity: focused ? 0.45 : 0.18, weight: 2 });
    model.flown.setStyle({ color: model.color, opacity: focused ? 1 : 0.35, weight: focused ? 3.5 : 2.5 });
    const icon = model.marker.getElement();
    if (icon) {
      icon.style.setProperty("--craft", model.color);
      icon.classList.toggle("is-dimmed", !focused);
    }
  };

  const addModel = (track, index) => {
    const lineOptions = { pane: "cinc-tracks", renderer: renderers["cinc-tracks"], interactive: false };
    const model = {
      track,
      color: trackColor(track, index),
      route: L.polyline([], { ...lineOptions }).addTo(map),
      flown: L.polyline([], { ...lineOptions }).addTo(map),
      marker: L.marker([0, 0], {
        keyboard: false,
        title: track.label || track.id,
        icon: L.divIcon({
          className: "map-craft",
          iconSize: [22, 22],
          iconAnchor: [11, 11],
          html:
            '<svg class="map-craft-arrow" viewBox="-11 -11 22 22" aria-hidden="true">' +
            '<path d="M0,-9 L6.5,7.5 L0,3.5 L-6.5,7.5 Z"/></svg><span class="map-craft-label"></span>',
        }),
      }),
      current: null,
    };
    model.marker.addTo(map);
    const icon = model.marker.getElement();
    icon.querySelector(".map-craft-label").textContent = track.label || track.id;
    icon.classList.add("is-hidden");
    model.marker.on("click", () => focusTrack(focusId === track.id ? null : track.id));
    buildGeometry(model);
    styleModel(model);
    models.set(track.id, model);
  };

  const syncTracks = () => {
    readTracks().forEach((track, index) => {
      const model = models.get(track.id);
      if (!model) {
        addModel(track, index);
        return;
      }
      if (model.track === track) return;
      model.track = track;
      buildGeometry(model);
    });
    const previousEnd = domain.end;
    domain = timeDomain(readTracks(), readEvents());
    // Stay pinned to "now" in live mode when the clock was at the end.
    if (currentMs >= previousEnd && !playing) currentMs = domain.end;
    renderLegend();
    ensureDetailCovers();
  };

  // State of a track at `ms`: { lat, lon (unwrapped), alt, heading, speed, index } or null.
  const stateAt = (model, ms) => {
    const samples = model.track.samples || [];
    const index = sampleIndexAt(samples, ms);
    if (index < 0) return null;
    const last = samples.length - 1;
    if (index === last) {
      if (ms !== samples[last][0] && last > 0) return null; // the track has ended
      const leg = model.legs[last - 1];
      return { lat: samples[last][1], lon: model.lons[last], alt: samples[last][3], heading: leg?.heading ?? null, speed: 0, index };
    }
    const a = samples[index];
    const b = samples[index + 1];
    const leg = model.legs[index];
    const f = b[0] === a[0] ? 0 : (ms - a[0]) / (b[0] - a[0]);
    const [lat, lon] = along(a[1], model.lons[index], b[1], model.lons[index + 1], leg.dPsi, f);
    return { lat, lon, alt: a[3] + (b[3] - a[3]) * f, heading: leg.heading, speed: leg.speed, index };
  };

  const updateAtTime = () => {
    if (!map) return;
    models.forEach((model) => {
      const state = stateAt(model, currentMs);
      const icon = model.marker.getElement();
      model.current = state;
      if (!state) {
        icon?.classList.add("is-hidden");
        const index = sampleIndexAt(model.track.samples || [], currentMs);
        model.flown.setLatLngs(index >= 0 ? model.latlngs.slice(0, index + 1) : []);
        model.flownIndex = -2;
        return;
      }
      const position = [state.lat, state.lon];
      model.marker.setLatLng(position);
      if (icon) {
        icon.classList.remove("is-hidden");
        const arrow = icon.querySelector(".map-craft-arrow");
        arrow.style.transform = `rotate(${state.heading ?? 0}deg)`;
        arrow.classList.toggle("is-still", state.heading == null);
      }
      // Rebuild the flown line only when a new sample is passed; otherwise move its tip.
      if (model.flownIndex !== state.index) {
        model.flownLatLngs = model.latlngs.slice(0, state.index + 1);
        model.flownLatLngs.push(position);
        model.flownIndex = state.index;
      } else {
        model.flownLatLngs[model.flownLatLngs.length - 1] = position;
      }
      model.flown.setLatLngs(model.flownLatLngs);
    });
    followFocused();
  };

  // While playing with a track focused, keep it on screen.
  const followFocused = () => {
    if (!playing || focusId == null) return;
    const state = models.get(focusId)?.current;
    if (!state) return;
    const position = L.latLng(state.lat, state.lon);
    if (!map.getBounds().pad(-0.2).contains(position)) map.panTo(position, { animate: true, duration: 0.5 });
  };

  const renderClock = () => {
    const span = domain.end - domain.start;
    slider.max = String(span);
    slider.value = String(Math.min(span, Math.max(0, currentMs - domain.start)));
    clockLabel.textContent = formatClock(currentMs);
    legend.querySelectorAll("[data-track]").forEach((row) => {
      const state = models.get(row.dataset.track)?.current;
      const parts = state
        ? [
            `${state.lat.toFixed(4)}°, ${wrapLon(state.lon).toFixed(4)}°`,
            state.heading == null ? null : `${pad(Math.round(state.heading) % 360, 3)}°`,
            `${Math.round(state.speed * MS_TO_KT)} kt`,
            formatAlt(state.alt),
          ]
        : ["—"];
      row.querySelector(".map-legend-pos").textContent = parts.filter(Boolean).join(" · ");
    });
  };

  const setTime = (ms, { fromMap = false } = {}) => {
    if (!Number.isFinite(ms)) return;
    currentMs = ms;
    updateAtTime();
    renderClock();
    if (fromMap) driveLog(ms);
  };

  const renderLegend = () => {
    legend.innerHTML = "";
    models.forEach((model) => {
      const row = element("button", `map-legend-row${focusId === model.track.id ? " is-focused" : ""}`);
      row.type = "button";
      row.dataset.track = model.track.id;
      row.title = "Focus this track (click again to show all)";
      const swatch = element("span", "map-legend-swatch");
      swatch.style.background = model.color;
      row.append(swatch, element("span", "map-legend-name", model.track.label || model.track.id), element("span", "map-legend-pos", "—"));
      row.addEventListener("click", () => focusTrack(focusId === model.track.id ? null : model.track.id));
      legend.appendChild(row);
    });
    renderClock();
  };

  const fitView = (trackId = focusId, animate = true) => {
    if (!map) return;
    const size = map.getSize();
    if (!size.x || !size.y) return;
    const points = [];
    models.forEach((model) => {
      if (trackId == null || model.track.id === trackId) points.push(...model.latlngs);
    });
    if (!points.length) return;
    const bounds = L.latLngBounds(points);
    const legendWidth = Math.min(legend.offsetWidth + 16, size.x / 3);
    map.fitBounds(bounds, { paddingTopLeft: [legendWidth, 40], paddingBottomRight: [40, 40], maxZoom: 12, animate });
    fitted = true;
  };

  const focusTrack = (trackId) => {
    focusId = trackId;
    models.forEach(styleModel);
    renderLegend();
    fitView(trackId);
  };

  const applyTheme = () => {
    if (!map) return;
    styleBasemap();
    let index = 0;
    models.forEach((model) => {
      if (!model.track.color) model.color = trackColor(model.track, index);
      styleModel(model);
      index += 1;
    });
    renderLegend();
  };

  // Click near a track: move the clock (and the log) to that point of the track.
  const pickTime = (containerPoint) => {
    let best = null;
    models.forEach((model) => {
      if (focusId != null && focusId !== model.track.id) return;
      const samples = model.track.samples || [];
      const points = model.latlngs.map((latlng) => map.latLngToContainerPoint(latlng));
      points.forEach((point, i) => {
        const distance = point.distanceTo(containerPoint);
        if (distance <= PICK_RADIUS_PX && (!best || distance < best.distance)) best = { distance, ms: samples[i][0] };
        if (i === 0) return;
        const a = points[i - 1];
        const dx = point.x - a.x;
        const dy = point.y - a.y;
        const lengthSq = dx * dx + dy * dy;
        if (!lengthSq) return;
        const t = Math.max(0, Math.min(1, ((containerPoint.x - a.x) * dx + (containerPoint.y - a.y) * dy) / lengthSq));
        const onLine = L.point(a.x + t * dx, a.y + t * dy).distanceTo(containerPoint);
        if (onLine > PICK_RADIUS_PX || (best && onLine >= best.distance)) return;
        // t runs along the straight (Mercator) line; turn it back into time along the leg.
        const s0 = samples[i - 1];
        const s1 = samples[i];
        const leg = model.legs[i - 1];
        let f = t;
        if (Math.abs(leg.dPsi) > 1e-12 && s1[1] !== s0[1]) {
          const lat = invMercY(mercY(s0[1]) + t * leg.dPsi);
          f = (lat - s0[1]) / (s1[1] - s0[1]);
        }
        best = { distance: onLine, ms: s0[0] + f * (s1[0] - s0[0]) };
      });
    });
    return best?.ms ?? null;
  };

  // -- playback
  const stop = () => {
    playing = false;
    if (frameHandle) cancelAnimationFrame(frameHandle);
    frameHandle = 0;
    commandRefresh?.();
  };
  const play = () => {
    if (playing) return;
    if (currentMs >= domain.end) currentMs = domain.start;
    playing = true;
    let last = performance.now();
    const step = (now) => {
      if (!playing) return;
      const next = currentMs + (now - last) * SPEEDS[speedIndex];
      last = now;
      if (next >= domain.end) {
        setTime(domain.end, { fromMap: true });
        stop();
        return;
      }
      setTime(next, { fromMap: true });
      frameHandle = requestAnimationFrame(step);
    };
    frameHandle = requestAnimationFrame(step);
    commandRefresh?.();
  };

  slider.addEventListener("input", () => {
    stop();
    setTime(domain.start + Number(slider.value), { fromMap: true });
  });

  // -- incoming sync (log -> map)
  const followLog = (ms) => {
    if (playing || performance.now() < quietUntil || !Number.isFinite(ms)) return;
    setTime(ms);
  };
  if (store && typeof signalEffect === "function") {
    off.push(signalEffect(() => followLog(store.logScroll?.value?.timeMs)));
    off.push(signalEffect(() => followLog(Date.parse(store.selectedEvent?.value?.time || ""))));
    off.push(
      signalEffect(() => {
        store.tracksSignal?.value;
        if (!map) return;
        syncTracks();
        updateAtTime();
        renderClock();
      })
    );
  }

  // -- layer toggles (a small Leaflet control)
  const layerControl = () => {
    const Control = L.Control.extend({
      options: { position: "topright" },
      onAdd() {
        const box = element("div", "leaflet-bar map-layers");
        OVERLAYS.forEach(({ id, label }) => {
          const row = element("label", "map-layers-row");
          const input = element("input");
          input.type = "checkbox";
          input.checked = overlayOn(id);
          input.addEventListener("change", () => setOverlay(id, input.checked));
          row.append(input, element("span", null, label));
          box.appendChild(row);
        });
        L.DomEvent.disableClickPropagation(box);
        L.DomEvent.disableScrollPropagation(box);
        return box;
      },
    });
    return new Control();
  };

  // -- build the map once Leaflet is loaded
  const buildMap = (basemapData) => {
    map = L.map(host, {
      zoomControl: false,
      attributionControl: false,
      minZoom: 2,
      maxZoom: 13,
      zoomSnap: 0.25,
      zoomDelta: 0.5,
      wheelPxPerZoomLevel: 90,
      worldCopyJump: false,
    });
    PANES.forEach(([name, zIndex]) => {
      const pane = map.createPane(name);
      pane.style.zIndex = String(zIndex);
      if (name !== "cinc-tracks") pane.style.pointerEvents = "none";
      // Extra canvas around the view so panning does not show blank edges.
      renderers[name] = L.canvas({ pane: name, padding: name === "cinc-tracks" ? 0.1 : 0.5 });
    });
    L.control.zoom({ position: "topright" }).addTo(map);
    layerControl().addTo(map);
    L.control.scale({ position: "bottomleft", imperial: false }).addTo(map);
    L.control.attribution({ position: "bottomright", prefix: "Leaflet" }).addAttribution("Natural Earth").addTo(map);
    map.setView([20, 0], 2);

    addBasemap(basemapData);
    map.on("zoomend moveend", layoutLabels);
    map.on("click", (event) => {
      const ms = pickTime(event.containerPoint);
      if (ms == null) return;
      stop();
      setTime(ms, { fromMap: true });
    });
    off.push(onThemeChange(applyTheme));

    status.hidden = true;
    syncTracks();
    applyTheme();
    const scroll = store?.logScroll?.value?.timeMs;
    setTime(Number.isFinite(scroll) ? scroll : domain.start);
    fitView(null, false);
  };

  Promise.all([loadLeaflet(), fetchBasemap(trackBounds(readTracks())).catch((error) => {
    console.warn("Map: could not load the basemap", error);
    return null;
  })])
    .then(([loaded, basemapData]) => {
      if (destroyed) return;
      L = loaded;
      try {
        buildMap(basemapData);
      } catch (error) {
        console.error(error);
        status.hidden = false;
        status.textContent = `Map unavailable: ${error?.message || error}`;
      }
    })
    .catch((error) => {
      if (!destroyed) status.textContent = `Map unavailable: ${error?.message || error}`;
    });

  return {
    // Toolbar: play/pause, speed, fit.
    renderCommands(container) {
      const group = element("div", "chart-view-switcher map-commands");
      const playButton = element("button", "button button-ghost button-xs");
      playButton.type = "button";
      const speedButton = element("button", "button button-ghost button-xs");
      speedButton.type = "button";
      speedButton.title = "Playback speed";
      const fitButton = element("button", "button button-ghost button-xs", "Fit");
      fitButton.type = "button";
      fitButton.title = "Zoom to the tracks";
      const refresh = () => {
        playButton.textContent = playing ? "Pause" : "Play";
        playButton.classList.toggle("is-active", playing);
        speedButton.textContent = `${SPEEDS[speedIndex]}×`;
      };
      playButton.addEventListener("click", () => (playing ? stop() : play()));
      speedButton.addEventListener("click", () => {
        speedIndex = (speedIndex + 1) % SPEEDS.length;
        refresh();
      });
      fitButton.addEventListener("click", () => fitView());
      group.append(playButton, speedButton, fitButton);
      container.appendChild(group);
      commandRefresh = refresh;
      refresh();
      return () => {
        if (commandRefresh === refresh) commandRefresh = null;
      };
    },
    activate() {
      if (!map) return;
      map.invalidateSize();
      if (!fitted) fitView(null, false);
      setTime(currentMs);
    },
    deactivate() {
      stop();
    },
    resize() {
      if (!map) return;
      map.invalidateSize();
      if (!fitted) fitView(null, false);
    },
    destroy() {
      destroyed = true;
      stop();
      if (jumpTimer) clearTimeout(jumpTimer);
      off.forEach((unsubscribe) => unsubscribe && unsubscribe());
      map?.remove();
      map = null;
      models.clear();
      labels.length = 0;
      basemapLayers.clear();
    },
  };
};

LogMainViewChart.registerType({
  id: "map",
  label: "Map",
  // Only offered when the log has position tracks.
  available: (services) =>
    (services?.viewerStore?.tracksSignal?.value || services?.logData?.tracks || []).length > 0,
  renderPanel(panel, context) {
    return createMapController(panel, context);
  },
  buildCommands(container, context) {
    return context.panelController?.renderCommands?.(container) || null;
  },
});
