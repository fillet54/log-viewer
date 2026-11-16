import { effect } from "preact/signals";

export const STORAGE_KEYS = {
  // v2: the dense layout's defaults; v1 sizes would otherwise override them
  root: "loglayout.v2.split.root",
  top: "loglayout.v2.split.top",
  detailCollapsed: "loglayout.v2.split.detail.collapsed",
  bottomCollapsed: "loglayout.v2.split.bottom.collapsed",
  search: "loglayout.v2.split.search",
  rootExpanded: "loglayout.v2.split.root.expanded",
  density: "loglayout.density",
  timeMode: "loglayout.time.mode",
  searchHistory: "loglayout.search.history",
  searchPinned: "loglayout.search.pinned",
  searchFilters: "loglayout.search.filters",
  searchTab: "loglayout.search.tab",
  chartTooltips: "loglayout.chart.tooltips",
  chartType: "loglayout.chart.type",
  chartTimelineView: "loglayout.timeline.view",
  chartPlacement: "loglayout.chart.placement",
  sideCollapsed: "loglayout.side.collapsed",
  rightSplit: "loglayout.v2.split.right",
  mainViewMode: "loglayout.mainview.mode",
  mainViewSplit: "loglayout.v2.mainview.split",
  // Per log: the log id is appended (see activityKeys).
  bookmarks: "loglayout.bookmarks",
  comments: "loglayout.comments",
};

// Bookmarks and comments are kept per log, under "<key>.<log id>".
export const activityKeys = (logId) => ({
  bookmarks: `${STORAGE_KEYS.bookmarks}.${logId}`,
  comments: `${STORAGE_KEYS.comments}.${logId}`,
});

// Before they were per log, bookmarks and comments lived under the bare keys
// and every log overwrote the others'. Drop them.
export function removeLegacyActivityKeys() {
  try {
    localStorage.removeItem(STORAGE_KEYS.bookmarks);
    localStorage.removeItem(STORAGE_KEYS.comments);
  } catch {
    // ignore
  }
}

export function readStorage(key, fallback, options = {}) {
  try {
    const raw = localStorage.getItem(key);
    if (raw == null) return fallback;
    const parse = typeof options.parse === "function" ? options.parse : JSON.parse;
    return parse(raw);
  } catch {
    return fallback;
  }
}

export function writeStorage(key, value, options = {}) {
  try {
    const serialize = typeof options.serialize === "function" ? options.serialize : JSON.stringify;
    localStorage.setItem(key, serialize(value));
  } catch {
    // ignore
  }
}

export function syncSignalToStorage(key, sig, options = {}) {
  effect(() => writeStorage(key, sig.value, options));
}

export function setSignal(sig, nextValue, { normalize = (v) => v, equals = Object.is, afterChange = null } = {}) {
  const raw = typeof nextValue === "function" ? nextValue(sig.value) : nextValue;
  const next = normalize(raw);
  if (equals(sig.value, next)) return sig.value;
  sig.value = next;
  if (typeof afterChange === "function") afterChange(next);
  return sig.value;
}

export const BOOLEAN_STORAGE = {
  parse: (raw) => raw === true || raw === "true",
  serialize: (value) => (value ? "true" : "false"),
};

export const STRING_STORAGE = {
  parse: (raw) => {
    if (typeof raw !== "string") return String(raw || "");
    try {
      const parsed = JSON.parse(raw);
      return typeof parsed === "string" ? parsed : String(parsed || "");
    } catch {
      return String(raw || "");
    }
  },
  serialize: (value) => String(value || ""),
};

export const OBJECT_STORAGE = {
  parse: (raw) => {
    if (raw && typeof raw === "object" && !Array.isArray(raw)) return raw;
    if (typeof raw !== "string") return {};
    try {
      const parsed = JSON.parse(raw);
      return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
    } catch {
      return {};
    }
  },
  serialize: (value) => JSON.stringify(value && typeof value === "object" ? value : {}),
};

export const ARRAY_STORAGE = {
  parse: (raw) => {
    if (Array.isArray(raw)) return raw;
    if (typeof raw !== "string") return [];
    try {
      const parsed = JSON.parse(raw);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  },
  serialize: (value) => JSON.stringify(Array.isArray(value) ? value : []),
};
