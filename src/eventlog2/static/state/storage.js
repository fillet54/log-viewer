import { effect } from "preact/signals";

export const STORAGE_KEYS = {
  root: "loglayout.split.root",
  top: "loglayout.split.top",
  detailCollapsed: "loglayout.split.detail.collapsed",
  bottomCollapsed: "loglayout.split.bottom.collapsed",
  search: "loglayout.split.search",
  rootExpanded: "loglayout.split.root.expanded",
  searchHistory: "loglayout.search.history",
  searchPinned: "loglayout.search.pinned",
  searchFilters: "loglayout.search.filters",
  searchTab: "loglayout.search.tab",
  chartTooltips: "loglayout.chart.tooltips",
  chartType: "loglayout.chart.type",
  chartTimelineView: "loglayout.timeline.view",
  mainViewMode: "loglayout.mainview.mode",
  mainViewSplit: "loglayout.mainview.split",
  bookmarks: "loglayout.bookmarks",
  comments: "loglayout.comments",
};

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
