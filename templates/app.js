{# JS bundle rendered via Jinja to keep components co-located #}
{% include 'components/layout.js' %}

{% include 'components/log_view.js' %}

{% include 'components/top_chart.js' %}

{% include 'components/search_pane.js' %}

{% include 'components/right_pane.js' %}

{% include 'components/search_utils.js' %}

window.LogApp = window.LogApp || {};

LogApp.STORAGE_KEYS = {
  root: "loglayout.split.root",
  top: "loglayout.split.top",
  bottomCollapsed: "loglayout.split.bottom.collapsed",
  search: "loglayout.split.search",
  rootExpanded: "loglayout.split.root.expanded",
  searchHistory: "loglayout.search.history",
  searchPinned: "loglayout.search.pinned",
  searchFilters: "loglayout.search.filters",
  chartTooltips: "loglayout.chart.tooltips",
  bookmarks: "loglayout.bookmarks",
  bookmarkNotes: "loglayout.bookmark.notes",
};

LogApp.createEventBus = () => {
  const listeners = new Map();
  return {
    on(event, handler) {
      if (!listeners.has(event)) listeners.set(event, new Set());
      listeners.get(event).add(handler);
      return () => listeners.get(event)?.delete(handler);
    },
    emit(event, payload) {
      const handlers = listeners.get(event);
      if (!handlers) return;
      handlers.forEach((handler) => handler(payload));
    },
  };
};

LogApp.loadSizes = (key, fallback) => {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return fallback;
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed) || parsed.length !== fallback.length) return fallback;
    if (parsed.some((value) => typeof value !== "number")) return fallback;
    return parsed;
  } catch (err) {
    return fallback;
  }
};

LogApp.saveSizes = (key, sizes) => {
  localStorage.setItem(key, JSON.stringify(sizes));
};

LogApp.loadLogData = () => {
  const logPayload = document.getElementById("log-data");
  if (!logPayload) return null;
  try {
    return JSON.parse(logPayload.textContent || "{}");
  } catch (err) {
    return null;
  }
};

LogApp.smoothScrollTo = (container, targetTop, durationMs = 200, onComplete = null) => {
  const startTop = container.scrollTop;
  const delta = targetTop - startTop;
  if (Math.abs(delta) < 2) {
    container.scrollTop = targetTop;
    if (onComplete) onComplete();
    return;
  }
  const startTime = performance.now();
  const easeOut = (t) => 1 - Math.pow(1 - t, 3);
  const step = (now) => {
    const elapsed = now - startTime;
    const progress = Math.min(1, elapsed / durationMs);
    container.scrollTop = startTop + delta * easeOut(progress);
    if (progress < 1) {
      requestAnimationFrame(step);
    } else if (onComplete) {
      onComplete();
    }
  };
  requestAnimationFrame(step);
};

LogApp.createBookmarkStore = (events = []) => {
  const validIds = new Set(events.map((event) => String(event.row_id)));
  const load = () => {
    try {
      const raw = localStorage.getItem(LogApp.STORAGE_KEYS.bookmarks);
      const parsed = raw ? JSON.parse(raw) : {};
      if (Array.isArray(parsed)) {
        const legacy = {};
        parsed.forEach((id) => {
          const key = String(id);
          if (validIds.has(key)) legacy[key] = 1;
        });
        return legacy;
      }
      if (parsed && typeof parsed === "object") {
        const sanitized = {};
        Object.entries(parsed).forEach(([key, value]) => {
          const id = String(key);
          if (!validIds.has(id)) return;
          const index = Number(value) || 0;
          sanitized[id] = Math.max(0, Math.min(5, index));
        });
        return sanitized;
      }
      return {};
    } catch (err) {
      return {};
    }
  };
  let bookmarks = load();
  const save = () => {
    localStorage.setItem(
      LogApp.STORAGE_KEYS.bookmarks,
      JSON.stringify(bookmarks)
    );
  };
  const cycle = (rowId) => {
    const key = String(rowId);
    if (!validIds.has(key)) return 0;
    const current = Number(bookmarks[key]) || 0;
    const next = (current + 1) % 6;
    if (next === 0) {
      delete bookmarks[key];
    } else {
      bookmarks[key] = next;
    }
    save();
    return next;
  };
  const setColor = (rowId, colorIndex) => {
    const key = String(rowId);
    if (!validIds.has(key)) return 0;
    const next = Math.max(0, Math.min(5, Number(colorIndex) || 0));
    if (next === 0) {
      delete bookmarks[key];
    } else {
      bookmarks[key] = next;
    }
    save();
    return next;
  };
  const getColor = (rowId) => Number(bookmarks[String(rowId)]) || 0;
  const isBookmarked = (rowId) => getColor(rowId) > 0;
  const getAll = () => Object.keys(bookmarks);
  const getAllWithColors = () => ({ ...bookmarks });
  return { cycle, setColor, getColor, isBookmarked, getAll, getAllWithColors };
};

window.addEventListener("DOMContentLoaded", () => {
  const bus = LogApp.createEventBus();
  const logData = LogApp.loadLogData();
  LogApp.searchWorker = LogApp.createSearchWorker(logData?.events || []);
  LogApp.bookmarks = LogApp.createBookmarkStore(logData?.events || []);

  LogApp.initSplits();
  LogApp.initLogList(logData, bus);
  LogApp.initChart(logData, bus);
  LogApp.initSearchPane(logData, bus);
  LogApp.initRightPane(bus);
});
