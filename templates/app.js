{# JS bundle rendered via Jinja to keep components co-located #}
{% include 'components/layout.js' %}

{% include 'components/log_view.js' %}

{% include 'components/top_chart.js' %}

{% include 'components/search_pane.js' %}

{% include 'components/right_pane.js' %}

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

window.addEventListener("DOMContentLoaded", () => {
  const bus = LogApp.createEventBus();
  const logData = LogApp.loadLogData();

  LogApp.initSplits();
  LogApp.initLogList(logData, bus);
  LogApp.initChart(logData, bus);
  LogApp.initSearchPane(logData, bus);
  LogApp.initRightPane(bus);
});
