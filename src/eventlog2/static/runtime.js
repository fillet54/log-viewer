import { effect } from "preact/signals";
import { html } from "htm/preact";

const globalObject = window;
const existingEventLog2 = globalObject.EventLog2 || {};
const rowComponents = existingEventLog2._rowComponents || new Map();
const pendingViewRegistrations = existingEventLog2._pendingViewRegistrations || [];

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

globalObject.STORAGE_KEYS = STORAGE_KEYS;

export const EventLog2 = {
  ...existingEventLog2,
  _rowComponents: rowComponents,
  _pendingViewRegistrations: pendingViewRegistrations,
  registerPluginRowComponent(pluginId, Component) {
    const normalizedPluginId = String(pluginId || "").trim();
    if (!normalizedPluginId) throw new Error("Plugin row components must define a plugin id.");
    if (typeof Component !== "function") throw new Error("Plugin row components must be functions.");
    rowComponents.set(normalizedPluginId, Component);
    return Component;
  },
  resolveRowComponent(plugin) {
    const pluginId =
      plugin && typeof plugin === "object"
        ? String(plugin.id || "").trim()
        : String(plugin || "").trim();
    return rowComponents.get(pluginId) || null;
  },
  html,
  signalEffect: effect,
  registerPluginChartType(pluginId, definition) {
    const normalizedPluginId = String(pluginId || "").trim();
    if (!normalizedPluginId) throw new Error("Plugin chart types must define a plugin id.");
    const chartRegistry = globalObject.LogMainViewChart || null;
    if (chartRegistry && typeof chartRegistry.registerPluginType === "function") {
      return chartRegistry.registerPluginType(normalizedPluginId, definition);
    }
    pendingViewRegistrations.push({ kind: "chart", pluginId: normalizedPluginId, definition });
    return definition;
  },
  registerPluginTimelineView(pluginId, definition) {
    const normalizedPluginId = String(pluginId || "").trim();
    if (!normalizedPluginId) throw new Error("Plugin timeline views must define a plugin id.");
    const timelineRegistry = globalObject.LogMainViewTimeline || null;
    if (timelineRegistry && typeof timelineRegistry.registerPluginView === "function") {
      return timelineRegistry.registerPluginView(normalizedPluginId, definition);
    }
    pendingViewRegistrations.push({ kind: "timeline", pluginId: normalizedPluginId, definition });
    return definition;
  },
};

globalObject.EventLog2 = EventLog2;
