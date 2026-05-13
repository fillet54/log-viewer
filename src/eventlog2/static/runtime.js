import * as preact from "preact";
import * as hooks from "preact/hooks";
import * as signals from "preact/signals";
import { html } from "htm/preact";

const globalObject = window;
const existingEventLog2 = globalObject.EventLog2 || {};
const rowRenderers = existingEventLog2._rowRenderers || new Map();
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

const existingUi = globalObject.EventLog2UI || {};

export const EventLog2 = {
  ...existingEventLog2,
  _rowRenderers: rowRenderers,
  _pendingViewRegistrations: pendingViewRegistrations,
  registerPluginRowRenderer(pluginId, renderer) {
    const normalizedPluginId = String(pluginId || "").trim();
    if (!normalizedPluginId) throw new Error("Plugin row renderers must define a plugin id.");
    if (typeof renderer !== "function") throw new Error("Plugin row renderers must be functions.");
    rowRenderers.set(normalizedPluginId, renderer);
    return renderer;
  },
  resolveRowRenderer(plugin) {
    const pluginId =
      plugin && typeof plugin === "object"
        ? String(plugin.id || "").trim()
        : String(plugin || "").trim();
    return rowRenderers.get(pluginId) || null;
  },
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

export const EventLog2UI = {
  ...existingUi,
  html: html,
  render: preact.render,
  hydrate: preact.hydrate,
  createElement: preact.h,
  createContext: preact.createContext,
  Fragment: preact.Fragment,
  hooks: {
    useState: hooks.useState,
    useEffect: hooks.useEffect,
    useLayoutEffect: hooks.useLayoutEffect,
    useMemo: hooks.useMemo,
    useRef: hooks.useRef,
    useContext: hooks.useContext,
    useReducer: hooks.useReducer,
    useCallback: hooks.useCallback,
  },
  signals: {
    signal: signals.signal,
    computed: signals.computed,
    effect: signals.effect,
    batch: signals.batch,
    untracked: signals.untracked,
    useSignal: signals.useSignal,
    useComputed: signals.useComputed,
    useSignalEffect: signals.useSignalEffect,
    Signal: signals.Signal,
  },
  available: true,
  signalsAvailable: true,
};

globalObject.EventLog2UI = EventLog2UI;

