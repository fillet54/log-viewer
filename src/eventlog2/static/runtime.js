(function () {
  const globalObject = window;
  const existingEventLog2 = globalObject.EventLog2 || {};
  const rowRenderers = existingEventLog2._rowRenderers || new Map();
  const pendingViewRegistrations = existingEventLog2._pendingViewRegistrations || [];
  
  globalObject.STORAGE_KEYS = {
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

  const existingUi = globalObject.EventLog2UI || {};
  const preactApi = globalObject.preact || {};
  const hooksApi = globalObject.preactHooks || {};
  const htmApi = globalObject.htm || {};
  const signalsApi = globalObject.preactSignals || {};

  const bindHtml = () => {
    if (typeof htmApi.bind === "function" && typeof preactApi.h === "function") {
      return htmApi.bind(preactApi.h);
    }
    return function missingRuntime() {
      throw new Error(
        "EventLog2UI.html requires local Preact vendor files. Replace the placeholder vendor files with real browser builds."
      );
    };
  };

  const noopRender = function missingRender() {
    throw new Error(
      "EventLog2UI.render requires local Preact vendor files. Replace the placeholder vendor files with real browser builds."
    );
  };

  globalObject.EventLog2 = {
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

  globalObject.EventLog2UI = {
    ...existingUi,
    html: bindHtml(),
    render: typeof preactApi.render === "function" ? preactApi.render.bind(preactApi) : noopRender,
    hydrate: typeof preactApi.hydrate === "function" ? preactApi.hydrate.bind(preactApi) : noopRender,
    createElement: typeof preactApi.createElement === "function" ? preactApi.createElement.bind(preactApi) : preactApi.h,
    createContext: typeof preactApi.createContext === "function" ? preactApi.createContext.bind(preactApi) : null,
    Fragment: preactApi.Fragment || null,
    hooks: {
      useState: hooksApi.useState || null,
      useEffect: hooksApi.useEffect || null,
      useLayoutEffect: hooksApi.useLayoutEffect || null,
      useMemo: hooksApi.useMemo || null,
      useRef: hooksApi.useRef || null,
      useContext: hooksApi.useContext || null,
      useReducer: hooksApi.useReducer || null,
      useCallback: hooksApi.useCallback || null,
    },
    signals: {
      signal: signalsApi.signal || null,
      computed: signalsApi.computed || null,
      effect: signalsApi.effect || null,
      batch: signalsApi.batch || null,
      untracked: signalsApi.untracked || null,
      useSignal: signalsApi.useSignal || null,
      useComputed: signalsApi.useComputed || null,
      useSignalEffect: signalsApi.useSignalEffect || null,
      Signal: signalsApi.Signal || null,
    },
    available:
      typeof preactApi.h === "function" &&
      typeof preactApi.createContext === "function" &&
      typeof preactApi.render === "function" &&
      typeof htmApi.bind === "function" &&
      typeof hooksApi.useContext === "function" &&
      typeof hooksApi.useEffect === "function" &&
      typeof hooksApi.useLayoutEffect === "function" &&
      typeof hooksApi.useMemo === "function" &&
      typeof hooksApi.useRef === "function" &&
      typeof hooksApi.useState === "function",
    signalsAvailable: typeof signalsApi.signal === "function",
  };

  if (!globalObject.EventLog2UI.available) {
    console.warn(
      "EventLog2UI loaded without full Preact vendor files. The current app will still run, but Preact components cannot mount yet."
    );
  }
})();
