import { queryById } from "../../../shared.js";
import { Cinc } from "../../../runtime.js";
import { themeColor, themeFont, onThemeChange } from "../../../theme.js";

const LogMainViewChart = window.LogMainViewChart || (window.LogMainViewChart = {});
const LogMainViewTimeline = window.LogMainViewTimeline || (window.LogMainViewTimeline = {});
import { signals } from "logview/lib";

const signalEffect = signals?.effect || null;

const normalizePluginId = (id) => String(id || "").trim().toLowerCase().replace(/_/g, "-") || null;

LogMainViewChart.registry = LogMainViewChart.registry || new Map();
LogMainViewTimeline.registry = LogMainViewTimeline.registry || new Map();

LogMainViewChart.buildRegistryKey = (pluginId, chartId) => {
  const pluginPart = pluginId == null ? "global" : String(pluginId).trim() || "global";
  return `${pluginPart}::${String(chartId).trim()}`;
};
LogMainViewTimeline.buildRegistryKey = LogMainViewChart.buildRegistryKey;

LogMainViewChart.registerType = (definition) => {
  const id = String(definition?.id || "").trim();
  if (!id) throw new Error("Chart types must define an id.");
  const pluginId = definition?.pluginId == null ? null : String(definition.pluginId).trim() || null;
  LogMainViewChart.registry.set(LogMainViewChart.buildRegistryKey(pluginId, id), {
    ...definition,
    id,
    pluginId,
    label: String(definition.label || id),
  });
};

LogMainViewChart.registerPluginType = (pluginId, definition) => {
  const normalizedPluginId = String(pluginId || "").trim();
  if (!normalizedPluginId) throw new Error("Plugin chart types must define a plugin id.");
  LogMainViewChart.registerType({ ...definition, pluginId: normalizedPluginId });
};

LogMainViewTimeline.registerView = (definition) => {
  const id = String(definition?.id || "").trim();
  if (!id) throw new Error("Timeline views must define an id.");
  const pluginId = definition?.pluginId == null ? null : String(definition.pluginId).trim() || null;
  LogMainViewTimeline.registry.set(LogMainViewTimeline.buildRegistryKey(pluginId, id), {
    ...definition,
    id,
    pluginId,
    label: String(definition.label || id),
    kind: String(definition.kind || "histogram"),
  });
};

LogMainViewTimeline.registerPluginView = (pluginId, definition) => {
  const normalizedPluginId = String(pluginId || "").trim();
  if (!normalizedPluginId) throw new Error("Plugin timeline views must define a plugin id.");
  LogMainViewTimeline.registerView({ ...definition, pluginId: normalizedPluginId });
};

// -- slots: where a chart can be shown -------------------------------------------------------------
// "main" is the chart area above the log; "side" is the dock in the right pane, under the row details.
// A chart lists the slots it supports in `slots` (default ["main"]); the first is where it starts, and
// the user can move it to another of its slots (stored in viewerStore.chartPlacement).
const SLOTS = ["main", "side"];
LogMainViewChart.slotsOf = (type) => {
  const slots = (Array.isArray(type?.slots) ? type.slots : ["main"]).filter((slot) => SLOTS.includes(slot));
  return slots.length ? slots : ["main"];
};
LogMainViewChart.placementOf = (type, placement = {}) => {
  const slots = LogMainViewChart.slotsOf(type);
  const chosen = placement?.[type?.id];
  return slots.includes(chosen) ? chosen : slots[0];
};
// Charts (not timeline views) offered for this page: global ones, plus those of log types present.
LogMainViewChart.availableCharts = (services) => {
  const { logData, presentTypes } = services || {};
  const present = new Set((presentTypes || []).map((id) => normalizePluginId(id)).filter(Boolean));
  const declared = normalizePluginId(logData?.logTypeId || logData?.pluginId);
  if (declared) present.add(declared);
  return Array.from(LogMainViewChart.registry.values()).filter(
    (type) =>
      (!type.pluginId || present.has(normalizePluginId(type.pluginId))) &&
      (typeof type.available !== "function" || type.available(services))
  );
};
LogMainViewChart.chartsIn = (slot, services) =>
  LogMainViewChart.availableCharts(services).filter(
    (type) => LogMainViewChart.placementOf(type, services?.viewerStore?.chartPlacement?.value) === slot
  );

// Timeline views for a log with these types: the global ones, then each type's own (first id wins).
LogMainViewTimeline.listFor = (typeIds = []) => {
  const present = new Set((typeIds || []).map((id) => normalizePluginId(id)).filter(Boolean));
  const views = Array.from(LogMainViewTimeline.registry.values()).filter(
    (view) => !view.pluginId || present.has(normalizePluginId(view.pluginId))
  );
  return views
    .filter((view, index) => views.findIndex((other) => other.id === view.id) === index)
    .map((view) => ({ id: view.id, label: view.label }));
};

if (Cinc._pendingViewRegistrations.length) {
  const pending = Cinc._pendingViewRegistrations.splice(0);
  pending.forEach(({ kind, pluginId, definition }) => {
    if ((kind || "chart") === "timeline") {
      if (pluginId == null) LogMainViewTimeline.registerView(definition);
      else LogMainViewTimeline.registerPluginView(pluginId, definition);
      return;
    }
    if (pluginId == null) LogMainViewChart.registerType(definition);
    else LogMainViewChart.registerPluginType(pluginId, definition);
  });
}

LogMainViewChart.mount = (root, services) => {
  const { logData, logTypes, presentTypes, viewerStore } = services;
  if (!logData) return null;

  const chartRegion = queryById(root, "chart-region");
  const chartPanelHost = queryById(root, "chart-panel-host");
  if (!chartRegion || !chartPanelHost) return null;

  const resolvedLogTypeId =
    logData?.logTypeId ||
    logData?.pluginId ||
    (presentTypes?.length === 1 ? presentTypes[0] : null);
  const logTypeConfig = logTypes?.[resolvedLogTypeId] || null;
  const logType = logTypeConfig
    ? { id: resolvedLogTypeId, ...logTypeConfig }
    : null;
  const activePluginId = normalizePluginId(
    logType?.id ||
      logData?.logTypeId ||
      logData?.pluginId ||
      (presentTypes?.length === 1 ? presentTypes[0] : null)
  );
  const availablePluginIds = new Set(
    (presentTypes || []).map((id) => normalizePluginId(id)).filter(Boolean)
  );
  if (activePluginId) availablePluginIds.add(activePluginId);
  const mountedPanels = new Map();
  let activeType = null;
  let activePanel = null;
  let cleanupCommands = null;
  let commandBar = null;

  const listTypes = () => {
    const chartTypes = LogMainViewChart.chartsIn("main", services);
    const timelineTypes = Array.from(LogMainViewTimeline.registry.values())
      .filter(
        (view) =>
          !view.pluginId || availablePluginIds.has(normalizePluginId(view.pluginId))
      )
      .map((view) => ({
        ...view,
        id: `timeline-${view.id}`,
        renderPanel(panel, context) {
          const controller = createTimelineChartController(panel, {
            ...context,
            timelinePluginId: view.pluginId || null,
            availablePluginIds,
          });
          controller.setView(view.id);
          return controller;
        },
      }));
    return [...chartTypes, ...timelineTypes];
  };

  const buildContext = (extra = {}) => ({
    root,
    slot: "main",
    plugin: logType,
    logType,
    viewerStore,
    logData,
    bookmarks: viewerStore,
    comments: viewerStore,
    chartRegion,
    chartPanelHost,
    controller,
    ...extra,
  });

  const ensurePanel = (type) => {
    if (!type) return null;
    if (mountedPanels.has(type.id)) return mountedPanels.get(type.id);

    const panel = document.createElement("div");
    panel.className = "chart-panel";
    panel.dataset.chartType = type.id;
    chartPanelHost.appendChild(panel);

    if (typeof type.panelHtml === "string" && type.panelHtml.trim()) {
      panel.innerHTML = type.panelHtml;
    }

    let panelController = {};
    if (typeof type.renderPanel === "function") {
      panelController = type.renderPanel(panel, buildContext({ type, panel })) || {};
    } else if (typeof type.mountPanel === "function") {
      panelController = type.mountPanel(buildContext({ type, panel })) || {};
    }

    const record = { panel, type, controller: panelController || {} };
    mountedPanels.set(type.id, record);
    return record;
  };

  const clearCommandBar = () => {
    if (cleanupCommands) {
      cleanupCommands();
      cleanupCommands = null;
    }
    if (commandBar) commandBar.innerHTML = "";
  };

  const renderNoTypesMessage = () => {
    if (!commandBar) return;
    commandBar.innerHTML = '<div class="chart-command-hint">No charts registered for this plugin.</div>';
  };

  const renderCommands = (type, panelRecord) => {
    clearCommandBar();
    if (!commandBar || !type || !panelRecord) return;
    const buildCommands = type.buildCommands || type.renderControls;
    if (typeof buildCommands !== "function") return;
    cleanupCommands =
      buildCommands(
        commandBar,
        buildContext({
          type,
          panel: panelRecord.panel,
          panelController: panelRecord.controller || null,
        })
      ) || null;
  };

  const controller = {
    currentType: "timeline",
    chart: null,
    listTypes,
    getCurrentType() {
      return controller.currentType;
    },
    getActivePanel() {
      return activePanel;
    },
    getActivePanelController() {
      return activePanel?.controller || null;
    },
    setType(typeId) {
      const availableTypes = controller.listTypes();
      const fallbackType = availableTypes[0] || null;
      const type =
        availableTypes.find((entry) => entry.id === typeId) ||
        availableTypes.find((entry) => entry.id === controller.currentType) ||
        fallbackType;
      if (!type) {
        controller.currentType = "";
        renderNoTypesMessage();
        return;
      }

      clearCommandBar();

      if (activePanel?.controller && typeof activePanel.controller.deactivate === "function") {
        activePanel.controller.deactivate(buildContext({ type: activeType, panel: activePanel.panel }));
      }
      if (activeType && typeof activeType.deactivate === "function") {
        activeType.deactivate(
          buildContext({
            type: activeType,
            panel: activePanel?.panel,
            panelController: activePanel?.controller || null,
          })
        );
      }

      const panelRecord = ensurePanel(type);
      if (!panelRecord) return;

      controller.currentType = type.id;
      activeType = type;
      activePanel = panelRecord;
      controller.chart = panelRecord.controller?.chart || null;

      mountedPanels.forEach(({ panel }, panelId) => {
        panel.classList.toggle("is-active", panelId === type.id);
      });

      const context = buildContext({
        type,
        panel: panelRecord.panel,
        panelController: panelRecord.controller || null,
      });

      if (typeof type.activate === "function") type.activate(context);
      if (panelRecord.controller && typeof panelRecord.controller.activate === "function") {
        panelRecord.controller.activate(context);
      }

      renderCommands(type, panelRecord);
      controller.resize();
    },
    // After charts move between slots: drop panels of charts no longer here; returns the new list.
    refreshTypes() {
      const types = controller.listTypes();
      const ids = new Set(types.map((type) => type.id));
      mountedPanels.forEach((record, id) => {
        if (ids.has(id)) return;
        if (record === activePanel) {
          clearCommandBar();
          activePanel = null;
          activeType = null;
        }
        record.controller?.destroy?.(buildContext({ type: record.type, panel: record.panel, panelController: record.controller }));
        record.panel.remove();
        mountedPanels.delete(id);
      });
      return types;
    },
    attachToolbar({ commandBarEl = null } = {}) {
      commandBar = commandBarEl || commandBar;

      const types = controller.listTypes();
      if (!types.length) {
        renderNoTypesMessage();
        return;
      }

      if (activeType && activePanel) {
        renderCommands(activeType, activePanel);
      }
    },
    bindToolbar() {
      controller.attachToolbar({
        commandBarEl: queryById(root, "chart-command-bar"),
      });
      const types = controller.listTypes();
      if (!types.length) return;
      controller.setType(
        types.some((type) => type.id === controller.currentType) ? controller.currentType : types[0].id
      );
    },
    resize() {
      if (activePanel?.controller && typeof activePanel.controller.resize === "function") {
        activePanel.controller.resize(
          buildContext({
            type: activeType,
            panel: activePanel.panel,
            panelController: activePanel.controller,
          })
        );
      }
    },
    destroy() {
      clearCommandBar();
      mountedPanels.forEach(({ controller: panelController, panel, type }) => {
        const context = buildContext({ type, panel, panelController });
        if (panelController && typeof panelController.destroy === "function") {
          panelController.destroy(context);
        }
      });
      mountedPanels.clear();
      chartPanelHost.innerHTML = "";
      controller.chart = null;
      activePanel = null;
      activeType = null;
    },
  };

  return controller;
};

// The side dock: one collapsible section per chart placed in the "side" slot, each with the chart's own
// commands in its header. Panels get context.slot = "side" so they can lay themselves out vertically.
LogMainViewChart.mountSide = (host, services) => {
  const { viewerStore, logData } = services || {};
  const sections = new Map(); // chart id -> { type, section, body, commands, controller, cleanup, observer, open }
  const buildContext = (extra = {}) => ({
    root: host,
    slot: "side",
    viewerStore,
    logData,
    bookmarks: viewerStore,
    comments: viewerStore,
    ...extra,
  });
  const contextFor = (record) =>
    buildContext({ type: record.type, panel: record.body, panelController: record.controller || null });

  const createSection = (type) => {
    const section = document.createElement("section");
    section.className = "side-section";
    section.dataset.chart = type.id;
    const header = document.createElement("div");
    header.className = "side-section-header";
    const toggle = document.createElement("button");
    toggle.type = "button";
    toggle.className = "side-section-toggle";
    toggle.title = "Show or hide this chart";
    toggle.innerHTML = '<span class="side-section-caret" aria-hidden="true">▾</span><span class="side-section-label"></span>';
    toggle.querySelector(".side-section-label").textContent = type.label;
    toggle.addEventListener("click", () => {
      viewerStore?.setSideCollapsed?.(type.id, !viewerStore?.sideCollapsed?.value?.includes(type.id));
    });
    const commands = document.createElement("div");
    commands.className = "side-section-commands";
    header.append(toggle, commands);
    if (LogMainViewChart.slotsOf(type).includes("main")) {
      const move = document.createElement("button");
      move.type = "button";
      move.className = "button button-ghost button-xs side-section-move";
      move.title = "Move to the chart area";
      move.setAttribute("aria-label", "Move to the chart area");
      move.innerHTML =
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true">' +
        '<rect x="3.5" y="4.5" width="17" height="15" rx="1.5"/><path d="M3.5 10h17"/></svg>';
      move.addEventListener("click", () => {
        viewerStore?.setChartPlacement?.(type.id, "main");
        viewerStore?.setChartType?.(type.id);
      });
      header.appendChild(move);
    }
    const body = document.createElement("div");
    body.className = "chart-panel is-active side-section-body";
    section.append(header, body);
    const record = { type, section, body, commands, controller: null, cleanup: null, observer: null, open: false };
    sections.set(type.id, record);
    return record;
  };

  const open = (record) => {
    if (record.open) return;
    record.open = true;
    if (!record.controller) {
      record.controller = record.type.renderPanel?.(record.body, contextFor(record)) || {};
      const buildCommands = record.type.buildCommands || record.type.renderControls;
      if (typeof buildCommands === "function") record.cleanup = buildCommands(record.commands, contextFor(record)) || null;
      if (typeof ResizeObserver === "function") {
        record.observer = new ResizeObserver(() => record.controller?.resize?.(contextFor(record)));
        record.observer.observe(record.body);
      }
    }
    record.controller?.activate?.(contextFor(record));
  };

  const close = (record) => {
    if (!record.open) return;
    record.open = false;
    record.controller?.deactivate?.(contextFor(record));
  };

  const destroySection = (id) => {
    const record = sections.get(id);
    if (!record) return;
    close(record);
    record.observer?.disconnect();
    if (typeof record.cleanup === "function") record.cleanup();
    record.controller?.destroy?.(contextFor(record));
    record.section.remove();
    sections.delete(id);
  };

  return {
    // Match the sections to the charts placed in the side slot; returns how many there are.
    sync() {
      const types = LogMainViewChart.chartsIn("side", services);
      const ids = new Set(types.map((type) => type.id));
      Array.from(sections.keys()).forEach((id) => {
        if (!ids.has(id)) destroySection(id);
      });
      const collapsed = new Set(viewerStore?.sideCollapsed?.value || []);
      types.forEach((type) => {
        const record = sections.get(type.id) || createSection(type);
        host.appendChild(record.section); // keeps registration order
        const isCollapsed = collapsed.has(type.id);
        record.section.classList.toggle("is-collapsed", isCollapsed);
        if (isCollapsed) close(record);
        else open(record);
      });
      return types.length;
    },
    resize() {
      sections.forEach((record) => record.open && record.controller?.resize?.(contextFor(record)));
    },
    destroy() {
      Array.from(sections.keys()).forEach(destroySection);
    },
  };
};

const getEventAbsoluteMs = (event) => {
  const time = Date.parse(event?.time || "");
  return Number.isFinite(time) ? time : null;
};

const getTimelineBounds = (logData, events) => {
  const configStart = new Date(logData?.start).getTime();
  const configEnd = new Date(logData?.end).getTime();
  const hasConfigStart = Number.isFinite(configStart);
  const hasConfigEnd = Number.isFinite(configEnd);

  // Use absolute UTC timestamps from events when config bounds are missing.
  const firstAbsMs = events.length > 0 ? getEventAbsoluteMs(events[0]) : null;
  const lastAbsMs = events.length > 0 ? getEventAbsoluteMs(events[events.length - 1]) : null;

  const startMs = hasConfigStart ? configStart
    : firstAbsMs != null ? firstAbsMs
    : firstAbsMs ?? 0;

  let endMs = hasConfigEnd ? configEnd
    : lastAbsMs != null ? lastAbsMs
    : startMs + 1000;

  // For live sessions (no config end), add headroom so the X-axis doesn't
  // rescale on every incoming event — it only steps forward when rebuilt.
  if (!hasConfigEnd && events.length > 0) {
    const span = endMs - startMs;
    endMs += Math.max(8000, span * 0.15);
  }

  return { startMs, endMs, spanMs: Math.max(1, endMs - startMs) };
};

const buildTimelineHelpers = ({ logData, allEvents, panel }) => {
  const { startMs, endMs, spanMs } = getTimelineBounds(logData, allEvents);
  const width = Math.max(320, Math.round(panel.getBoundingClientRect().width || 640));
  const defaultBucketCount = Math.max(24, Math.min(160, Math.ceil(width / 10)));
  const getEventMs = (event) => {
    const time = Date.parse(event?.time || "");
    return Number.isFinite(time) ? time : NaN;
  };
  const eventToSeconds = (event) => (getEventMs(event) - startMs) / 1000;
  const buildBucketPoints = (sourceEvents, spec = {}, reducer = null) => {
    const bucketCount = Math.max(2, Number(spec.bucketCount) || defaultBucketCount);
    const bucketMs = Math.max(1, Math.ceil(spanMs / bucketCount));
    const bins = new Array(bucketCount).fill(0);
    sourceEvents.forEach((event) => {
      if (typeof spec.filter === "function" && !spec.filter(event)) return;
      const index = Math.min(
        bucketCount - 1,
        Math.max(0, Math.floor((getEventMs(event) - startMs) / bucketMs))
      );
      const value =
        typeof reducer === "function"
          ? reducer(event, index)
          : typeof spec.value === "function"
            ? Number(spec.value(event)) || 0
            : 1;
      bins[index] += value;
    });
    return bins.map((y, index) => ({
      x: ((index + 0.5) * bucketMs) / 1000,
      y,
    }));
  };
  return {
    startMs,
    endMs,
    spanMs,
    getEventMs,
    eventToSeconds,
    defaultBucketCount,
    buildBucketPoints,
  };
};

const buildTimelineDatasets = (view, context) => {
  const sourceEvents = Array.isArray(context.filteredEvents) ? context.filteredEvents : context.allEvents;
  if (typeof view.buildDatasets === "function") {
    return view.buildDatasets({
      ...context,
      events: sourceEvents,
    }) || [];
  }

  const specs = Array.isArray(view.datasets) ? view.datasets : [];
  const kind = view.kind || "histogram";
  return specs.map((spec) => {
    const color = spec.color || "var(--chart-bar)";
    const backgroundColor = themeColor(spec.backgroundColor || color);
    const borderColor = themeColor(spec.borderColor || color);
    if (kind === "scatter") {
      const data = sourceEvents
        .filter((event) => (typeof spec.filter === "function" ? spec.filter(event) : true))
        .map((event) => ({
          x: context.helpers.eventToSeconds(event),
          y: typeof spec.value === "function" ? Number(spec.value(event)) || 0 : 1,
        }));
      return {
        label: spec.label,
        type: "scatter",
        data,
        parsing: false,
        showLine: false,
        backgroundColor,
        borderColor,
        pointRadius: spec.pointRadius ?? 2,
      };
    }

    // A line with a `value` plots each event's value (a measurement); otherwise counts per time bucket.
    const data =
      kind === "line" && typeof spec.value === "function"
        ? sourceEvents
            .filter((event) => (typeof spec.filter === "function" ? spec.filter(event) : true))
            .map((event) => ({ x: context.helpers.eventToSeconds(event), y: Number(spec.value(event)) }))
            .filter((point) => Number.isFinite(point.x) && Number.isFinite(point.y))
            .sort((a, b) => a.x - b.x)
        : context.helpers.buildBucketPoints(sourceEvents, spec);
    if (kind === "line") {
      return {
        label: spec.label,
        type: "line",
        data,
        parsing: false,
        backgroundColor,
        borderColor,
        borderWidth: spec.borderWidth ?? 1.5,
        fill: spec.fill ?? false,
        tension: spec.tension ?? 0.18,
        pointRadius: spec.pointRadius ?? 0,
      };
    }
    // Histogram: solid, square bars with a clear gap between buckets. Stacked segments are split by a
    // hairline in the ground colour, so adjacent severities don't run together.
    return {
      label: spec.label,
      type: "bar",
      data,
      parsing: false,
      backgroundColor,
      borderColor: view.stacked ? themeColor("var(--chart-gap)") : backgroundColor,
      borderWidth: view.stacked ? { top: 1, right: 0, bottom: 0, left: 0 } : 0,
      borderSkipped: false,
      borderRadius: 0,
      barPercentage: spec.barPercentage ?? 0.78,
      categoryPercentage: 1,
    };
  });
};

const createTimelineChartController = (panel, context) => {
  // Use a getter so we always read the latest events — supports live streaming.
  const getAllEvents = () => {
    const events = Array.isArray(context.logData?.events) ? context.logData.events : [];
    const typeId =
      context.logType?.id || context.plugin?.id || context.type?.pluginId;
    return typeId
      ? events.filter((event) => !event.log_type || event.log_type === typeId)
      : events;
  };
  let allEvents = getAllEvents();
  let filteredEvents = Array.isArray(context.viewerStore?.filteredEvents?.value)
    ? context.viewerStore.filteredEvents.value
    : allEvents;
  let selectedRowId = context.viewerStore?.selectedEvent?.value?.row_id ?? null;
  let currentViewId = context.viewerStore?.timelineView?.value || "events";
  let resizeObserver = null;
  let liveUpdateTimer = null;
  const debugTimeline =
    window.localStorage?.getItem("loglayout.debug.timeline") === "true" ||
    window.EVENTLOG2_DEBUG_TIMELINE === true;

  const listViews = () => {
    const availablePluginIds =
      context.availablePluginIds ||
      new Set(
        getAllEvents()
          .map((event) => normalizePluginId(event.log_type))
          .filter(Boolean)
      );
    if (availablePluginIds && availablePluginIds.size > 1) {
      return Array.from(LogMainViewTimeline.registry.values()).filter(
        (view) =>
          !view.pluginId ||
          availablePluginIds.has(normalizePluginId(view.pluginId))
      );
    }
    const activeId = normalizePluginId(
      context.logType?.id ||
        context.plugin?.id ||
        context.timelinePluginId ||
        context.type?.pluginId ||
        context.logData?.logTypeId ||
        context.logData?.pluginId
    );
    return Array.from(LogMainViewTimeline.registry.values()).filter(
      (view) => !view.pluginId || normalizePluginId(view.pluginId) === activeId
    );
  };

  const ensureCurrentView = () => {
    const views = listViews();
    return (
      views.find((view) => view.id === currentViewId) ||
      views.find((view) => view.default) ||
      views[0] ||
      null
    );
  };

  panel.innerHTML = '<canvas class="timeline-strip-canvas"></canvas>';
  panel.style.position = "relative";
  const canvas = panel.querySelector("canvas");
  const hoverOverlay = document.createElement("div");
  hoverOverlay.className = "timeline-hover-overlay";
  hoverOverlay.hidden = true;
  hoverOverlay.innerHTML = `
    <div class="timeline-hover-line"></div>
    <div class="timeline-hover-label"></div>
  `;
  panel.appendChild(hoverOverlay);
  const hoverLabel = hoverOverlay.querySelector(".timeline-hover-label");
  // Mutable helpers — rebuilt periodically in live mode so spanMs/startMs extend
  // as new events arrive without rescaling on every single incoming event.
  let helpers = buildTimelineHelpers({ logData: context.logData, allEvents, panel });

  const rebuildHelpers = () => {
    allEvents = getAllEvents();
    const last = allEvents.length > 0 ? allEvents[allEvents.length - 1] : null;
    const lastAbsMs = last ? getEventAbsoluteMs(last) : null;
    const configEndMs = new Date(context.logData?.end).getTime();
    // If live events have arrived after the initial logData.end (which was
    // fixed at page-load time), strip the end so getTimelineBounds derives it
    // from current events and adds headroom — preventing late events from
    // piling into the final bucket.
    const logDataForBounds =
      Number.isFinite(configEndMs) && lastAbsMs !== null && lastAbsMs > configEndMs
        ? { ...context.logData, end: undefined }
        : context.logData;
    helpers = buildTimelineHelpers({ logData: logDataForBounds, allEvents, panel });
    chart.options.scales.x.max = helpers.spanMs / 1000;
  };

  // Throttle chart redraws during live streaming: accumulate events for up to
  // LIVE_INTERVAL ms, then do one rebuild + redraw. Avoids constant axis rescaling.
  const LIVE_INTERVAL = 3000;
  const scheduleUpdate = () => {
    if (liveUpdateTimer !== null) return;
    liveUpdateTimer = setTimeout(() => {
      liveUpdateTimer = null;
      rebuildHelpers();
      applyView(currentViewId);
    }, LIVE_INTERVAL);
  };

  let hoverSeconds = null;

  const genericPlugins = {
    markerPlugin: {
      id: "timelineMarkers",
      afterDatasetsDraw(chart) {
        const { ctx, chartArea } = chart;
        const bookmarkKeys = context.bookmarks?.getAll() || [];
        const commentMap = context.comments?.getByRowKey() || new Map();
        const commentKeys = Array.from(commentMap.keys());
        const markers = [];
        const markerRadius = 4;
        const drawMarker = (rowKey, y, fillStyle) => {
          const event = context.bookmarks?.eventForKey?.(rowKey);
          if (!event) return;
          const rowId = event.row_id;
          const x = chart.scales.x.getPixelForValue(helpers.eventToSeconds(event));
          ctx.save();
          ctx.beginPath();
          ctx.fillStyle = fillStyle;
          ctx.arc(x, y, markerRadius, 0, Math.PI * 2);
          ctx.fill();
          if (String(selectedRowId) === String(rowId)) {
            ctx.lineWidth = 1.5;
            ctx.strokeStyle = themeColor("var(--ink)");
            ctx.stroke();
          }
          ctx.restore();
          markers.push({ x, y, rowId });
        };
        bookmarkKeys.forEach((rowKey) => {
          const colorIndex = context.bookmarks?.getColor(rowKey) || 1;
          drawMarker(rowKey, chartArea.top + 8, themeColor(`var(--bookmark-color-${colorIndex})`));
        });
        commentKeys.forEach((rowKey) => drawMarker(rowKey, chartArea.bottom - 8, themeColor("var(--chart-tick)")));
        chart.$timelineMarkers = markers;
      },
    },
    scrollPlugin: {
      id: "timelineScroll",
      afterDatasetsDraw(chart) {
        if (typeof chart.$scrollSeconds !== "number") return;
        const x = chart.scales.x.getPixelForValue(chart.$scrollSeconds);
        const { ctx, chartArea } = chart;
        ctx.save();
        ctx.beginPath();
        ctx.moveTo(x, chartArea.top);
        ctx.lineTo(x, chartArea.bottom);
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 4]);
        ctx.strokeStyle = themeColor("var(--accent)");
        ctx.stroke();
        ctx.restore();
      },
    },
  };

  const chart = new Chart(canvas.getContext("2d"), {
    type: "bar",
    data: { datasets: [] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: { mode: "index", intersect: true },
      plugins: {
        legend: { display: false },
        tooltip: { mode: "index", intersect: true, displayColors: true },
      },
      layout: {
        padding: { top: 2, bottom: 4, left: 4, right: 8 },
      },
      scales: {
        x: {
          type: "linear",
          min: 0,
          max: helpers.spanMs / 1000,
          offset: false,
          bounds: "data",
          grid: { display: false, drawBorder: false, offset: false },
          border: { display: true, color: themeColor("var(--chart-grid)") },
          ticks: {
            maxTicksLimit: 6,
            includeBounds: true,
            maxRotation: 0,
            padding: 4,
            callback(value) {
              return new Date(helpers.startMs + Number(value) * 1000).toISOString().slice(11, 19);
            },
          },
        },
        y: {
          beginAtZero: true,
          grace: "20%",
          border: { display: false },
          grid: { drawTicks: false, lineWidth: 1 },
          ticks: { precision: 0, maxTicksLimit: 4, padding: 6 },
        },
      },
      onClick(event) {
        const pos = Chart.helpers.getRelativePosition(event, chart);
        const markers = chart.$timelineMarkers || [];
        const hit = markers.find((marker) => {
          const dx = pos.x - marker.x;
          const dy = pos.y - marker.y;
          return Math.sqrt(dx * dx + dy * dy) <= 8;
        });
        if (hit) {
          const selected = allEvents.find((entry) => String(entry.row_id) === String(hit.rowId));
          if (selected) {
            context.viewerStore?.setSelectedEvent(selected);
          }
          context.viewerStore?.setLogJump({ rowId: hit.rowId });
          return;
        }
        const seconds = chart.scales.x.getValueForPixel(pos.x);
        if (Number.isFinite(seconds)) {
          const payload = { timeMs: helpers.startMs + seconds * 1000 };
          context.viewerStore?.setLogJump(payload);
        }
      },
      onHover(event) {
        const pos = Chart.helpers.getRelativePosition(event, chart);
        const { chartArea, scales } = chart;
        if (debugTimeline) {
          console.debug("[timeline:hover:event]", {
            type: event?.type,
            pos,
            chartArea,
          });
        }
        if (
          pos.x < chartArea.left ||
          pos.x > chartArea.right ||
          pos.y < chartArea.top ||
          pos.y > chartArea.bottom
        ) {
          if (hoverSeconds !== null) {
            hoverSeconds = null;
            if (debugTimeline) console.debug("[timeline:hover:clear]", { reason: "outside-chart" });
            syncHoverOverlay();
          }
          return;
        }
        const seconds = scales.x.getValueForPixel(pos.x);
        const nextHover = Number.isFinite(seconds) ? seconds : null;
        if (nextHover !== hoverSeconds) {
          if (debugTimeline) {
            console.debug("[timeline:hover:update]", {
              previous: hoverSeconds,
              next: nextHover,
            });
          }
          hoverSeconds = nextHover;
          syncHoverOverlay();
        }
      },
    },
    plugins: [genericPlugins.markerPlugin, genericPlugins.scrollPlugin],
  });

  const syncHoverOverlay = () => {
    if (typeof hoverSeconds !== "number") {
      hoverOverlay.hidden = true;
      return;
    }
    const { chartArea, scales } = chart;
    if (!chartArea || !scales?.x) {
      hoverOverlay.hidden = true;
      return;
    }
    const x = scales.x.getPixelForValue(hoverSeconds);
    if (x < chartArea.left || x > chartArea.right) {
      hoverOverlay.hidden = true;
      return;
    }
    hoverOverlay.hidden = false;
    hoverOverlay.style.left = `${x}px`;
    hoverOverlay.style.top = `${chartArea.top}px`;
    hoverOverlay.style.height = `${chartArea.bottom - chartArea.top}px`;
    hoverLabel.textContent = new Date(helpers.startMs + hoverSeconds * 1000).toISOString().slice(11, 19);
    if (debugTimeline) {
      console.debug("[timeline:hover:overlay]", {
        hoverSeconds,
        x,
        chartArea,
      });
    }
  };

  const clearHover = () => {
    if (hoverSeconds === null) return;
    hoverSeconds = null;
    if (debugTimeline) console.debug("[timeline:hover:clear]", { reason: "mouseleave" });
    syncHoverOverlay();
  };
  canvas.addEventListener("mouseleave", clearHover);

  if (debugTimeline) {
    window.__cincTimelineDebug = {
      chart,
      canvas,
      panel,
      helpers,
      get hoverSeconds() {
        return hoverSeconds;
      },
      get currentViewId() {
        return currentViewId;
      },
    };
    console.debug("[timeline:init]", {
      currentViewId,
      eventCount: allEvents.length,
      spanSeconds: helpers.spanMs / 1000,
      chartArea: chart.chartArea,
    });
  }

  let activeViewPlugins = [];
  const applyView = (viewId = currentViewId) => {
    const view = listViews().find((entry) => entry.id === viewId) || ensureCurrentView();
    if (!view) return;
    currentViewId = view.id;
    context.viewerStore?.setTimelineView?.(currentViewId);

    const timelineContext = {
      allEvents,
      filteredEvents,
      logData: context.logData,
      bookmarks: context.bookmarks,
      comments: context.comments,
      chart,
      helpers,
    };
    const datasets = buildTimelineDatasets(view, timelineContext);
    chart.config.type = view.kind === "scatter" ? "scatter" : view.kind === "line" ? "line" : "bar";
    chart.data.datasets = datasets;
    chart.options.plugins.legend.display = datasets.length > 1;
    const tick = themeColor("var(--chart-tick)");
    chart.options.color = tick;
    chart.options.font = { family: themeFont(), size: 11 };
    const mono = getComputedStyle(document.documentElement).getPropertyValue("--font-mono").trim();
    chart.options.scales.x.ticks.color = tick;
    chart.options.scales.x.ticks.font = { family: mono, size: 11 };
    chart.options.scales.x.border.color = themeColor("var(--chart-grid)");
    chart.options.scales.y.ticks = { ...(chart.options.scales.y.ticks || {}), color: tick, font: { family: mono, size: 11 } };
    chart.options.scales.y.grid.color = themeColor("var(--chart-grid)");
    chart.options.plugins.legend.labels = { color: tick, boxWidth: 8, boxHeight: 8, padding: 14, font: { family: themeFont(), size: 11 } };
    chart.options.scales.y.stacked = Boolean(view.stacked);
    chart.options.scales.x.stacked = Boolean(view.stacked);
    chart.options.scales.x.min = 0;
    chart.options.scales.x.max = helpers.spanMs / 1000;
    chart.options.scales.y.min = undefined;
    chart.options.scales.y.max = undefined;
    chart.options.scales.y.suggestedMin = undefined;
    chart.options.scales.y.suggestedMax = undefined;
    chart.options.scales.y.beginAtZero = true;
    chart.options.scales.y.grace = "10%";

    while (chart.config.plugins.length > 2) chart.config.plugins.pop();
    activeViewPlugins = typeof view.buildPlugins === "function" ? view.buildPlugins(timelineContext) || [] : [];
    activeViewPlugins.forEach((plugin) => chart.config.plugins.push(plugin));

    if (typeof view.configureChart === "function") {
      view.configureChart(chart, timelineContext);
    }

    chart.update();
    syncHoverOverlay();
  };

  const renderViewButtons = (container) => {
    const views = listViews();
    container.innerHTML = "";
    if (!views.length) {
      container.innerHTML = '<div class="chart-command-hint">No timeline views registered.</div>';
      return;
    }
    const group = document.createElement("div");
    group.className = "chart-view-switcher";
    views.forEach((view) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = `button button-ghost button-xs${view.id === currentViewId ? " is-active" : ""}`;
      button.textContent = view.label;
      button.addEventListener("click", () => {
        applyView(view.id);
        renderViewButtons(container);
      });
      group.appendChild(button);
    });
    container.appendChild(group);
  };

  const off = [];
  off.push(onThemeChange(() => applyView(currentViewId)));
  if (context.viewerStore && typeof signalEffect === "function") {
    off.push(
      signalEffect(() => {
        const newFiltered = Array.isArray(context.viewerStore.filteredEvents?.value)
          ? context.viewerStore.filteredEvents.value
          : getAllEvents();
        const currentAllEvents = getAllEvents();
        const hasNewEvents = currentAllEvents.length > allEvents.length;
        filteredEvents = newFiltered;
        if (hasNewEvents) {
          // New events arrived (live streaming): throttle to avoid rescaling
          // the time axis on every incoming batch.
          scheduleUpdate();
        } else {
          // Filter changed by user: update immediately.
          applyView(currentViewId);
        }
      })
    );
    off.push(
      signalEffect(() => {
        const payload = context.viewerStore.logScroll?.value || null;
        chart.$scrollSeconds =
          payload && typeof payload.timeMs === "number"
            ? (payload.timeMs - helpers.startMs) / 1000
            : undefined;
        chart.update("none");
        syncHoverOverlay();
      })
    );
    off.push(
      signalEffect(() => {
        context.viewerStore.bookmarks?.value;
        chart.update("none");
        syncHoverOverlay();
      })
    );
    off.push(
      signalEffect(() => {
        context.viewerStore.comments?.value;
        chart.update("none");
        syncHoverOverlay();
      })
    );
    off.push(
      signalEffect(() => {
        selectedRowId = context.viewerStore.selectedEvent?.value?.row_id ?? null;
        chart.update("none");
        syncHoverOverlay();
      })
    );
    off.push(
      signalEffect(() => {
        const nextViewId = context.viewerStore.timelineView?.value || currentViewId;
        if (nextViewId !== currentViewId) {
          applyView(nextViewId);
        }
      })
    );
  }

  if (typeof ResizeObserver === "function") {
    resizeObserver = new ResizeObserver(() => chart.resize());
    resizeObserver.observe(context.chartRegion);
    resizeObserver.observe(panel);
  } else {
    window.addEventListener("resize", () => chart.resize());
  }

  applyView(currentViewId);

  return {
    chart,
    listViews,
    getCurrentViewId() {
      return currentViewId;
    },
    setView(viewId) {
      applyView(viewId);
    },
    renderViewButtons,
    activate() {
      chart.resize();
      chart.update("none");
      syncHoverOverlay();
    },
    resize() {
      chart.resize();
      chart.update("none");
      syncHoverOverlay();
    },
    destroy() {
      off.forEach((unsubscribe) => unsubscribe && unsubscribe());
      if (liveUpdateTimer !== null) { clearTimeout(liveUpdateTimer); liveUpdateTimer = null; }
      if (resizeObserver) resizeObserver.disconnect();
      canvas.removeEventListener("mouseleave", clearHover);
      chart.destroy();
    },
  };
};

LogMainViewChart.registerType({
  id: "timeline",
  label: "Timeline",
  renderPanel(panel, context) {
    return createTimelineChartController(panel, context);
  },
});

LogMainViewTimeline.registerView({
  id: "events",
  label: "Events",
  default: true,
  kind: "histogram",
  datasets: [
    {
      label: "Events",
      backgroundColor: "var(--chart-bar)",
      borderColor: "var(--chart-bar-line)",
    },
  ],
});
