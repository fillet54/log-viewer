window.LogMainViewChart = window.LogMainViewChart || {};
window.LogMainViewTimeline = window.LogMainViewTimeline || {};
const signalEffect = window.EventLog2UI?.signals?.effect || null;

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

if (window.EventLog2?._pendingViewRegistrations?.length) {
  const pending = window.EventLog2._pendingViewRegistrations.splice(0);
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
  const { logData, bus, bookmarks, comments, plugin, viewerStore } = services;
  if (!logData) return null;

  const chartRegion = queryById(root, "chart-region");
  const chartPanelHost = queryById(root, "chart-panel-host");
  if (!chartRegion || !chartPanelHost) return null;

  const activePluginId = String(plugin?.id || logData?.pluginId || "").trim() || null;
  const mountedPanels = new Map();
  let activeType = null;
  let activePanel = null;
  let cleanupCommands = null;
  let commandBar = null;

  const listTypes = () =>
    Array.from(LogMainViewChart.registry.values()).filter((type) => !type.pluginId || type.pluginId === activePluginId);

  const buildContext = (extra = {}) => ({
    root,
    plugin,
    bus,
    viewerStore,
    logData,
    bookmarks,
    comments,
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

const getTimelineBounds = (logData, events) => {
  const start = new Date(logData?.start).getTime();
  const end = new Date(logData?.end).getTime();
  const fallbackStart = Number(events[0]?.norm_time || 0) * 1000;
  const fallbackEnd = Number(events[events.length - 1]?.norm_time || 1) * 1000;
  const startMs = Number.isFinite(start) ? start : fallbackStart;
  const endMs = Number.isFinite(end) ? end : fallbackEnd;
  return { startMs, endMs, spanMs: Math.max(1, endMs - startMs) };
};

const buildTimelineHelpers = ({ logData, allEvents, panel }) => {
  const { startMs, endMs, spanMs } = getTimelineBounds(logData, allEvents);
  const width = Math.max(320, Math.round(panel.getBoundingClientRect().width || 640));
  const defaultBucketCount = Math.max(24, Math.min(160, Math.ceil(width / 10)));
  const getEventMs = (event) => {
    const utc = new Date(event?.utctime).getTime();
    if (Number.isFinite(utc)) return utc;
    return Number(event?.norm_time || 0) * 1000;
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
    const color = spec.color || "rgba(59, 130, 246, 0.55)";
    const backgroundColor = spec.backgroundColor || color;
    const borderColor = spec.borderColor || color;
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

    const data = context.helpers.buildBucketPoints(sourceEvents, spec);
    return {
      label: spec.label,
      type: kind === "line" ? "line" : "bar",
      data,
      parsing: false,
      backgroundColor,
      borderColor,
      borderWidth: spec.borderWidth ?? 1,
      fill: spec.fill ?? false,
      tension: spec.tension ?? 0.18,
      pointRadius: kind === "line" ? spec.pointRadius ?? 0 : spec.pointRadius,
      barPercentage: 1,
      categoryPercentage: 1,
      borderRadius: kind === "histogram" ? spec.borderRadius ?? 2 : spec.borderRadius,
    };
  });
};

const createTimelineChartController = (panel, context) => {
  const allEvents = Array.isArray(context.logData?.events) ? context.logData.events : [];
  let filteredEvents = Array.isArray(context.viewerStore?.filteredEvents?.value)
    ? context.viewerStore.filteredEvents.value
    : allEvents;
  let selectedRowId = context.viewerStore?.selectedEvent?.value?.row_id ?? null;
  let currentViewId = context.viewerStore?.timelineView?.value || "events";
  let resizeObserver = null;
  const debugTimeline =
    window.localStorage?.getItem("loglayout.debug.timeline") === "true" ||
    window.EVENTLOG2_DEBUG_TIMELINE === true;

  const listViews = () =>
    Array.from(LogMainViewTimeline.registry.values()).filter(
      (view) => !view.pluginId || view.pluginId === (String(context.plugin?.id || context.logData?.pluginId || "").trim() || null)
    );

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
  const helpers = buildTimelineHelpers({ logData: context.logData, allEvents, panel });
  let hoverSeconds = null;

  const genericPlugins = {
    markerPlugin: {
      id: "timelineMarkers",
      afterDatasetsDraw(chart) {
        const { ctx, chartArea } = chart;
        const bookmarkIds = context.bookmarks?.getAll() || [];
        const commentMap = context.comments?.getByRowId() || new Map();
        const commentIds = Array.from(commentMap.keys());
        const markers = [];
        const markerRadius = 4;
        const drawMarker = (rowId, y, fillStyle) => {
          const event = allEvents.find((entry) => String(entry.row_id) === String(rowId));
          if (!event) return;
          const x = chart.scales.x.getPixelForValue(helpers.eventToSeconds(event));
          ctx.save();
          ctx.beginPath();
          ctx.fillStyle = fillStyle;
          ctx.arc(x, y, markerRadius, 0, Math.PI * 2);
          ctx.fill();
          if (String(selectedRowId) === String(rowId)) {
            ctx.lineWidth = 1.5;
            ctx.strokeStyle = "rgba(15, 23, 42, 0.9)";
            ctx.stroke();
          }
          ctx.restore();
          markers.push({ x, y, rowId });
        };
        bookmarkIds.forEach((rowId) => {
          const colorIndex = context.bookmarks?.getColor(rowId) || 1;
          const fill =
            getComputedStyle(document.documentElement).getPropertyValue(`--bookmark-color-${colorIndex}`) ||
            "rgba(59, 130, 246, 0.95)";
          drawMarker(rowId, chartArea.top + 8, fill.trim());
        });
        commentIds.forEach((rowId) => drawMarker(rowId, chartArea.bottom - 8, "rgba(71, 85, 105, 0.9)"));
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
        ctx.strokeStyle = "rgba(30, 64, 175, 0.65)";
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
        padding: { top: 10, bottom: 8 },
      },
      scales: {
        x: {
          type: "linear",
          min: 0,
          max: helpers.spanMs / 1000,
          offset: false,
          bounds: "data",
          grid: { display: false, drawBorder: false, offset: false },
          ticks: {
            maxTicksLimit: 5,
            includeBounds: true,
            color: "rgba(71, 85, 105, 0.9)",
            callback(value) {
              return new Date(helpers.startMs + Number(value) * 1000).toISOString().slice(11, 19);
            },
          },
        },
        y: {
          beginAtZero: true,
          grace: "20%",
          grid: { color: "rgba(148, 163, 184, 0.16)", drawBorder: false },
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
            if (context.viewerStore) context.viewerStore.setSelectedEvent(selected);
            else context.bus.emit("event:selected", selected);
          }
          if (context.viewerStore) context.viewerStore.setLogJump({ rowId: hit.rowId });
          else context.bus.emit("log:jump", { rowId: hit.rowId });
          return;
        }
        const seconds = chart.scales.x.getValueForPixel(pos.x);
        if (Number.isFinite(seconds)) {
          const payload = { seconds: Math.max(0, Math.floor(seconds)) };
          if (context.viewerStore) context.viewerStore.setLogJump(payload);
          else context.bus.emit("log:jump", payload);
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
    window.__eventlog2TimelineDebug = {
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
    chart.options.scales.y.stacked = Boolean(view.stacked);
    chart.options.scales.x.stacked = Boolean(view.stacked);
    chart.options.scales.x.min = 0;
    chart.options.scales.x.max = helpers.spanMs / 1000;
    chart.options.scales.y.min = undefined;
    chart.options.scales.y.max = undefined;
    chart.options.scales.y.suggestedMin = undefined;
    chart.options.scales.y.suggestedMax = undefined;
    chart.options.scales.y.beginAtZero = true;
    chart.options.scales.y.grace = "20%";

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
  if (context.viewerStore && typeof signalEffect === "function") {
    off.push(
      signalEffect(() => {
        filteredEvents = Array.isArray(context.viewerStore.filteredEvents?.value)
          ? context.viewerStore.filteredEvents.value
          : allEvents;
        applyView(currentViewId);
      })
    );
    off.push(
      signalEffect(() => {
        const payload = context.viewerStore.logScroll?.value || null;
        chart.$scrollSeconds =
          payload && typeof payload.seconds === "number" ? payload.seconds : undefined;
        chart.update("none");
        syncHoverOverlay();
      })
    );
    off.push(
      signalEffect(() => {
        context.viewerStore.bookmarkVersion?.value;
        chart.update("none");
        syncHoverOverlay();
      })
    );
    off.push(
      signalEffect(() => {
        context.viewerStore.commentVersion?.value;
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
  } else if (context.bus) {
    off.push(
      context.bus.on("log:filtered", (filtered) => {
        filteredEvents = Array.isArray(filtered) ? filtered : allEvents;
        applyView(currentViewId);
      })
    );
    off.push(
      context.bus.on("log:scroll", (payload) => {
        if (payload && typeof payload.seconds === "number") {
          chart.$scrollSeconds = payload.seconds;
          chart.update("none");
          syncHoverOverlay();
        }
      })
    );
    off.push(context.bus.on("bookmarks:changed", () => {
      chart.update("none");
      syncHoverOverlay();
    }));
    off.push(context.bus.on("comments:changed", () => {
      chart.update("none");
      syncHoverOverlay();
    }));
    off.push(
      context.bus.on("event:selected", (event) => {
        selectedRowId = event?.row_id ?? null;
        chart.update("none");
        syncHoverOverlay();
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
  buildCommands(container, context) {
    context.panelController?.renderViewButtons?.(container);
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
      backgroundColor: "rgba(59, 130, 246, 0.28)",
      borderColor: "rgba(37, 99, 235, 0.5)",
    },
  ],
});
