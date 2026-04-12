window.LogMainViewChart = window.LogMainViewChart || {};
LogMainViewChart.registry = LogMainViewChart.registry || new Map();

LogMainViewChart.buildRegistryKey = (pluginId, chartId) => {
  const pluginPart = pluginId == null ? "global" : String(pluginId).trim() || "global";
  return `${pluginPart}::${String(chartId).trim()}`;
};

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
  if (!normalizedPluginId) {
    throw new Error("Plugin chart types must define a plugin id.");
  }
  LogMainViewChart.registerType({ ...definition, pluginId: normalizedPluginId });
};

if (window.EventLog2?._pendingChartRegistrations?.length) {
  const pending = window.EventLog2._pendingChartRegistrations.splice(0);
  pending.forEach(({ pluginId, definition }) => {
    if (pluginId == null) {
      LogMainViewChart.registerType(definition);
      return;
    }
    LogMainViewChart.registerPluginType(pluginId, definition);
  });
}

LogMainViewChart.mount = (root, services) => {
  const { logData, bus, bookmarks, comments, plugin } = services;
  if (!logData) return null;

  const chartRegion = queryById(root, "chart-region");
  const chartPanelHost = queryById(root, "chart-panel-host");
  const chartTypeSelect = queryById(root, "chart-type-select");
  const commandBar = queryById(root, "chart-command-bar");
  if (!chartRegion || !chartPanelHost || !chartTypeSelect || !commandBar) {
    return null;
  }

  const activePluginId = String(plugin?.id || logData?.pluginId || "").trim() || null;
  const mountedPanels = new Map();
  let activeType = null;
  let activePanel = null;
  let cleanupCommands = null;

  const listTypes = () =>
    Array.from(LogMainViewChart.registry.values()).filter((type) => !type.pluginId || type.pluginId === activePluginId);

  const buildContext = (extra = {}) => ({
    root,
    plugin,
    bus,
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

  const controller = {
    currentType: null,
    chart: null,
    listTypes,
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
      if (!type) return;

      if (cleanupCommands) {
        cleanupCommands();
        cleanupCommands = null;
      }

      if (activePanel?.controller && typeof activePanel.controller.deactivate === "function") {
        activePanel.controller.deactivate(buildContext({ type: activeType, panel: activePanel.panel }));
      }
      if (activeType && typeof activeType.deactivate === "function") {
        activeType.deactivate(buildContext({ type: activeType, panel: activePanel?.panel, panelController: activePanel?.controller || null }));
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

      chartTypeSelect.value = type.id;
      commandBar.innerHTML = "";

      const context = buildContext({
        type,
        panel: panelRecord.panel,
        panelController: panelRecord.controller || null,
      });

      if (typeof type.activate === "function") {
        type.activate(context);
      }
      if (panelRecord.controller && typeof panelRecord.controller.activate === "function") {
        panelRecord.controller.activate(context);
      }

      const buildCommands = type.buildCommands || type.renderControls;
      if (typeof buildCommands === "function") {
        cleanupCommands = buildCommands(commandBar, context) || null;
      }

      controller.resize();
    },
    bindToolbar() {
      const types = controller.listTypes();
      if (!types.length) {
        chartTypeSelect.innerHTML = "";
        chartTypeSelect.disabled = true;
        commandBar.innerHTML = '<div class="chart-command-hint">No charts registered for this plugin.</div>';
        return;
      }
      chartTypeSelect.disabled = false;
      chartTypeSelect.innerHTML = types.map((type) => `<option value="${type.id}">${type.label}</option>`).join("");
      chartTypeSelect.addEventListener("change", () => controller.setType(chartTypeSelect.value));
      controller.setType(types.some((type) => type.id === controller.currentType) ? controller.currentType : types[0].id);
    },
    resize() {
      if (activePanel?.controller && typeof activePanel.controller.resize === "function") {
        activePanel.controller.resize(buildContext({
          type: activeType,
          panel: activePanel.panel,
          panelController: activePanel.controller,
        }));
      }
    },
    destroy() {
      if (cleanupCommands) cleanupCommands();
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

LogMainViewChart.registerType({
  id: "timeline",
  label: "Timeline",
  renderPanel(panel, context) {
    const events = Array.isArray(context.logData?.events) ? context.logData.events : [];
    panel.innerHTML = '<canvas class="timeline-strip-canvas"></canvas>';
    const canvas = panel.querySelector("canvas");
    let resizeObserver = null;
    let selectedRowId = null;

    const getBounds = () => {
      const start = new Date(context.logData?.start).getTime();
      const end = new Date(context.logData?.end).getTime();
      const fallbackStart = Number(events[0]?.norm_time || 0) * 1000;
      const fallbackEnd = Number(events[events.length - 1]?.norm_time || 1) * 1000;
      const startMs = Number.isFinite(start) ? start : fallbackStart;
      const endMs = Number.isFinite(end) ? end : fallbackEnd;
      return { startMs, spanMs: Math.max(1, endMs - startMs) };
    };

    const getEventMs = (event) => {
      const utc = new Date(event?.utctime).getTime();
      if (Number.isFinite(utc)) return utc;
      return Number(event?.norm_time || 0) * 1000;
    };

    const { startMs, spanMs } = getBounds();
    const bucketCount = Math.max(24, Math.min(160, Math.ceil(panel.getBoundingClientRect().width / 8) || 72));
    const bucketMs = Math.max(1, Math.ceil(spanMs / bucketCount));
    const labels = Array.from({ length: bucketCount }, (_, index) =>
      new Date(startMs + index * bucketMs).toISOString().slice(11, 19)
    );
    const buildBuckets = () => {
      const bins = new Array(bucketCount).fill(0);
      events.forEach((entry) => {
        const ratio = Math.max(0, Math.min(1, (getEventMs(entry) - startMs) / spanMs));
        const index = Math.min(bucketCount - 1, Math.max(0, Math.floor(ratio * (bucketCount - 1))));
        bins[index] += 1;
      });
      return bins;
    };

    const markerPlugin = {
      id: "timelineMarkers",
      afterDatasetsDraw(chart) {
        const { ctx, chartArea } = chart;
        const bookmarkIds = context.bookmarks?.getAll() || [];
        const commentMap = context.comments?.getByRowId() || new Map();
        const commentIds = Array.from(commentMap.keys());
        const markers = [];
        const markerRadius = 4;
        const bookmarkY = chartArea.top + 8;
        const commentY = chartArea.bottom - 8;

        const drawMarker = (rowId, y, fillStyle, kind) => {
          const event = events.find((entry) => String(entry.row_id) === String(rowId));
          if (!event) return;
          const ratio = Math.max(0, Math.min(1, (getEventMs(event) - startMs) / spanMs));
          const x = chartArea.left + ratio * chartArea.width;
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
          markers.push({ x, y, rowId, kind });
        };

        bookmarkIds.forEach((rowId) => {
          const colorIndex = context.bookmarks?.getColor(rowId) || 1;
          const fill =
            getComputedStyle(document.documentElement).getPropertyValue(`--bookmark-color-${colorIndex}`) ||
            "rgba(59, 130, 246, 0.95)";
          drawMarker(rowId, bookmarkY, fill.trim(), "bookmark");
        });
        commentIds.forEach((rowId) => drawMarker(rowId, commentY, "rgba(71, 85, 105, 0.9)", "comment"));
        chart.$timelineMarkers = markers;
      },
      afterDraw(chart) {
        const active = chart.tooltip?.getActiveElements?.() || [];
        if (!active.length) return;
        const { ctx, chartArea } = chart;
        const element = active[0]?.element;
        if (!element) return;
        const x = element.x;
        ctx.save();
        ctx.beginPath();
        ctx.moveTo(x, chartArea.top);
        ctx.lineTo(x, chartArea.bottom);
        ctx.lineWidth = 1;
        ctx.strokeStyle = "rgba(100, 116, 139, 0.55)";
        ctx.stroke();
        ctx.restore();
      },
    };

    const timelineChart = new Chart(canvas.getContext("2d"), {
      type: "bar",
      data: {
        labels,
        datasets: [
          {
            label: "Events",
            data: buildBuckets(),
            backgroundColor: "rgba(59, 130, 246, 0.28)",
            borderColor: "rgba(37, 99, 235, 0.5)",
            borderWidth: 1,
            borderRadius: 2,
            categoryPercentage: 1,
            barPercentage: 1,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            mode: "index",
            intersect: false,
            displayColors: false,
            callbacks: {
              title(items) {
                const index = items[0]?.dataIndex ?? 0;
                return labels[index] || "";
              },
              label(item) {
                const value = Number(item.raw) || 0;
                return value === 1 ? "1 event" : `${value} events`;
              },
            },
          },
        },
        interaction: {
          mode: "index",
          intersect: false,
        },
        layout: {
          padding: { top: 10, bottom: 12 },
        },
        scales: {
          x: {
            grid: { display: false, drawBorder: false },
            ticks: {
              maxTicksLimit: 5,
              color: "rgba(71, 85, 105, 0.9)",
              font: {
                family: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace",
                size: 11,
              },
            },
          },
          y: {
            display: false,
            beginAtZero: true,
            grid: { display: false, drawBorder: false },
          },
        },
        onClick(event) {
          if (!context.bus) return;
          const pos = Chart.helpers.getRelativePosition(event, timelineChart);
          const markers = timelineChart.$timelineMarkers || [];
          const hit = markers.find((marker) => {
            const dx = pos.x - marker.x;
            const dy = pos.y - marker.y;
            return Math.sqrt(dx * dx + dy * dy) <= 8;
          });
          if (hit) {
            const selected = events.find((entry) => String(entry.row_id) === String(hit.rowId));
            if (selected) context.bus.emit("event:selected", selected);
            context.bus.emit("log:jump", { rowId: hit.rowId });
            return;
          }
          const ratio = Math.max(0, Math.min(1, (pos.x - timelineChart.chartArea.left) / timelineChart.chartArea.width));
          context.bus.emit("log:jump", { seconds: Math.floor((ratio * spanMs) / 1000) });
        },
      },
      plugins: [markerPlugin],
    });

    const refresh = () => {
      timelineChart.data.datasets[0].data = buildBuckets();
      timelineChart.update("none");
    };

    const off = [];
    if (context.bus) {
      off.push(context.bus.on("log:filtered", (filtered) => {
        const source = Array.isArray(filtered) ? filtered : events;
        const bins = new Array(bucketCount).fill(0);
        source.forEach((entry) => {
          const ratio = Math.max(0, Math.min(1, (getEventMs(entry) - startMs) / spanMs));
          const index = Math.min(bucketCount - 1, Math.max(0, Math.floor(ratio * (bucketCount - 1))));
          bins[index] += 1;
        });
        timelineChart.data.datasets[0].data = bins;
        timelineChart.update("none");
      }));
      off.push(context.bus.on("bookmarks:changed", refresh));
      off.push(context.bus.on("comments:changed", refresh));
      off.push(
        context.bus.on("event:selected", (event) => {
          selectedRowId = event?.row_id ?? null;
          timelineChart.update("none");
        })
      );
    }

    if (typeof ResizeObserver === "function") {
      resizeObserver = new ResizeObserver(() => timelineChart.resize());
      resizeObserver.observe(context.chartRegion);
      resizeObserver.observe(panel);
    } else {
      window.addEventListener("resize", refresh);
    }

    return {
      chart: timelineChart,
      resize() {
        timelineChart.resize();
        timelineChart.update("none");
      },
      activate() {
        timelineChart.resize();
        timelineChart.update("none");
      },
      destroy() {
        off.forEach((unsubscribe) => unsubscribe && unsubscribe());
        if (resizeObserver) resizeObserver.disconnect();
        else window.removeEventListener("resize", refresh);
        timelineChart.destroy();
      },
    };
  },
  buildCommands(container) {
    const hint = document.createElement("div");
    hint.className = "chart-command-hint";
    hint.textContent = "Bookmarks and comments across the log timeline.";
    container.appendChild(hint);
  },
});
