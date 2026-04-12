window.LogMainViewChart = window.LogMainViewChart || {};

LogMainViewChart.mount = (root, services) => {
  const { logData, bus, bookmarks } = services;
  if (!logData) return null;

  const stackedCanvas = queryById(root, "stacked-chart");
  const chartRegion = queryById(root, "chart-region");
  const severityTab = queryById(root, "tab-chart-severity");
  const systemsTab = queryById(root, "tab-chart-systems");
  const severityPanel = queryById(root, "chart-panel-severity");
  const systemsPanel = queryById(root, "chart-panel-systems");
  const tools = root.querySelector(".chart-tools");
  const systemStatusBoard = queryById(root, "system-status-board");
  if (!stackedCanvas || !chartRegion || !severityTab || !systemsTab || !severityPanel || !systemsPanel || !systemStatusBoard) {
    return null;
  }

  const events = Array.isArray(logData?.events) ? logData.events : [];
  if (!events.length) return null;

  const setChartTab = (tab) => {
    const isSeverity = tab === "severity";
    severityTab.classList.toggle("is-active", isSeverity);
    systemsTab.classList.toggle("is-active", !isSeverity);
    severityTab.setAttribute("aria-selected", String(isSeverity));
    systemsTab.setAttribute("aria-selected", String(!isSeverity));
    severityPanel.classList.toggle("is-active", isSeverity);
    systemsPanel.classList.toggle("is-active", !isSeverity);
    if (tools) tools.classList.toggle("hidden", !isSeverity);
  };

  severityTab.addEventListener("click", () => setChartTab("severity"));
  systemsTab.addEventListener("click", () => setChartTab("systems"));
  setChartTab("severity");

  const startTime = new Date(logData.start);
  const endTime = new Date(logData.end);
  const spanMs = Math.max(1, endTime - startTime);
  const bucketMs = 5 * 60 * 1000;
  const bucketCount = Math.max(1, Math.ceil(spanMs / bucketMs));

  const labels = [];
  for (let i = 0; i < bucketCount; i += 1) {
    const t = new Date(startTime.getTime() + i * bucketMs);
    labels.push(t.toISOString().slice(11, 16));
  }

  const buildBuckets = (sourceEvents) => {
    const buckets = {
      Green: new Array(bucketCount).fill(0),
      Yellow: new Array(bucketCount).fill(0),
      Red: new Array(bucketCount).fill(0),
      "Flashing Red": new Array(bucketCount).fill(0),
    };
    sourceEvents.forEach((event) => {
      const timestamp = new Date(event.utctime);
      const index = Math.min(
        bucketCount - 1,
        Math.max(0, Math.floor((timestamp - startTime) / bucketMs))
      );
      if (buckets[event.color]) buckets[event.color][index] += 1;
    });
    return buckets;
  };

  const severityRank = {
    Green: 0,
    Yellow: 1,
    Red: 2,
    "Flashing Red": 3,
  };
  const systemStatusClass = {
    Green: "status-green",
    Yellow: "status-yellow",
    Red: "status-red",
    "Flashing Red": "status-flashing-red",
  };
  const systems = Array.from(
    new Set(events.map((event) => String(event.system || "").trim()).filter(Boolean))
  ).sort((a, b) => a.localeCompare(b));

  const buildStatusSnapshots = () => {
    const ordered = events
      .slice()
      .sort(
        (a, b) =>
          (Number(a.norm_time) || 0) - (Number(b.norm_time) || 0) ||
          (Number(a.row_id) || 0) - (Number(b.row_id) || 0)
      );
    const openByKey = new Map();
    const countsBySystem = new Map(
      systems.map((system) => [
        system,
        { Green: 0, Yellow: 0, Red: 0, "Flashing Red": 0 },
      ])
    );
    const snapshots = new Map();

    const getStatus = (system) => {
      const counts = countsBySystem.get(system);
      if (!counts) return "Green";
      if (counts["Flashing Red"] > 0) return "Flashing Red";
      if (counts.Red > 0) return "Red";
      if (counts.Yellow > 0) return "Yellow";
      return "Green";
    };

    ordered.forEach((event) => {
      const action = String(event.set_clear || "").trim().toLowerCase();
      const channels = Array.isArray(event.channels) ? event.channels : [];
      channels.forEach((channel) => {
        const key = [
          String(event.system || "").trim(),
          String(event.subsystem || "").trim(),
          String(event.unit || "").trim(),
          String(event.code || "").trim(),
          String(channel || "").trim(),
        ].join("|");
        const system = String(event.system || "").trim();
        const color = severityRank[event.color] != null ? event.color : "Green";
        const systemCounts = countsBySystem.get(system);
        if (!systemCounts) return;

        if (action === "set") {
          if (openByKey.has(key)) return;
          openByKey.set(key, { system, color });
          systemCounts[color] += 1;
          return;
        }

        if (action !== "clear" || !openByKey.has(key)) return;
        const open = openByKey.get(key);
        openByKey.delete(key);
        if (open?.color && systemCounts[open.color] > 0) {
          systemCounts[open.color] -= 1;
        }
      });

      snapshots.set(
        String(event.row_id),
        Object.fromEntries(systems.map((system) => [system, getStatus(system)]))
      );
    });

    return snapshots;
  };

  const statusSnapshots = buildStatusSnapshots();
  const renderSystemStatus = (rowId = events[0]?.row_id ?? null) => {
    const snapshot =
      (rowId != null ? statusSnapshots.get(String(rowId)) : null) ||
      Object.fromEntries(systems.map((system) => [system, "Green"]));
    systemStatusBoard.innerHTML = "";
    const fragment = document.createDocumentFragment();
    systems.forEach((system) => {
      const state = snapshot[system] || "Green";
      const item = document.createElement("div");
      item.className = `system-status-card ${systemStatusClass[state] || "status-green"}`;
      item.title = `${system}: ${state}`;
      item.innerHTML = `<div class="system-status-name">${system}</div>`;
      fragment.appendChild(item);
    });
    systemStatusBoard.appendChild(fragment);
  };
  renderSystemStatus(events[0]?.row_id ?? null);

  const modeSegments = Array.isArray(logData.modes) ? logData.modes : [];
  const modeColors = [
    "rgba(59, 130, 246, 0.08)",
    "rgba(14, 165, 233, 0.08)",
    "rgba(16, 185, 129, 0.08)",
    "rgba(249, 115, 22, 0.08)",
  ];

  const hoverLinePlugin = {
    id: "hoverLine",
    afterDatasetsDraw(chart) {
      const { ctx, chartArea } = chart;
      const x = chart.$hoverX;
      if (typeof x !== "number") return;
      ctx.save();
      ctx.beginPath();
      ctx.moveTo(x, chartArea.top);
      ctx.lineTo(x, chartArea.bottom);
      ctx.lineWidth = 1;
      ctx.strokeStyle = "rgba(100, 116, 139, 0.6)";
      ctx.stroke();
      const tooltipEnabled = chart.options.plugins?.tooltip?.enabled !== false;
      if (!tooltipEnabled && chart.$hoverTime) {
        const label = chart.$hoverTime.toISOString().slice(11, 19);
        const padding = 4;
        ctx.font = "12px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace";
        const textWidth = ctx.measureText(label).width;
        const boxWidth = textWidth + padding * 2;
        const boxHeight = 18;
        const boxX = Math.min(
          chartArea.right - boxWidth,
          Math.max(chartArea.left, x - boxWidth / 2)
        );
        const boxY = chartArea.top + 6;
        ctx.fillStyle = "rgba(15, 23, 42, 0.75)";
        ctx.fillRect(boxX, boxY, boxWidth, boxHeight);
        ctx.fillStyle = "#f8fafc";
        ctx.textBaseline = "middle";
        ctx.fillText(label, boxX + padding, boxY + boxHeight / 2);
      }
      ctx.restore();
    },
  };

  const scrollIndicatorPlugin = {
    id: "scrollIndicator",
    afterDatasetsDraw(chart) {
      const { ctx, chartArea } = chart;
      const ratio = chart.$scrollRatio;
      if (typeof ratio !== "number") return;
      const x = chartArea.left + ratio * chartArea.width;
      ctx.save();
      ctx.beginPath();
      ctx.moveTo(x, chartArea.top);
      ctx.lineTo(x, chartArea.bottom);
      ctx.lineWidth = 1;
      ctx.strokeStyle = "rgba(30, 64, 175, 0.65)";
      ctx.setLineDash([4, 4]);
      ctx.stroke();
      ctx.restore();
    },
  };

  const modeBandPlugin = {
    id: "modeBands",
    beforeDatasetsDraw(chart) {
      const { ctx, chartArea } = chart;
      if (!modeSegments.length) return;
      ctx.save();
      modeSegments.forEach((mode, idx) => {
        const start = new Date(mode.start).getTime();
        const end = new Date(mode.end).getTime();
        const startRatio = Math.max(0, Math.min(1, (start - startTime.getTime()) / spanMs));
        const endRatio = Math.max(0, Math.min(1, (end - startTime.getTime()) / spanMs));
        const x0 = chartArea.left + startRatio * chartArea.width;
        const x1 = chartArea.left + endRatio * chartArea.width;
        ctx.fillStyle = modeColors[idx % modeColors.length];
        ctx.fillRect(x0, chartArea.top, Math.max(0, x1 - x0), chartArea.height);
        ctx.fillStyle = "rgba(15, 23, 42, 0.5)";
        ctx.font = "11px ui-sans-serif, system-ui, -apple-system, sans-serif";
        ctx.textBaseline = "top";
        ctx.fillText(mode.name, x0 + 4, chartArea.top + 4);
      });
      ctx.restore();
    },
  };

  const bookmarkPlugin = {
    id: "bookmarkDots",
    afterDatasetsDraw(chart) {
      const { ctx, chartArea, scales } = chart;
      const ids = bookmarks?.getAll() || [];
      const dots = [];
      const xScale = scales?.x;
      const yBase = xScale ? xScale.bottom : chartArea.bottom;
      const dotY = Math.min(chartArea.bottom + 5, yBase + 2);
      ctx.save();
      ids.forEach((id) => {
        const event = events.find((entry) => String(entry.row_id) === String(id));
        if (!event) return;
        const colorIndex = bookmarks?.getColor(event.row_id) || 1;
        const timestamp = new Date(event.utctime).getTime();
        const ratio = Math.max(0, Math.min(1, (timestamp - startTime.getTime()) / spanMs));
        const x = chartArea.left + ratio * chartArea.width;
        ctx.beginPath();
        ctx.fillStyle = getComputedStyle(document.documentElement).getPropertyValue(
          `--bookmark-color-${colorIndex}`
        ) || "rgba(14, 116, 144, 0.9)";
        ctx.arc(x, dotY, 3, 0, Math.PI * 2);
        ctx.fill();
        dots.push({ x, y: dotY, rowId: event.row_id });
      });
      chart.$bookmarkDots = dots;
      ctx.restore();
    },
  };

  const initialBuckets = buildBuckets(events);
  const stackedChart = new Chart(stackedCanvas.getContext("2d"), {
    type: "bar",
    data: {
      labels,
      datasets: [
        { label: "Green", data: initialBuckets.Green, backgroundColor: "rgba(34, 197, 94, 0.75)", borderColor: "rgba(22, 163, 74, 1)", borderWidth: 1 },
        { label: "Yellow", data: initialBuckets.Yellow, backgroundColor: "rgba(250, 204, 21, 0.8)", borderColor: "rgba(234, 179, 8, 1)", borderWidth: 1 },
        { label: "Red", data: initialBuckets.Red, backgroundColor: "rgba(239, 68, 68, 0.8)", borderColor: "rgba(220, 38, 38, 1)", borderWidth: 1 },
        { label: "Flashing Red", data: initialBuckets["Flashing Red"], backgroundColor: "rgba(127, 29, 29, 0.85)", borderColor: "rgba(88, 28, 28, 1)", borderWidth: 1 },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: { mode: "index", intersect: false, enabled: true },
      },
      scales: {
        x: { stacked: true },
        y: { stacked: true, beginAtZero: true },
      },
      onClick: (event) => {
        const pos = Chart.helpers.getRelativePosition(event, stackedChart);
        const { chartArea, scales } = stackedChart;
        const dots = stackedChart.$bookmarkDots || [];
        for (const dot of dots) {
          const dx = pos.x - dot.x;
          const dy = pos.y - dot.y;
          if (Math.sqrt(dx * dx + dy * dy) <= 10) {
            const hit = events.find((entry) => String(entry.row_id) === String(dot.rowId));
            if (bus && hit) bus.emit("event:selected", hit);
            if (bus) bus.emit("log:jump", { rowId: dot.rowId });
            return;
          }
        }
        const xScale = scales?.x;
        const dotHitBottom = xScale ? xScale.bottom + 12 : chartArea.bottom + 12;
        if (pos.x < chartArea.left || pos.x > chartArea.right) return;
        if (pos.y < chartArea.top || pos.y > dotHitBottom) return;
        if (bus) {
          const ratio = (pos.x - chartArea.left) / chartArea.width;
          bus.emit("log:jump", { seconds: Math.floor((ratio * spanMs) / 1000) });
        }
      },
    },
    plugins: [modeBandPlugin, bookmarkPlugin, hoverLinePlugin, scrollIndicatorPlugin],
  });

  let resizeRaf = 0;
  const scheduleResize = () => {
    if (resizeRaf) cancelAnimationFrame(resizeRaf);
    resizeRaf = requestAnimationFrame(() => {
      resizeRaf = 0;
      stackedChart.resize();
      stackedChart.update("none");
    });
  };

  scheduleResize();
  if (typeof ResizeObserver === "function") {
    const resizeObserver = new ResizeObserver(() => scheduleResize());
    resizeObserver.observe(chartRegion);
    resizeObserver.observe(severityPanel);
    resizeObserver.observe(systemsPanel);
    stackedChart.$resizeObserver = resizeObserver;
  } else {
    window.addEventListener("resize", scheduleResize);
    stackedChart.$resizeFallback = scheduleResize;
  }

  const updateHover = (event) => {
    const pos = Chart.helpers.getRelativePosition(event, stackedChart);
    const { chartArea } = stackedChart;
    if (
      pos.x < chartArea.left ||
      pos.x > chartArea.right ||
      pos.y < chartArea.top ||
      pos.y > chartArea.bottom
    ) {
      stackedChart.$hoverX = null;
      stackedChart.$hoverTime = null;
      stackedChart.draw();
      return;
    }
    const ratio = (pos.x - chartArea.left) / chartArea.width;
    stackedChart.$hoverX = pos.x;
    stackedChart.$hoverTime = new Date(startTime.getTime() + ratio * spanMs);
    stackedChart.draw();
  };

  stackedCanvas.addEventListener("mousemove", updateHover);
  stackedCanvas.addEventListener("mouseleave", () => {
    stackedChart.$hoverX = null;
    stackedChart.$hoverTime = null;
    stackedChart.draw();
  });

  const toggleTooltips = queryById(root, "toggle-tooltips");
  if (toggleTooltips) {
    const stored = localStorage.getItem(STORAGE_KEYS.chartTooltips);
    const initialEnabled = stored === null ? false : stored === "true";
    stackedChart.options.plugins.tooltip.enabled = initialEnabled;
    const setToggleState = (enabled) => {
      toggleTooltips.setAttribute("aria-pressed", String(enabled));
      toggleTooltips.classList.toggle("tooltip-disabled", !enabled);
      toggleTooltips.classList.toggle("is-enabled", enabled);
    };
    setToggleState(initialEnabled);
    toggleTooltips.addEventListener("click", () => {
      const current = stackedChart.options.plugins.tooltip.enabled !== false;
      stackedChart.options.plugins.tooltip.enabled = !current;
      setToggleState(!current);
      localStorage.setItem(STORAGE_KEYS.chartTooltips, String(!current));
      stackedChart.update();
    });
  }

  if (bus) {
    let pendingScrollSeconds = null;
    let scrollUpdateScheduled = false;
    let lastScrollPaintAt = 0;
    const minScrollPaintIntervalMs = 80;

    const paintScrollIndicator = (timestamp = performance.now()) => {
      scrollUpdateScheduled = false;
      if (typeof pendingScrollSeconds !== "number") return;
      stackedChart.$scrollRatio = Math.max(0, Math.min(1, pendingScrollSeconds / (spanMs / 1000)));
      pendingScrollSeconds = null;
      lastScrollPaintAt = timestamp;
      stackedChart.update("none");
    };

    bus.on("log:filtered", (filtered) => {
      const buckets = buildBuckets(filtered || []);
      stackedChart.data.datasets[0].data = buckets.Green;
      stackedChart.data.datasets[1].data = buckets.Yellow;
      stackedChart.data.datasets[2].data = buckets.Red;
      stackedChart.data.datasets[3].data = buckets["Flashing Red"];
      stackedChart.update();
    });
    bus.on("log:scroll", (payload) => {
      if (!payload || typeof payload.seconds !== "number") return;
      pendingScrollSeconds = payload.seconds;
      renderSystemStatus(payload.rowId ?? null);
      if (scrollUpdateScheduled) return;

      const now = performance.now();
      const elapsed = now - lastScrollPaintAt;
      scrollUpdateScheduled = true;

      if (elapsed >= minScrollPaintIntervalMs) {
        requestAnimationFrame((timestamp) => paintScrollIndicator(timestamp));
        return;
      }

      window.setTimeout(() => {
        requestAnimationFrame((timestamp) => paintScrollIndicator(timestamp));
      }, Math.max(0, minScrollPaintIntervalMs - elapsed));
    });
    bus.on("bookmarks:changed", () => {
      stackedChart.update();
    });
    bus.on("event:selected", (event) => {
      renderSystemStatus(event?.row_id ?? null);
    });
  }

  return stackedChart;
};
