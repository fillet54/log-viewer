window.LogApp = window.LogApp || {};

LogApp.initChart = (logData, bus) => {
  if (!logData) return null;

  const stackedCanvas = document.getElementById("stacked-chart");
  if (!stackedCanvas) return null;

  const events = Array.isArray(logData?.events) ? logData.events : [];
  if (!events.length) return null;

  const startTime = new Date(logData.start);
  const endTime = new Date(logData.end);
  const startMs = startTime.getTime();
  const endMs = endTime.getTime();
  const spanMs = Math.max(1, endMs - startMs);
  const bucketMs = 5 * 60 * 1000;
  const bucketCount = Math.max(1, Math.ceil(spanMs / bucketMs));
  const modeSegments = Array.isArray(logData.modes) ? logData.modes : [];
  const modeColors = [
    "rgba(59, 130, 246, 0.08)",
    "rgba(14, 165, 233, 0.08)",
    "rgba(16, 185, 129, 0.08)",
    "rgba(249, 115, 22, 0.08)",
  ];
  const connectivityLinks = [
    { label: "eth1", eventName: "ETH1_DOWN" },
    { label: "eth2", eventName: "ETH2_DOWN" },
    { label: "wifi", eventName: "WIFI_DOWN" },
    { label: "cellular", eventName: "CELLULAR_DOWN" },
  ];
  const connectivityRows = ["summary", ...connectivityLinks.map((link) => link.label)];
  const statusColors = {
    Up: {
      backgroundColor: "rgba(34, 197, 94, 0.82)",
      borderColor: "rgba(21, 128, 61, 1)",
    },
    Unstable: {
      backgroundColor: "rgba(250, 204, 21, 0.88)",
      borderColor: "rgba(202, 138, 4, 1)",
    },
    Down: {
      backgroundColor: "rgba(0, 0, 0, 0)",
      borderColor: "rgba(0, 0, 0, 0)",
    },
  };
  const severityPalette = [
    {
      label: "Green",
      backgroundColor: "rgba(34, 197, 94, 0.75)",
      borderColor: "rgba(22, 163, 74, 1)",
    },
    {
      label: "Yellow",
      backgroundColor: "rgba(250, 204, 21, 0.8)",
      borderColor: "rgba(234, 179, 8, 1)",
    },
    {
      label: "Red",
      backgroundColor: "rgba(239, 68, 68, 0.8)",
      borderColor: "rgba(220, 38, 38, 1)",
    },
    {
      label: "Flashing Red",
      backgroundColor: "rgba(127, 29, 29, 0.85)",
      borderColor: "rgba(88, 28, 28, 1)",
    },
  ];
  const labels = [];
  for (let i = 0; i < bucketCount; i += 1) {
    const t = new Date(startMs + i * bucketMs);
    labels.push(t.toISOString().slice(11, 16));
  }

  let filteredEvents = events.slice();
  let chartMode =
    localStorage.getItem(LogApp.STORAGE_KEYS.chartMode) === "connectivity"
      ? "connectivity"
      : "severity";
  let stackedChart = null;

  const formatTime = (ms) => new Date(ms).toISOString().slice(11, 19);

  const buildSeverityBuckets = (sourceEvents) => {
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

  const appendStatusSegment = (segments, start, end, status) => {
    if (end <= start) return;
    const previous = segments[segments.length - 1];
    if (previous && previous.status === status && previous.end === start) {
      previous.end = end;
      return;
    }
    segments.push({ start, end, status });
  };

  const applyFaultWindow = (segments, intervalStart, intervalEnd, isDown, lastChangeMs) => {
    if (intervalEnd <= intervalStart) return;
    const stableAt = Math.min(intervalEnd, lastChangeMs + 10000);
    appendStatusSegment(segments, intervalStart, stableAt, "Unstable");
    if (stableAt < intervalEnd) {
      appendStatusSegment(segments, stableAt, intervalEnd, isDown ? "Down" : "Up");
    }
  };

  const buildLinkConnectivitySegments = (link) => {
    const relevant = events
      .filter((event) => event.name === link.eventName)
      .sort((a, b) => (a.norm_time || 0) - (b.norm_time || 0));
    const segments = [];
    let cursor = startMs;
    let isDown = false;
    let lastChangeMs = startMs;

    relevant.forEach((event) => {
      const eventMs = new Date(event.utctime).getTime();
      if (eventMs < startMs || eventMs > endMs) return;
      applyFaultWindow(segments, cursor, eventMs, isDown, lastChangeMs);
      isDown = String(event.set_clear || "").toLowerCase() === "set";
      lastChangeMs = eventMs;
      cursor = eventMs;
    });

    applyFaultWindow(segments, cursor, endMs, isDown, lastChangeMs);
    return segments;
  };

  const getStatusAtTime = (segments, atMs) => {
    const segment = segments.find((item) => atMs >= item.start && atMs < item.end);
    if (segment) return segment.status;
    const fallback = segments[segments.length - 1];
    return fallback ? fallback.status : "Down";
  };

  const summarizeStatuses = (statuses) => {
    if (statuses.includes("Up")) return "Up";
    if (statuses.includes("Unstable")) return "Unstable";
    return "Down";
  };

  const buildConnectivityDatasets = () => {
    const laneSegments = [];
    const segmentsByLink = new Map();
    const summaryBoundaries = new Set([startMs, endMs]);

    connectivityLinks.forEach((link) => {
      const segments = buildLinkConnectivitySegments(link);
      segmentsByLink.set(link.label, segments);
      segments.forEach((segment) => {
        summaryBoundaries.add(segment.start);
        summaryBoundaries.add(segment.end);
        laneSegments.push({
          x: [segment.start, segment.end],
          y: link.label,
          status: segment.status,
          link: link.label,
          start: segment.start,
          end: segment.end,
        });
      });
    });

    const orderedBoundaries = Array.from(summaryBoundaries).sort((a, b) => a - b);
    const summarySegments = [];
    for (let index = 0; index < orderedBoundaries.length - 1; index += 1) {
      const segmentStart = orderedBoundaries[index];
      const segmentEnd = orderedBoundaries[index + 1];
      if (segmentEnd <= segmentStart) continue;
      const statuses = connectivityLinks.map((link) =>
        getStatusAtTime(segmentsByLink.get(link.label) || [], segmentStart)
      );
      appendStatusSegment(summarySegments, segmentStart, segmentEnd, summarizeStatuses(statuses));
    }

    summarySegments.forEach((segment) => {
      laneSegments.push({
        x: [segment.start, segment.end],
        y: "summary",
        status: segment.status,
        link: "summary",
        start: segment.start,
        end: segment.end,
      });
    });

    return [
      {
      label: "Connectivity",
      data: laneSegments,
      backgroundColor(context) {
        const point = context.raw || {};
        return statusColors[point.status]?.backgroundColor || statusColors.Down.backgroundColor;
      },
      borderColor(context) {
        const point = context.raw || {};
        return statusColors[point.status]?.borderColor || statusColors.Down.borderColor;
      },
      borderWidth: 1,
      borderSkipped: false,
      borderRadius: 3,
      barPercentage: 0.82,
      categoryPercentage: 0.72,
      },
    ];
  };

  const chartRatioFromPixel = (chart, pixelX) => {
    const { chartArea } = chart;
    if (!chartArea || chartArea.width <= 0) return 0;
    return Math.max(0, Math.min(1, (pixelX - chartArea.left) / chartArea.width));
  };

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
        const label = formatTime(chart.$hoverTime.getTime());
        const padding = 4;
        ctx.font =
          "12px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace";
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
        const startRatio = Math.max(0, Math.min(1, (start - startMs) / spanMs));
        const endRatio = Math.max(0, Math.min(1, (end - startMs) / spanMs));
        const x0 = chartArea.left + startRatio * chartArea.width;
        const x1 = chartArea.left + endRatio * chartArea.width;
        const width = Math.max(0, x1 - x0);
        ctx.fillStyle = modeColors[idx % modeColors.length];
        ctx.fillRect(x0, chartArea.top, width, chartArea.height);
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
      const ids = LogApp.bookmarks?.getAll() || [];
      const dots = [];
      const xScale = scales?.x;
      const yBase = xScale ? xScale.bottom : chartArea.bottom;
      const dotY = Math.min(chartArea.bottom + 5, yBase + 2);
      ctx.save();
      ids.forEach((id) => {
        const event = events.find((entry) => String(entry.row_id) === String(id));
        if (!event) return;
        const colorIndex = LogApp.bookmarks?.getColor(event.row_id) || 1;
        const timestamp = new Date(event.utctime).getTime();
        const ratio = Math.max(0, Math.min(1, (timestamp - startMs) / spanMs));
        const x = chartArea.left + ratio * chartArea.width;
        ctx.beginPath();
        ctx.fillStyle =
          getComputedStyle(document.documentElement).getPropertyValue(
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

  const createSeverityChart = () => {
    const buckets = buildSeverityBuckets(filteredEvents);
    return new Chart(stackedCanvas.getContext("2d"), {
      type: "bar",
      data: {
        labels,
        datasets: severityPalette.map((item) => ({
          label: item.label,
          data: buckets[item.label],
          backgroundColor: item.backgroundColor,
          borderColor: item.borderColor,
          borderWidth: 1,
        })),
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
      },
      plugins: [modeBandPlugin, bookmarkPlugin, hoverLinePlugin, scrollIndicatorPlugin],
    });
  };

  const createConnectivityChart = () =>
    new Chart(stackedCanvas.getContext("2d"), {
      type: "bar",
      data: {
        datasets: buildConnectivityDatasets(),
      },
      options: {
        indexAxis: "y",
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            enabled: true,
            intersect: false,
            callbacks: {
              title(items) {
                const point = items?.[0]?.raw;
                return point?.link ? point.link : "";
              },
              label(context) {
                const point = context.raw || {};
                return `${point.status}: ${formatTime(point.start)} - ${formatTime(point.end)}`;
              },
            },
          },
        },
        scales: {
          x: {
            type: "linear",
            min: startMs,
            max: endMs,
            grid: { color: "rgba(148, 163, 184, 0.15)" },
            ticks: {
              callback(value) {
                return formatTime(Number(value)).slice(0, 5);
              },
            },
          },
          y: {
            type: "category",
            labels: connectivityRows,
            grid: { display: false },
          },
        },
      },
      plugins: [modeBandPlugin, bookmarkPlugin, hoverLinePlugin, scrollIndicatorPlugin],
    });

  const setTooltipState = () => {
    const toggleTooltips = document.getElementById("toggle-tooltips");
    if (!toggleTooltips || !stackedChart) return;
    const stored = localStorage.getItem(LogApp.STORAGE_KEYS.chartTooltips);
    const enabled = stored === "true";
    stackedChart.options.plugins.tooltip.enabled = enabled;
    toggleTooltips.setAttribute("aria-pressed", String(enabled));
    toggleTooltips.classList.toggle("tooltip-disabled", !enabled);
    toggleTooltips.classList.toggle("is-enabled", enabled);
    stackedChart.update();
  };

  const setModeButtons = () => {
    document.querySelectorAll("[data-chart-mode]").forEach((button) => {
      const active = button.dataset.chartMode === chartMode;
      button.setAttribute("aria-pressed", String(active));
      button.classList.toggle("is-enabled", active);
    });
  };

  const rebuildChart = () => {
    const previousScrollRatio = stackedChart?.$scrollRatio;
    const previousHoverX = stackedChart?.$hoverX;
    const previousHoverTime = stackedChart?.$hoverTime;
    if (stackedChart) stackedChart.destroy();
    stackedChart =
      chartMode === "connectivity" ? createConnectivityChart() : createSeverityChart();
    stackedChart.$scrollRatio = previousScrollRatio;
    stackedChart.$hoverX = previousHoverX;
    stackedChart.$hoverTime = previousHoverTime;
    setModeButtons();
    setTooltipState();
  };

  const handleChartClick = (event) => {
    if (!stackedChart) return;
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
    const ratio = chartRatioFromPixel(stackedChart, pos.x);
    if (bus) bus.emit("log:jump", { seconds: Math.floor((ratio * spanMs) / 1000) });
  };

  const updateHover = (event) => {
    if (!stackedChart) return;
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
    const ratio = chartRatioFromPixel(stackedChart, pos.x);
    stackedChart.$hoverX = pos.x;
    stackedChart.$hoverTime = new Date(startMs + ratio * spanMs);
    stackedChart.draw();
  };

  stackedCanvas.addEventListener("mousemove", updateHover);
  stackedCanvas.addEventListener("mouseleave", () => {
    if (!stackedChart) return;
    stackedChart.$hoverX = null;
    stackedChart.$hoverTime = null;
    stackedChart.draw();
  });
  stackedCanvas.addEventListener("click", (event) => handleChartClick(event));

  const toggleTooltips = document.getElementById("toggle-tooltips");
  if (toggleTooltips) {
    toggleTooltips.addEventListener("click", () => {
      if (!stackedChart) return;
      const current = stackedChart.options.plugins.tooltip.enabled !== false;
      localStorage.setItem(LogApp.STORAGE_KEYS.chartTooltips, String(!current));
      setTooltipState();
    });
  }

  document.querySelectorAll("[data-chart-mode]").forEach((button) => {
    button.addEventListener("click", () => {
      const requestedMode = button.dataset.chartMode;
      if (!requestedMode || requestedMode === chartMode) return;
      chartMode = requestedMode;
      localStorage.setItem(LogApp.STORAGE_KEYS.chartMode, chartMode);
      rebuildChart();
    });
  });

  const updateFilteredEvents = (nextFilteredEvents) => {
    filteredEvents = Array.isArray(nextFilteredEvents) ? nextFilteredEvents : [];
    if (chartMode !== "severity" || !stackedChart) return;
    const buckets = buildSeverityBuckets(filteredEvents);
    severityPalette.forEach((item, index) => {
      stackedChart.data.datasets[index].data = buckets[item.label];
    });
    stackedChart.update();
  };

  rebuildChart();

  if (bus) {
    bus.on("log:filtered", (filtered) => {
      updateFilteredEvents(filtered || []);
    });
    bus.on("log:scroll", (payload) => {
      if (!stackedChart || !payload || typeof payload.seconds !== "number") return;
      stackedChart.$scrollRatio = Math.max(0, Math.min(1, payload.seconds / (spanMs / 1000)));
      stackedChart.update("none");
    });
    bus.on("bookmarks:changed", () => {
      if (stackedChart) stackedChart.update();
    });
  }

  return {
    updateFilteredEvents,
  };
};
