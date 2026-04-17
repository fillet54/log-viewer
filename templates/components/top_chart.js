window.LogApp = window.LogApp || {};

LogApp.initChart = (logData, bus) => {
  if (!logData) return null;

  const stackedCanvas = document.getElementById("stacked-chart");
  if (!stackedCanvas || typeof Chart === "undefined") return null;

  const events = Array.isArray(logData.events) ? logData.events : [];
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
  const connectivityRows = ["summary"].concat(connectivityLinks.map((link) => link.label));
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
    labels.push(new Date(startMs + i * bucketMs).toISOString().slice(11, 16));
  }

  let filteredEvents = events.slice();
  let chartMode =
    localStorage.getItem(LogApp.STORAGE_KEYS.chartMode) === "connectivity"
      ? "connectivity"
      : "severity";
  let stackedChart = null;

  const formatTime = (ms) => new Date(ms).toISOString().slice(11, 19);

  const getChartArea = (chart) => chart && chart.chartArea;
  const getXScale = (chart) => (chart && chart.scales ? chart.scales["x-axis-0"] : null);

  const buildSeverityBuckets = (sourceEvents) => {
    const buckets = {
      Green: new Array(bucketCount).fill(0),
      Yellow: new Array(bucketCount).fill(0),
      Red: new Array(bucketCount).fill(0),
      "Flashing Red": new Array(bucketCount).fill(0),
    };
    sourceEvents.forEach((event) => {
      const timestamp = new Date(event.utctime).getTime();
      const index = Math.min(
        bucketCount - 1,
        Math.max(0, Math.floor((timestamp - startMs) / bucketMs))
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
    for (let i = 0; i < segments.length; i += 1) {
      const segment = segments[i];
      if (atMs >= segment.start && atMs < segment.end) return segment.status;
    }
    const fallback = segments[segments.length - 1];
    return fallback ? fallback.status : "Down";
  };

  const summarizeStatuses = (statuses) => {
    if (statuses.indexOf("Up") >= 0) return "Up";
    if (statuses.indexOf("Unstable") >= 0) return "Unstable";
    return "Down";
  };

  const buildConnectivitySegments = () => {
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
          row: link.label,
          link: link.label,
          status: segment.status,
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
        row: "summary",
        link: "summary",
        status: segment.status,
        start: segment.start,
        end: segment.end,
      });
    });

    return laneSegments;
  };

  const getTooltipEnabled = () => {
    const stored = localStorage.getItem(LogApp.STORAGE_KEYS.chartTooltips);
    return stored === "true";
  };

  const chartRatioFromPixel = (chart, pixelX) => {
    const chartArea = getChartArea(chart);
    if (!chartArea || chartArea.right <= chartArea.left) return 0;
    return Math.max(0, Math.min(1, (pixelX - chartArea.left) / (chartArea.right - chartArea.left)));
  };

  const hitBookmarkDot = (chart, pos) => {
    const dots = chart.$bookmarkDots || [];
    for (let i = 0; i < dots.length; i += 1) {
      const dot = dots[i];
      const dx = pos.x - dot.x;
      const dy = pos.y - dot.y;
      if (Math.sqrt(dx * dx + dy * dy) <= 10) return dot;
    }
    return null;
  };

  const drawInfoBox = (ctx, chartArea, lines, anchorX) => {
    if (!lines.length) return;
    const padding = 5;
    const lineHeight = 14;
    ctx.save();
    ctx.font = "12px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace";
    let maxWidth = 0;
    lines.forEach((line) => {
      maxWidth = Math.max(maxWidth, ctx.measureText(line).width);
    });
    const boxWidth = maxWidth + padding * 2;
    const boxHeight = lines.length * lineHeight + padding * 2;
    const boxX = Math.min(
      chartArea.right - boxWidth,
      Math.max(chartArea.left, anchorX - boxWidth / 2)
    );
    const boxY = chartArea.top + 6;
    ctx.fillStyle = "rgba(15, 23, 42, 0.78)";
    ctx.fillRect(boxX, boxY, boxWidth, boxHeight);
    ctx.fillStyle = "#f8fafc";
    ctx.textBaseline = "top";
    lines.forEach((line, index) => {
      ctx.fillText(line, boxX + padding, boxY + padding + index * lineHeight);
    });
    ctx.restore();
  };

  const modeBandPlugin = {
    beforeDatasetsDraw(chartInstance) {
      const chartArea = getChartArea(chartInstance);
      if (!chartArea || !modeSegments.length) return;
      const ctx = chartInstance.ctx;
      ctx.save();
      modeSegments.forEach((mode, idx) => {
        const start = new Date(mode.start).getTime();
        const end = new Date(mode.end).getTime();
        const startRatio = Math.max(0, Math.min(1, (start - startMs) / spanMs));
        const endRatio = Math.max(0, Math.min(1, (end - startMs) / spanMs));
        const x0 = chartArea.left + startRatio * (chartArea.right - chartArea.left);
        const x1 = chartArea.left + endRatio * (chartArea.right - chartArea.left);
        ctx.fillStyle = modeColors[idx % modeColors.length];
        ctx.fillRect(x0, chartArea.top, Math.max(0, x1 - x0), chartArea.bottom - chartArea.top);
        ctx.fillStyle = "rgba(15, 23, 42, 0.5)";
        ctx.font = "11px ui-sans-serif, system-ui, -apple-system, sans-serif";
        ctx.textBaseline = "top";
        ctx.fillText(mode.name, x0 + 4, chartArea.top + 4);
      });
      ctx.restore();
    },
  };

  const connectivityLanePlugin = {
    afterDatasetsDraw(chartInstance) {
      if (chartInstance.$mode !== "connectivity") return;
      const chartArea = getChartArea(chartInstance);
      const xScale = getXScale(chartInstance);
      const segments = chartInstance.$connectivitySegments || [];
      if (!chartArea || !xScale || !segments.length) return;

      const ctx = chartInstance.ctx;
      const laneHeight = (chartArea.bottom - chartArea.top) / connectivityRows.length;

      ctx.save();
      ctx.strokeStyle = "rgba(148, 163, 184, 0.16)";
      ctx.lineWidth = 1;
      for (let i = 0; i <= connectivityRows.length; i += 1) {
        const y = chartArea.top + laneHeight * i;
        ctx.beginPath();
        ctx.moveTo(chartArea.left, y);
        ctx.lineTo(chartArea.right, y);
        ctx.stroke();
      }

      segments.forEach((segment) => {
        const rowIndex = connectivityRows.indexOf(segment.row);
        if (rowIndex < 0) return;
        const x0 = xScale.getPixelForValue(segment.start);
        const x1 = xScale.getPixelForValue(segment.end);
        const yTop = chartArea.top + rowIndex * laneHeight + 3;
        const height = Math.max(4, laneHeight - 6);
        const width = Math.max(0, x1 - x0);
        const colors = statusColors[segment.status] || statusColors.Down;
        if (colors.backgroundColor !== "rgba(0, 0, 0, 0)" && width > 0) {
          ctx.fillStyle = colors.backgroundColor;
          ctx.fillRect(x0, yTop, width, height);
        }
        if (colors.borderColor !== "rgba(0, 0, 0, 0)" && width > 0) {
          ctx.strokeStyle = colors.borderColor;
          ctx.lineWidth = 1;
          ctx.strokeRect(x0 + 0.5, yTop + 0.5, Math.max(0, width - 1), Math.max(0, height - 1));
        }
      });
      ctx.restore();
    },
  };

  const bookmarkPlugin = {
    afterDatasetsDraw(chartInstance) {
      const chartArea = getChartArea(chartInstance);
      if (!chartArea) return;
      const ids = LogApp.bookmarks?.getAll() || [];
      const dots = [];
      const ctx = chartInstance.ctx;
      const dotY = Math.min(chartArea.bottom + 5, chartArea.bottom + 2);
      ctx.save();
      ids.forEach((id) => {
        const event = events.find((entry) => String(entry.row_id) === String(id));
        if (!event) return;
        const colorIndex = LogApp.bookmarks?.getColor(event.row_id) || 1;
        const timestamp = new Date(event.utctime).getTime();
        const ratio = Math.max(0, Math.min(1, (timestamp - startMs) / spanMs));
        const x = chartArea.left + ratio * (chartArea.right - chartArea.left);
        ctx.beginPath();
        ctx.fillStyle =
          getComputedStyle(document.documentElement).getPropertyValue(
            `--bookmark-color-${colorIndex}`
          ) || "rgba(14, 116, 144, 0.9)";
        ctx.arc(x, dotY, 3, 0, Math.PI * 2);
        ctx.fill();
        dots.push({ x, y: dotY, rowId: event.row_id });
      });
      chartInstance.$bookmarkDots = dots;
      ctx.restore();
    },
  };

  const hoverLinePlugin = {
    afterDatasetsDraw(chartInstance) {
      const chartArea = getChartArea(chartInstance);
      const x = chartInstance.$hoverX;
      if (!chartArea || typeof x !== "number") return;

      const ctx = chartInstance.ctx;
      ctx.save();
      ctx.beginPath();
      ctx.moveTo(x, chartArea.top);
      ctx.lineTo(x, chartArea.bottom);
      ctx.lineWidth = 1;
      ctx.strokeStyle = "rgba(100, 116, 139, 0.6)";
      ctx.stroke();

      const tooltipEnabled = chartInstance.$tooltipsEnabled === true;
      const hoverTime = chartInstance.$hoverTime;
      if (!hoverTime) {
        ctx.restore();
        return;
      }

      if (chartInstance.$mode === "connectivity") {
        const hit = chartInstance.$hoverSegment;
        if (tooltipEnabled && hit) {
          drawInfoBox(
            ctx,
            chartArea,
            [
              `${hit.link} ${hit.status}`,
              `${formatTime(hit.start)} - ${formatTime(hit.end)}`,
            ],
            x
          );
        } else if (!tooltipEnabled) {
          drawInfoBox(ctx, chartArea, [formatTime(hoverTime.getTime())], x);
        }
      } else if (!tooltipEnabled) {
        drawInfoBox(ctx, chartArea, [formatTime(hoverTime.getTime())], x);
      }

      ctx.restore();
    },
  };

  const scrollIndicatorPlugin = {
    afterDatasetsDraw(chartInstance) {
      const chartArea = getChartArea(chartInstance);
      const ratio = chartInstance.$scrollRatio;
      if (!chartArea || typeof ratio !== "number") return;
      const x = chartArea.left + ratio * (chartArea.right - chartArea.left);
      const ctx = chartInstance.ctx;
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

  const createSeverityChart = () => {
    const buckets = buildSeverityBuckets(filteredEvents);
    const chart = new Chart(stackedCanvas.getContext("2d"), {
      type: "bar",
      data: {
        labels: labels,
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
        legend: { display: false },
        tooltips: { mode: "index", intersect: false, enabled: getTooltipEnabled() },
        scales: {
          xAxes: [{ stacked: true }],
          yAxes: [{ stacked: true, ticks: { beginAtZero: true } }],
        },
      },
      plugins: [modeBandPlugin, bookmarkPlugin, hoverLinePlugin, scrollIndicatorPlugin],
    });
    chart.$mode = "severity";
    chart.$tooltipsEnabled = getTooltipEnabled();
    return chart;
  };

  const createConnectivityChart = () => {
    const chart = new Chart(stackedCanvas.getContext("2d"), {
      type: "horizontalBar",
      data: {
        labels: connectivityRows,
        datasets: [
          {
            label: "Connectivity",
            data: connectivityRows.map(() => 0),
            backgroundColor: "rgba(0, 0, 0, 0)",
            borderWidth: 0,
            hoverBackgroundColor: "rgba(0, 0, 0, 0)",
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        legend: { display: false },
        tooltips: { enabled: false },
        animation: false,
        hover: { mode: null },
        events: [],
        scales: {
          xAxes: [
            {
              type: "linear",
              ticks: {
                min: startMs,
                max: endMs,
                callback(value) {
                  return formatTime(Number(value)).slice(0, 5);
                },
              },
              gridLines: { color: "rgba(148, 163, 184, 0.15)" },
            },
          ],
          yAxes: [
            {
              gridLines: { display: false },
            },
          ],
        },
      },
      plugins: [
        modeBandPlugin,
        connectivityLanePlugin,
        bookmarkPlugin,
        hoverLinePlugin,
        scrollIndicatorPlugin,
      ],
    });
    chart.$mode = "connectivity";
    chart.$connectivitySegments = buildConnectivitySegments();
    chart.$tooltipsEnabled = getTooltipEnabled();
    return chart;
  };

  const setModeButtons = () => {
    document.querySelectorAll("[data-chart-mode]").forEach((button) => {
      const active = button.dataset.chartMode === chartMode;
      button.setAttribute("aria-pressed", String(active));
      button.classList.toggle("is-enabled", active);
    });
  };

  const setTooltipState = () => {
    const toggleTooltips = document.getElementById("toggle-tooltips");
    if (!toggleTooltips || !stackedChart) return;
    const enabled = getTooltipEnabled();
    stackedChart.$tooltipsEnabled = enabled;
    if (stackedChart.options && stackedChart.options.tooltips) {
      stackedChart.options.tooltips.enabled = chartMode === "severity" ? enabled : false;
    }
    toggleTooltips.setAttribute("aria-pressed", String(enabled));
    toggleTooltips.classList.toggle("tooltip-disabled", !enabled);
    toggleTooltips.classList.toggle("is-enabled", enabled);
    stackedChart.update(0);
  };

  const rebuildChart = () => {
    const previousScrollRatio = stackedChart ? stackedChart.$scrollRatio : undefined;
    const previousHoverX = stackedChart ? stackedChart.$hoverX : undefined;
    const previousHoverTime = stackedChart ? stackedChart.$hoverTime : undefined;
    const previousHoverSegment = stackedChart ? stackedChart.$hoverSegment : undefined;
    if (stackedChart) stackedChart.destroy();
    stackedChart = chartMode === "connectivity" ? createConnectivityChart() : createSeverityChart();
    stackedChart.$scrollRatio = previousScrollRatio;
    stackedChart.$hoverX = previousHoverX;
    stackedChart.$hoverTime = previousHoverTime;
    stackedChart.$hoverSegment = previousHoverSegment;
    setModeButtons();
    setTooltipState();
  };

  const findConnectivitySegmentAt = (chart, pos) => {
    if (chart.$mode !== "connectivity") return null;
    const chartArea = getChartArea(chart);
    if (!chartArea) return null;
    if (pos.x < chartArea.left || pos.x > chartArea.right || pos.y < chartArea.top || pos.y > chartArea.bottom) {
      return null;
    }
    const laneHeight = (chartArea.bottom - chartArea.top) / connectivityRows.length;
    const rowIndex = Math.min(
      connectivityRows.length - 1,
      Math.max(0, Math.floor((pos.y - chartArea.top) / laneHeight))
    );
    const row = connectivityRows[rowIndex];
    const hoverMs = startMs + chartRatioFromPixel(chart, pos.x) * spanMs;
    const segments = chart.$connectivitySegments || [];
    for (let i = 0; i < segments.length; i += 1) {
      const segment = segments[i];
      if (segment.row === row && hoverMs >= segment.start && hoverMs < segment.end) return segment;
    }
    return null;
  };

  const handleChartClick = (event) => {
    if (!stackedChart) return;
    const pos = Chart.helpers.getRelativePosition(event, stackedChart);
    const chartArea = getChartArea(stackedChart);
    if (!chartArea) return;

    const dot = hitBookmarkDot(stackedChart, pos);
    if (dot) {
      const hit = events.find((entry) => String(entry.row_id) === String(dot.rowId));
      if (bus && hit) bus.emit("event:selected", hit);
      if (bus) bus.emit("log:jump", { rowId: dot.rowId });
      return;
    }

    const xScale = getXScale(stackedChart);
    const dotHitBottom = xScale ? xScale.bottom + 12 : chartArea.bottom + 12;
    if (pos.x < chartArea.left || pos.x > chartArea.right) return;
    if (pos.y < chartArea.top || pos.y > dotHitBottom) return;
    const ratio = chartRatioFromPixel(stackedChart, pos.x);
    if (bus) bus.emit("log:jump", { seconds: Math.floor((ratio * spanMs) / 1000) });
  };

  const updateHover = (event) => {
    if (!stackedChart) return;
    const pos = Chart.helpers.getRelativePosition(event, stackedChart);
    const chartArea = getChartArea(stackedChart);
    if (
      !chartArea ||
      pos.x < chartArea.left ||
      pos.x > chartArea.right ||
      pos.y < chartArea.top ||
      pos.y > chartArea.bottom
    ) {
      stackedChart.$hoverX = null;
      stackedChart.$hoverTime = null;
      stackedChart.$hoverSegment = null;
      stackedChart.draw();
      return;
    }

    const ratio = chartRatioFromPixel(stackedChart, pos.x);
    stackedChart.$hoverX = pos.x;
    stackedChart.$hoverTime = new Date(startMs + ratio * spanMs);
    stackedChart.$hoverSegment = findConnectivitySegmentAt(stackedChart, pos);
    stackedChart.draw();
  };

  stackedCanvas.addEventListener("mousemove", updateHover);
  stackedCanvas.addEventListener("mouseleave", () => {
    if (!stackedChart) return;
    stackedChart.$hoverX = null;
    stackedChart.$hoverTime = null;
    stackedChart.$hoverSegment = null;
    stackedChart.draw();
  });
  stackedCanvas.addEventListener("click", (event) => {
    handleChartClick(event);
  });

  const toggleTooltips = document.getElementById("toggle-tooltips");
  if (toggleTooltips) {
    toggleTooltips.addEventListener("click", () => {
      if (!stackedChart) return;
      const next = !getTooltipEnabled();
      localStorage.setItem(LogApp.STORAGE_KEYS.chartTooltips, String(next));
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
    stackedChart.update(0);
  };

  rebuildChart();

  if (bus) {
    bus.on("log:filtered", (filtered) => {
      updateFilteredEvents(filtered || []);
    });
    bus.on("log:scroll", (payload) => {
      if (!stackedChart || !payload || typeof payload.seconds !== "number") return;
      stackedChart.$scrollRatio = Math.max(0, Math.min(1, payload.seconds / (spanMs / 1000)));
      stackedChart.update(0);
    });
    bus.on("bookmarks:changed", () => {
      if (stackedChart) stackedChart.update(0);
    });
  }

  return {
    updateFilteredEvents,
  };
};
