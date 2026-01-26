window.LogApp = window.LogApp || {};

LogApp.initChart = (logData, bus) => {
  if (!logData) return null;

  const stackedCanvas = document.getElementById("stacked-chart");
  if (!stackedCanvas) return null;

  const events = Array.isArray(logData?.events) ? logData.events : [];
  if (!events.length) return null;

  const startTime = new Date(logData.start);
  const endTime = new Date(logData.end);
  const spanMs = Math.max(1, endTime - startTime);
  const bucketMs = 5 * 60 * 1000;
  const bucketCount = Math.max(1, Math.ceil(spanMs / bucketMs));
  const bucketSize = bucketMs;

  const labels = [];
  for (let i = 0; i < bucketCount; i += 1) {
    const t = new Date(startTime.getTime() + i * bucketSize);
    labels.push(t.toISOString().slice(11, 16));
  }

  const buildBuckets = (sourceEvents) => {
    const buckets = {
      green: new Array(bucketCount).fill(0),
      yellow: new Array(bucketCount).fill(0),
      red: new Array(bucketCount).fill(0),
      "dark red": new Array(bucketCount).fill(0),
    };
    sourceEvents.forEach((event) => {
      const timestamp = new Date(event.utc);
      const index = Math.min(
        bucketCount - 1,
        Math.max(0, Math.floor((timestamp - startTime) / bucketSize))
      );
      if (buckets[event.level]) {
        buckets[event.level][index] += 1;
      }
    });
    return buckets;
  };

  const initialBuckets = buildBuckets(events);

  const stackedContext = stackedCanvas.getContext("2d");
  const modeSegments = Array.isArray(logData.modes) ? logData.modes : [];
  const modeColors = ["rgba(59, 130, 246, 0.08)", "rgba(14, 165, 233, 0.08)", "rgba(16, 185, 129, 0.08)", "rgba(249, 115, 22, 0.08)"];
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
      const { ctx, chartArea } = chart;
      const ids = LogApp.bookmarks?.getAll() || [];
      const dots = [];
      ctx.save();
      ids.forEach((id) => {
        const event = events.find((entry) => String(entry.row_id) === String(id));
        if (!event) return;
        const timestamp = new Date(event.utc).getTime();
        const ratio = Math.max(0, Math.min(1, (timestamp - startTime.getTime()) / spanMs));
        const x = chartArea.left + ratio * chartArea.width;
        const y = chartArea.bottom - 6;
        ctx.beginPath();
        ctx.fillStyle = "rgba(14, 116, 144, 0.9)";
        ctx.arc(x, y, 3, 0, Math.PI * 2);
        ctx.fill();
        dots.push({ x, y, rowId: event.row_id });
      });
      chart.$bookmarkDots = dots;
      ctx.restore();
    },
  };

  const stackedChart = new Chart(stackedContext, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Green",
          data: initialBuckets.green,
          backgroundColor: "rgba(34, 197, 94, 0.75)",
          borderColor: "rgba(22, 163, 74, 1)",
          borderWidth: 1,
        },
        {
          label: "Yellow",
          data: initialBuckets.yellow,
          backgroundColor: "rgba(250, 204, 21, 0.8)",
          borderColor: "rgba(234, 179, 8, 1)",
          borderWidth: 1,
        },
        {
          label: "Red",
          data: initialBuckets.red,
          backgroundColor: "rgba(239, 68, 68, 0.8)",
          borderColor: "rgba(220, 38, 38, 1)",
          borderWidth: 1,
        },
        {
          label: "Dark Red",
          data: initialBuckets["dark red"],
          backgroundColor: "rgba(127, 29, 29, 0.85)",
          borderColor: "rgba(88, 28, 28, 1)",
          borderWidth: 1,
        },
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
        const { chartArea } = stackedChart;
        if (pos.x < chartArea.left || pos.x > chartArea.right) return;
        const dots = stackedChart.$bookmarkDots || [];
        for (const dot of dots) {
          const dx = pos.x - dot.x;
          const dy = pos.y - dot.y;
          if (Math.sqrt(dx * dx + dy * dy) <= 6) {
            if (bus) bus.emit("log:jump", { rowId: dot.rowId });
            return;
          }
        }
        const ratio = (pos.x - chartArea.left) / chartArea.width;
        const targetSeconds = Math.floor((ratio * spanMs) / 1000);
        if (bus) bus.emit("log:jump", { seconds: targetSeconds });
      },
    },
    plugins: [modeBandPlugin, bookmarkPlugin, hoverLinePlugin],
  });

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
    const hoverMs = startTime.getTime() + ratio * spanMs;
    stackedChart.$hoverX = pos.x;
    stackedChart.$hoverTime = new Date(hoverMs);
    stackedChart.draw();
  };

  stackedCanvas.addEventListener("mousemove", updateHover);
  stackedCanvas.addEventListener("mouseleave", () => {
    stackedChart.$hoverX = null;
    stackedChart.$hoverTime = null;
    stackedChart.draw();
  });

  const toggleTooltips = document.getElementById("toggle-tooltips");
  if (toggleTooltips) {
    const stored = localStorage.getItem(LogApp.STORAGE_KEYS.chartTooltips);
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
      localStorage.setItem(LogApp.STORAGE_KEYS.chartTooltips, String(!current));
      stackedChart.update();
    });
  }

  const updateFilteredEvents = (filteredEvents) => {
    const buckets = buildBuckets(filteredEvents);
    stackedChart.data.datasets[0].data = buckets.green;
    stackedChart.data.datasets[1].data = buckets.yellow;
    stackedChart.data.datasets[2].data = buckets.red;
    stackedChart.data.datasets[3].data = buckets["dark red"];
    stackedChart.update();
  };

  if (bus) {
    bus.on("log:filtered", (filtered) => {
      updateFilteredEvents(filtered || []);
    });
    bus.on("bookmarks:changed", () => {
      stackedChart.update();
    });
  }

  return {
    updateFilteredEvents,
  };
};
