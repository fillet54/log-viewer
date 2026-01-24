const STORAGE_KEYS = {
  root: "loglayout.split.root",
  top: "loglayout.split.top",
  bottomCollapsed: "loglayout.split.bottom.collapsed",
};

const loadSizes = (key, fallback) => {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return fallback;
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed) || parsed.length !== fallback.length) return fallback;
    if (parsed.some((value) => typeof value !== "number")) return fallback;
    return parsed;
  } catch (err) {
    return fallback;
  }
};

const saveSizes = (key, sizes) => {
  localStorage.setItem(key, JSON.stringify(sizes));
};

let rootSplit = null;

const setBottomCollapsed = (collapsed) => {
  const root = document.getElementById("split-root");
  const button = document.getElementById("toggle-bottom");
  if (!root || !button || !rootSplit) return;

  if (collapsed) {
    root.classList.add("bottom-collapsed");
    rootSplit.setSizes([100, 0]);
    button.textContent = "Expand Timeline";
  } else {
    root.classList.remove("bottom-collapsed");
    const saved = loadSizes(STORAGE_KEYS.root, [70, 30]);
    rootSplit.setSizes(saved);
    button.textContent = "Collapse Timeline";
  }
};

const initSplits = () => {
  const rootSizes = loadSizes(STORAGE_KEYS.root, [70, 30]);
  const topSizes = loadSizes(STORAGE_KEYS.top, [22, 56, 22]);
  const isCollapsed = localStorage.getItem(STORAGE_KEYS.bottomCollapsed) === "true";

  rootSplit = Split(["#pane-top", "#pane-bottom"], {
    sizes: isCollapsed ? [100, 0] : rootSizes,
    direction: "vertical",
    gutterSize: 10,
    minSize: [240, 52],
    onDragEnd: (sizes) => {
      saveSizes(STORAGE_KEYS.root, sizes);
      if (sizes[1] === 0) {
        localStorage.setItem(STORAGE_KEYS.bottomCollapsed, "true");
      } else {
        localStorage.setItem(STORAGE_KEYS.bottomCollapsed, "false");
      }
    },
  });

  Split(["#pane-left", "#pane-center", "#pane-right"], {
    sizes: topSizes,
    direction: "horizontal",
    gutterSize: 10,
    minSize: [180, 320, 200],
    onDragEnd: (sizes) => saveSizes(STORAGE_KEYS.top, sizes),
  });

  const toggleButton = document.getElementById("toggle-bottom");
  if (toggleButton) {
    toggleButton.addEventListener("click", () => {
      const next = !(localStorage.getItem(STORAGE_KEYS.bottomCollapsed) === "true");
      localStorage.setItem(STORAGE_KEYS.bottomCollapsed, String(next));
      setBottomCollapsed(next);
    });
  }

  setBottomCollapsed(isCollapsed);
};

const loadLogData = () => {
  const logPayload = document.getElementById("log-data");
  if (!logPayload) return null;
  try {
    return JSON.parse(logPayload.textContent || "{}");
  } catch (err) {
    return null;
  }
};

const buildLogRow = (event) => {
  const row = document.createElement("div");
  row.className = `log-line log-${event.level.replace(" ", "-")}`;
  row.dataset.seconds = event.seconds_from_start;
  row.dataset.rowId = event.row_id;
  row.innerHTML = `
    <span class="badge badge-sm level-tag">${event.level}</span>
    <span class="log-time text-base-content/60">${event.utc}</span>
    <span class="log-action font-semibold">${event.action}</span>
    <span class="log-name">${event.name}</span>
    <span class="log-offset text-base-content/60">${event.seconds_from_start}s</span>
    <span class="log-desc text-base-content/70">${event.description}</span>
    <span class="log-code text-base-content/50">${event.system}/${event.subsystem}/${event.unit}/${event.code}</span>
  `;
  return row;
};

const initLogList = (logData) => {
  const logBody = document.getElementById("log-body");
  const logList = document.getElementById("log-list");
  const logSpacer = document.getElementById("log-spacer");
  const searchInput = document.getElementById("log-search");

  if (!logBody || !logList || !logSpacer || !logData) return null;
  const events = Array.isArray(logData.events) ? logData.events : [];
  if (!events.length) return null;

  const sample = buildLogRow(events[0]);
  sample.style.visibility = "hidden";
  logList.appendChild(sample);
  const rowHeight = sample.getBoundingClientRect().height || 38;
  const listStyle = getComputedStyle(logList);
  const gap = parseFloat(listStyle.rowGap || listStyle.gap || "0") || 0;
  logList.removeChild(sample);
  const rowStride = rowHeight + gap;

  const state = {
    events,
    filtered: events,
    rowStride,
    overscan: 8,
    lastRange: [0, 0],
    indexByRowId: new Map(),
  };

  const rebuildIndex = () => {
    state.indexByRowId.clear();
    state.filtered.forEach((event, idx) => {
      state.indexByRowId.set(String(event.row_id), idx);
    });
  };

  const setSpacer = () => {
    logSpacer.style.height = `${state.filtered.length * state.rowStride}px`;
  };

  const renderRange = (startIndex, endIndex) => {
    logList.style.transform = `translateY(${startIndex * state.rowStride}px)`;
    logList.innerHTML = "";
    const fragment = document.createDocumentFragment();
    for (let i = startIndex; i < endIndex; i += 1) {
      fragment.appendChild(buildLogRow(state.filtered[i]));
    }
    logList.appendChild(fragment);
  };

  const updateVirtual = () => {
    const scrollTop = logBody.scrollTop;
    const startIndex = Math.max(0, Math.floor(scrollTop / state.rowStride) - state.overscan);
    const visibleCount = Math.ceil(logBody.clientHeight / state.rowStride) + state.overscan * 2;
    const endIndex = Math.min(state.filtered.length, startIndex + visibleCount);
    if (state.lastRange[0] === startIndex && state.lastRange[1] === endIndex) return;
    state.lastRange = [startIndex, endIndex];
    renderRange(startIndex, endIndex);
  };

  const applyFilter = (query) => {
    const term = query.trim().toLowerCase();
    if (!term) {
      state.filtered = state.events;
    } else {
      state.filtered = state.events.filter((event) => {
        const haystack = `${event.level} ${event.action} ${event.name} ${event.description} ${event.system} ${event.subsystem} ${event.unit} ${event.code}`;
        return haystack.toLowerCase().includes(term);
      });
    }
    rebuildIndex();
    setSpacer();
    state.lastRange = [0, 0];
    updateVirtual();
  };

  const findClosestIndexBySeconds = (targetSeconds) => {
    const list = state.filtered;
    if (!list.length) return null;
    let lo = 0;
    let hi = list.length - 1;
    while (lo <= hi) {
      const mid = Math.floor((lo + hi) / 2);
      const seconds = list[mid].seconds_from_start;
      if (seconds === targetSeconds) return mid;
      if (seconds < targetSeconds) {
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    if (lo >= list.length) return list.length - 1;
    if (hi < 0) return 0;
    const loDiff = Math.abs(list[lo].seconds_from_start - targetSeconds);
    const hiDiff = Math.abs(list[hi].seconds_from_start - targetSeconds);
    return loDiff < hiDiff ? lo : hi;
  };

  const scrollToIndex = (index, duration = 180) => {
    if (index == null) return null;
    const targetTop =
      index * state.rowStride - logBody.clientHeight / 2 + state.rowStride / 2;
    const clamped = Math.max(0, Math.min(targetTop, logBody.scrollHeight));
    smoothScrollTo(logBody, clamped, duration, () => {
      const event = state.filtered[index];
      if (!event) return;
      const row = logList.querySelector(`[data-row-id="${event.row_id}"]`);
      if (!row) return;
      row.classList.remove("log-highlight");
      void row.offsetWidth;
      row.classList.add("log-highlight");
    });
    return state.filtered[index];
  };

  const scrollToSeconds = (seconds) => {
    const index = findClosestIndexBySeconds(seconds);
    return scrollToIndex(index);
  };

  const scrollToRowId = (rowId) => {
    const index = state.indexByRowId.get(String(rowId));
    return scrollToIndex(index);
  };

  logBody.addEventListener("scroll", () => {
    requestAnimationFrame(updateVirtual);
  });

  if (searchInput) {
    let debounce = null;
    searchInput.addEventListener("input", (event) => {
      if (debounce) window.clearTimeout(debounce);
      const value = event.target.value;
      debounce = window.setTimeout(() => applyFilter(value), 150);
    });
  }

  rebuildIndex();
  setSpacer();
  updateVirtual();

  return {
    scrollToSeconds,
    scrollToRowId,
    getFilteredEvents: () => state.filtered,
  };
};

const initChart = (logData, logController) => {
  if (!logData) return;

  const timelineCanvas = document.getElementById("chart");
  if (timelineCanvas) {
    const context = timelineCanvas.getContext("2d");
    new Chart(context, {
      type: "line",
      data: {
        labels: ["12:00", "12:05", "12:10", "12:15", "12:20", "12:25", "12:30"],
        datasets: [
          {
            label: "Errors",
            data: [2, 3, 1, 4, 2, 5, 3],
            borderColor: "#ef4444",
            backgroundColor: "rgba(239, 68, 68, 0.15)",
            tension: 0.35,
            fill: true,
          },
          {
            label: "Warnings",
            data: [4, 2, 3, 2, 4, 3, 2],
            borderColor: "#f59e0b",
            backgroundColor: "rgba(245, 158, 11, 0.15)",
            tension: 0.35,
            fill: true,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { position: "bottom" },
        },
        scales: {
          y: { beginAtZero: true },
        },
      },
    });
  }

  const stackedCanvas = document.getElementById("stacked-chart");
  if (!stackedCanvas) return;

  const events = Array.isArray(logData?.events) ? logData.events : [];
  if (!events.length) return;

  const startTime = new Date(logData.start);
  const endTime = new Date(logData.end);
  const spanMs = Math.max(1, endTime - startTime);
  const bucketMs = 5 * 60 * 1000;
  const bucketCount = Math.max(1, Math.ceil(spanMs / bucketMs));
  const bucketSize = bucketMs;

  const labels = [];
  const levelBuckets = {
    green: new Array(bucketCount).fill(0),
    yellow: new Array(bucketCount).fill(0),
    red: new Array(bucketCount).fill(0),
    "dark red": new Array(bucketCount).fill(0),
  };

  for (let i = 0; i < bucketCount; i += 1) {
    const t = new Date(startTime.getTime() + i * bucketSize);
    labels.push(t.toISOString().slice(11, 16));
  }

  events.forEach((event) => {
    const timestamp = new Date(event.utc);
    const index = Math.min(
      bucketCount - 1,
      Math.max(0, Math.floor((timestamp - startTime) / bucketSize))
    );
    if (levelBuckets[event.level]) {
      levelBuckets[event.level][index] += 1;
    }
  });

  const stackedContext = stackedCanvas.getContext("2d");
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

  const stackedChart = new Chart(stackedContext, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Green",
          data: levelBuckets.green,
          backgroundColor: "rgba(34, 197, 94, 0.75)",
          borderColor: "rgba(22, 163, 74, 1)",
          borderWidth: 1,
        },
        {
          label: "Yellow",
          data: levelBuckets.yellow,
          backgroundColor: "rgba(250, 204, 21, 0.8)",
          borderColor: "rgba(234, 179, 8, 1)",
          borderWidth: 1,
        },
        {
          label: "Red",
          data: levelBuckets.red,
          backgroundColor: "rgba(239, 68, 68, 0.8)",
          borderColor: "rgba(220, 38, 38, 1)",
          borderWidth: 1,
        },
        {
          label: "Dark Red",
          data: levelBuckets["dark red"],
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
        const ratio = (pos.x - chartArea.left) / chartArea.width;
        const targetSeconds = Math.floor((ratio * spanMs) / 1000);
        if (logController) {
          logController.scrollToSeconds(targetSeconds);
        }
      },
    },
    plugins: [hoverLinePlugin],
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
    toggleTooltips.addEventListener("click", () => {
      const current = stackedChart.options.plugins.tooltip.enabled !== false;
      stackedChart.options.plugins.tooltip.enabled = !current;
      toggleTooltips.setAttribute("aria-pressed", String(!current));
      toggleTooltips.classList.toggle("tooltip-disabled", current);
      stackedChart.update();
    });
  }
};

window.addEventListener("DOMContentLoaded", () => {
  initSplits();
  const logData = loadLogData();
  const logController = initLogList(logData);
  initChart(logData, logController);
});

const smoothScrollTo = (container, targetTop, durationMs = 200, onComplete = null) => {
  const startTop = container.scrollTop;
  const delta = targetTop - startTop;
  if (Math.abs(delta) < 2) {
    container.scrollTop = targetTop;
    if (onComplete) onComplete();
    return;
  }
  const startTime = performance.now();
  const easeOut = (t) => 1 - Math.pow(1 - t, 3);
  const step = (now) => {
    const elapsed = now - startTime;
    const progress = Math.min(1, elapsed / durationMs);
    container.scrollTop = startTop + delta * easeOut(progress);
    if (progress < 1) {
      requestAnimationFrame(step);
    } else if (onComplete) {
      onComplete();
    }
  };
  requestAnimationFrame(step);
};
