const STORAGE_KEYS = {
  root: "loglayout.split.root",
  top: "loglayout.split.top",
  bottomCollapsed: "loglayout.split.bottom.collapsed",
  search: "loglayout.split.search",
  rootExpanded: "loglayout.split.root.expanded",
  searchHistory: "loglayout.search.history",
  searchPinned: "loglayout.search.pinned",
  searchFilters: "loglayout.search.filters",
};

const createEventBus = () => {
  const listeners = new Map();
  return {
    on(event, handler) {
      if (!listeners.has(event)) listeners.set(event, new Set());
      listeners.get(event).add(handler);
      return () => listeners.get(event)?.delete(handler);
    },
    emit(event, payload) {
      const handlers = listeners.get(event);
      if (!handlers) return;
      handlers.forEach((handler) => handler(payload));
    },
  };
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
  const headerText = document.getElementById("search-header-text");
  const headerTabs = document.getElementById("search-header-tabs");
  if (!root || !button || !rootSplit) return;

  if (collapsed) {
    root.classList.add("bottom-collapsed");
    rootSplit.setSizes([96, 4]);
    button.setAttribute("aria-label", "Expand search pane");
    button.classList.add("is-collapsed");
    if (headerText) {
      headerText.textContent = "Search";
      headerText.classList.remove("hidden");
    }
    if (headerTabs) headerTabs.classList.add("hidden");
  } else {
    root.classList.remove("bottom-collapsed");
    const savedExpanded = loadSizes(STORAGE_KEYS.rootExpanded, [70, 30]);
    const safeSizes = savedExpanded[1] > 0 ? savedExpanded : [70, 30];
    rootSplit.setSizes(safeSizes);
    button.setAttribute("aria-label", "Collapse search pane");
    button.classList.remove("is-collapsed");
    if (headerText) headerText.classList.add("hidden");
    if (headerTabs) headerTabs.classList.remove("hidden");
  }
};

const initSplits = () => {
  const rootSizes = loadSizes(STORAGE_KEYS.root, [70, 30]);
  const topSizes = loadSizes(STORAGE_KEYS.top, [70, 30]);
  const isCollapsed = localStorage.getItem(STORAGE_KEYS.bottomCollapsed) === "true";

  rootSplit = Split(["#pane-top", "#pane-bottom"], {
    sizes: isCollapsed ? [100, 0] : rootSizes,
    direction: "vertical",
    gutterSize: 10,
    minSize: [240, 0],
    onDragEnd: (sizes) => {
      saveSizes(STORAGE_KEYS.root, sizes);
      if (sizes[1] > 6) {
        saveSizes(STORAGE_KEYS.rootExpanded, sizes);
      }
      if (sizes[1] <= 6) {
        localStorage.setItem(STORAGE_KEYS.bottomCollapsed, "true");
      } else {
        localStorage.setItem(STORAGE_KEYS.bottomCollapsed, "false");
      }
    },
  });

  Split(["#pane-center", "#pane-right"], {
    sizes: topSizes,
    direction: "horizontal",
    gutterSize: 10,
    minSize: [420, 240],
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

  const searchSizes = loadSizes(STORAGE_KEYS.search, [28, 72]);
  const searchHistory = document.getElementById("search-history-pane");
  const searchResults = document.getElementById("search-results-pane");
  if (searchHistory && searchResults) {
    Split(["#search-history-pane", "#search-results-pane"], {
      sizes: searchSizes,
      direction: "horizontal",
      gutterSize: 8,
      minSize: [160, 320],
      onDragEnd: (sizes) => saveSizes(STORAGE_KEYS.search, sizes),
    });
  }
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

const initLogList = (logData, bus) => {
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
    filterQuery: "",
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

  const matchesQuery = (event, term) => {
    const haystack = `${event.level} ${event.action} ${event.name} ${event.description} ${event.system} ${event.subsystem} ${event.unit} ${event.code}`;
    return haystack.toLowerCase().includes(term);
  };

  const applyFilterQueries = (queries) => {
    const terms = queries.map((q) => q.trim().toLowerCase()).filter(Boolean);
    state.filterQuery = terms.join(" | ");
    if (!terms.length) {
      state.filtered = state.events;
    } else {
      state.filtered = state.events.filter((event) => terms.some((term) => matchesQuery(event, term)));
    }
    rebuildIndex();
    setSpacer();
    state.lastRange = [0, 0];
    updateVirtual();
    if (bus) bus.emit("log:filtered", state.filtered);
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

  const ensureRowVisible = (rowId) => {
    if (!state.indexByRowId.has(String(rowId))) {
      applyFilter("");
      if (searchInput) searchInput.value = "";
    }
    return scrollToRowId(rowId);
  };

  logBody.addEventListener("scroll", () => {
    requestAnimationFrame(updateVirtual);
  });

  if (searchInput) {
    let debounce = null;
    searchInput.addEventListener("input", (event) => {
      if (debounce) window.clearTimeout(debounce);
      const value = event.target.value;
      debounce = window.setTimeout(() => applyFilterQueries([value]), 150);
    });
  }

  rebuildIndex();
  setSpacer();
  updateVirtual();
  if (bus) bus.emit("log:filtered", state.filtered);

  if (bus) {
    bus.on("filters:apply", (queries) => applyFilterQueries(queries || []));
    bus.on("log:jump", (payload) => {
      if (!payload) return;
      if (payload.rowId != null) ensureRowVisible(payload.rowId);
      if (payload.seconds != null) scrollToSeconds(payload.seconds);
    });
  }

  return {
    scrollToSeconds,
    scrollToRowId,
    ensureRowVisible,
    applyFilters: applyFilterQueries,
    getFilteredEvents: () => state.filtered,
  };
};

const initSearchPane = (logData, bus) => {
  const pinnedList = document.getElementById("search-pinned");
  const historyList = document.getElementById("search-history");
  const filtersList = document.getElementById("search-filters");
  const resultsList = document.getElementById("search-results");
  const queryInput = document.getElementById("search-query");
  const runButton = document.getElementById("run-search");
  const tabHistory = document.getElementById("tab-history");
  const tabFilters = document.getElementById("tab-filters");
  const historyView = document.getElementById("search-history-view");
  const filterView = document.getElementById("search-filter-view");

  if (
    !pinnedList ||
    !historyList ||
    !filtersList ||
    !resultsList ||
    !queryInput ||
    !runButton ||
    !tabHistory ||
    !tabFilters ||
    !historyView ||
    !filterView ||
    !logData
  )
    return;
  const events = Array.isArray(logData.events) ? logData.events : [];

  const pinIcon = `
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7">
      <path stroke-linecap="round" stroke-linejoin="round" d="M9 3h6l-1 6 3 3-1.5 1.5L12 10l-3.5 3.5L7 12l3-3-1-6Z" />
      <path stroke-linecap="round" stroke-linejoin="round" d="M12 10v9" />
    </svg>
  `;

  const filterIcon = `
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7">
      <path stroke-linecap="round" stroke-linejoin="round" d="M4 5h16l-6 7v6l-4 2v-8L4 5Z" />
    </svg>
  `;

  const removeIcon = `
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7">
      <path stroke-linecap="round" stroke-linejoin="round" d="M6 6l12 12M18 6l-12 12" />
    </svg>
  `;

  const renderList = (items, container, isPinned) => {
    container.innerHTML = "";
    const fragment = document.createDocumentFragment();
    items.forEach((item) => {
      const row = document.createElement("div");
      row.className = "search-item search-history-item";
      row.innerHTML = `
        <span class="search-level">${item.level}</span>
        <span>${item.label}</span>
        <span class="search-time">${item.count}</span>
        <button class="pin-button ${isPinned ? "is-pinned" : ""}" title="${isPinned ? "Unpin" : "Pin"}">
          ${pinIcon}
        </button>
        <button class="pin-button promote-button" title="Promote to filter">
          ${filterIcon}
        </button>
      `;
      row.querySelector(".pin-button").addEventListener("click", (event) => {
        event.stopPropagation();
        togglePin(item.query);
      });
      row.querySelector(".promote-button").addEventListener("click", (event) => {
        event.stopPropagation();
        promoteFilter(item.query);
      });
      row.addEventListener("click", () => {
        queryInput.value = item.query;
        runSearch();
      });
      fragment.appendChild(row);
    });
    container.appendChild(fragment);
  };

  const loadStored = (key, fallback) => {
    try {
      const raw = localStorage.getItem(key);
      if (!raw) return fallback;
      const parsed = JSON.parse(raw);
      return Array.isArray(parsed) ? parsed : fallback;
    } catch (err) {
      return fallback;
    }
  };

  const history = loadStored(STORAGE_KEYS.searchHistory, []);
  const pinned = loadStored(STORAGE_KEYS.searchPinned, []);
  const filters = loadStored(STORAGE_KEYS.searchFilters, []);

  const persist = () => {
    localStorage.setItem(STORAGE_KEYS.searchHistory, JSON.stringify(history));
    localStorage.setItem(STORAGE_KEYS.searchPinned, JSON.stringify(pinned));
    localStorage.setItem(STORAGE_KEYS.searchFilters, JSON.stringify(filters));
  };

  const isPinned = (query) => pinned.some((item) => item.query === query);

  const togglePin = (query) => {
    const index = pinned.findIndex((item) => item.query === query);
    if (index >= 0) {
      pinned.splice(index, 1);
    } else {
      const item = history.find((entry) => entry.query === query) || {
        query,
        count: 0,
        level: "search",
        label: query || "(all events)",
      };
      pinned.unshift(item);
    }
    persist();
    renderPinned();
  };

  const promoteFilter = (query) => {
    if (!query) return;
    const existing = filters.find((item) => item.query === query);
    if (existing) {
      existing.enabled = true;
    } else {
      filters.unshift({ query, enabled: true });
    }
    persist();
    renderFilters();
    applyFilters();
  };

  const renderPinned = () => {
    renderList(pinned.slice(0, 24), pinnedList, true);
  };

  const renderHistory = () => {
    renderList(history.slice(0, 50), historyList, false);
  };

  const addHistory = (query, count, level) => {
    if (query === "" && !count) return;
    const existingIndex = history.findIndex((item) => item.query === query);
    const item = {
      query,
      count,
      level: level || "search",
      label: query || "(all events)",
    };
    if (existingIndex >= 0) {
      history.splice(existingIndex, 1);
    }
    history.unshift(item);
    history.splice(50);
    persist();
    renderHistory();
  };

  const renderFilters = () => {
    filtersList.innerHTML = "";
    const fragment = document.createDocumentFragment();
    filters.forEach((filter) => {
      const row = document.createElement("div");
      row.className = "search-item search-filter-item";
      row.innerHTML = `
        <span>${filter.query}</span>
        <button class="filter-toggle ${filter.enabled ? "is-on" : ""}" aria-pressed="${filter.enabled}">
        </button>
        <button class="pin-button" title="Remove filter">
          ${removeIcon}
        </button>
      `;
      row.querySelector(".filter-toggle").addEventListener("click", (event) => {
        event.stopPropagation();
        filter.enabled = !filter.enabled;
        persist();
        renderFilters();
        applyFilters();
      });
      row.querySelector(".pin-button").addEventListener("click", (event) => {
        event.stopPropagation();
        const index = filters.indexOf(filter);
        if (index >= 0) filters.splice(index, 1);
        persist();
        renderFilters();
        applyFilters();
      });
      fragment.appendChild(row);
    });
    filtersList.appendChild(fragment);
  };

  const applyFilters = () => {
    const active = filters.filter((item) => item.enabled).map((item) => item.query);
    if (bus) bus.emit("filters:apply", active);
  };

  const renderResults = (items) => {
    resultsList.innerHTML = "";
    const fragment = document.createDocumentFragment();
    items.forEach((event) => {
      const row = document.createElement("div");
      row.className = "search-item search-result";
      row.innerHTML = `
        <span class="search-level">${event.level}</span>
        <span>${event.name} — ${event.description}</span>
        <span class="search-time">${event.utc.slice(11, 19)}</span>
      `;
      row.addEventListener("click", () => {
        if (bus) bus.emit("log:jump", { rowId: event.row_id });
      });
      fragment.appendChild(row);
    });
    resultsList.appendChild(fragment);
  };

  const runSearch = () => {
    const query = queryInput.value.trim().toLowerCase();
    const filtered = events.filter((event) => {
      if (!query) return true;
      const haystack = `${event.level} ${event.action} ${event.name} ${event.description} ${event.system} ${event.subsystem} ${event.unit} ${event.code}`;
      return haystack.toLowerCase().includes(query);
    });
    renderResults(filtered);
    addHistory(query, filtered.length, filtered[0]?.level);
  };

  runButton.addEventListener("click", runSearch);
  queryInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") runSearch();
  });

  const setTab = (tab) => {
    const isHistory = tab === "history";
    tabHistory.classList.toggle("is-active", isHistory);
    tabFilters.classList.toggle("is-active", !isHistory);
    historyView.classList.toggle("hidden", !isHistory);
    filterView.classList.toggle("hidden", isHistory);
  };

  tabHistory.addEventListener("click", () => setTab("history"));
  tabFilters.addEventListener("click", () => setTab("filters"));

  renderPinned();
  renderHistory();
  renderFilters();
  renderResults(events.slice(0, 40));
  applyFilters();
};

const initChart = (logData, bus) => {
  if (!logData) return;

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

  const initialEvents = events;
  const levelBuckets = buildBuckets(initialEvents);

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
        if (bus) bus.emit("log:jump", { seconds: targetSeconds });
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
    const setToggleState = (enabled) => {
      toggleTooltips.setAttribute("aria-pressed", String(enabled));
      toggleTooltips.classList.toggle("tooltip-disabled", !enabled);
      toggleTooltips.classList.toggle("is-enabled", enabled);
    };
    setToggleState(true);
    toggleTooltips.addEventListener("click", () => {
      const current = stackedChart.options.plugins.tooltip.enabled !== false;
      stackedChart.options.plugins.tooltip.enabled = !current;
      setToggleState(!current);
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
  }

  return {
    updateFilteredEvents,
  };
};

window.addEventListener("DOMContentLoaded", () => {
  initSplits();
  const bus = createEventBus();
  const logData = loadLogData();
  initLogList(logData, bus);
  initChart(logData, bus);
  initSearchPane(logData, bus);
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
