window.LogApp = window.LogApp || {};

LogApp.initSearchPane = (logData, bus) => {
  const pinnedList = document.getElementById("search-pinned");
  const historyList = document.getElementById("search-history");
  const filtersList = document.getElementById("search-filters");
  const bookmarksList = document.getElementById("search-bookmarks");
  const resultsList = document.getElementById("search-results");
  const resultsSpacer = document.getElementById("search-results-spacer");
  const resultsItems = document.getElementById("search-results-list");
  const queryInput = document.getElementById("search-query");
  const runButton = document.getElementById("run-search");
  const clearHistoryButton = document.getElementById("clear-history");
  const tabHistory = document.getElementById("tab-history");
  const tabFilters = document.getElementById("tab-filters");
  const tabBookmarks = document.getElementById("tab-bookmarks");
  const historyView = document.getElementById("search-history-view");
  const filterView = document.getElementById("search-filter-view");
  const bookmarkView = document.getElementById("search-bookmark-view");
  const searchSplit = document.getElementById("search-split");

  if (
    !pinnedList ||
    !historyList ||
    !filtersList ||
    !bookmarksList ||
    !resultsList ||
    !resultsSpacer ||
    !resultsItems ||
    !queryInput ||
    !runButton ||
    !clearHistoryButton ||
    !tabHistory ||
    !tabFilters ||
    !tabBookmarks ||
    !historyView ||
    !filterView ||
    !bookmarkView ||
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

  const { STORAGE_KEYS } = LogApp;
  const history = loadStored(STORAGE_KEYS.searchHistory, []);
  const pinned = loadStored(STORAGE_KEYS.searchPinned, []);
  const filters = loadStored(STORAGE_KEYS.searchFilters, []);

  const persist = () => {
    localStorage.setItem(STORAGE_KEYS.searchHistory, JSON.stringify(history));
    localStorage.setItem(STORAGE_KEYS.searchPinned, JSON.stringify(pinned));
    localStorage.setItem(STORAGE_KEYS.searchFilters, JSON.stringify(filters));
  };

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

  const clearHistory = () => {
    const pinnedQueries = new Set(pinned.map((item) => item.query));
    const filterQueries = new Set(filters.map((item) => item.query));
    const remaining = history.filter(
      (item) => pinnedQueries.has(item.query) || filterQueries.has(item.query)
    );
    history.splice(0, history.length, ...remaining);
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
        <button class="filter-toggle ${filter.enabled ? "is-on" : ""}" aria-pressed="${filter.enabled}"></button>
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

  const getBookmarkEvents = () => {
    const ids = LogApp.bookmarks?.getAll() || [];
    return ids
      .map((id) => events.find((entry) => String(entry.row_id) === String(id)))
      .filter(Boolean);
  };

  const renderBookmarks = () => {
    bookmarksList.innerHTML = "";
    const fragment = document.createDocumentFragment();
    getBookmarkEvents().forEach((event) => {
      const row = document.createElement("div");
      const colorIndex = LogApp.bookmarks?.getColor(event.row_id) || 0;
      row.className = `log-line log-${event.level.replace(" ", "-")} search-result-row${
        colorIndex ? " is-bookmarked" : ""
      }`;
      row.dataset.bookmarkColor = String(colorIndex);
      row.innerHTML = `
        <span class="badge badge-sm level-tag">${event.level}</span>
        <span class="log-time text-base-content/60">${event.utc}</span>
        <span class="log-action font-semibold">${event.action}</span>
        <span class="log-name">${event.name}</span>
        <span class="log-offset text-base-content/60">${event.seconds_from_start}s</span>
        <span class="log-desc text-base-content/70">${event.description}</span>
        <span class="log-code text-base-content/50">${event.system}/${event.subsystem}/${event.unit}/${event.code}</span>
        <button class="bookmark-toggle" title="Toggle bookmark">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6">
            <path stroke-linecap="round" stroke-linejoin="round" d="M6 4h12v16l-6-3-6 3z" />
          </svg>
        </button>
      `;
      row.querySelector(".bookmark-toggle").addEventListener("click", (eventClick) => {
        eventClick.stopPropagation();
        LogApp.bookmarks?.cycle(event.row_id);
        renderBookmarks();
        if (bus) bus.emit("bookmarks:changed", LogApp.bookmarks?.getAllWithColors() || {});
      });
      row.addEventListener("click", () => {
        if (bus) bus.emit("event:selected", event);
        if (bus) bus.emit("log:jump", { rowId: event.row_id });
      });
      fragment.appendChild(row);
    });
    bookmarksList.appendChild(fragment);
  };

  const applyFilters = () => {
    const active = filters.filter((item) => item.enabled).map((item) => item.query);
    if (bus) bus.emit("filters:apply", active);
  };

  const renderResultRow = (event) => {
    const row = document.createElement("div");
    const colorIndex = LogApp.bookmarks?.getColor(event.row_id) || 0;
    row.className = `log-line log-${event.level.replace(" ", "-")} search-result-row${
      colorIndex ? " is-bookmarked" : ""
    }`;
    row.dataset.bookmarkColor = String(colorIndex);
    row.innerHTML = `
      <span class="badge badge-sm level-tag">${event.level}</span>
      <span class="log-time text-base-content/60">${event.utc}</span>
      <span class="log-action font-semibold">${event.action}</span>
      <span class="log-name">${event.name}</span>
      <span class="log-offset text-base-content/60">${event.seconds_from_start}s</span>
      <span class="log-desc text-base-content/70">${event.description}</span>
      <span class="log-code text-base-content/50">${event.system}/${event.subsystem}/${event.unit}/${event.code}</span>
      <button class="bookmark-toggle" title="Toggle bookmark">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6">
          <path stroke-linecap="round" stroke-linejoin="round" d="M6 4h12v16l-6-3-6 3z" />
        </svg>
      </button>
    `;
    row.querySelector(".bookmark-toggle").addEventListener("click", (eventClick) => {
      eventClick.stopPropagation();
      const next = LogApp.bookmarks?.cycle(event.row_id) || 0;
      row.classList.toggle("is-bookmarked", next > 0);
      row.dataset.bookmarkColor = String(next);
      renderBookmarks();
      if (bus) bus.emit("bookmarks:changed", LogApp.bookmarks?.getAllWithColors() || {});
    });
    row.addEventListener("click", () => {
      if (bus) bus.emit("event:selected", event);
      if (bus) bus.emit("log:jump", { rowId: event.row_id });
    });
    return row;
  };

  const resultsState = {
    items: [],
    rowStride: 28,
    overscan: 4,
    maxVisible: 80,
    lastRange: [0, 0],
  };

  const measureResultRow = () => {
    const sample = renderResultRow(events[0]);
    sample.style.visibility = "hidden";
    resultsItems.appendChild(sample);
    const rowHeight = sample.getBoundingClientRect().height || 28;
    const listStyle = getComputedStyle(resultsItems);
    const gap = parseFloat(listStyle.rowGap || listStyle.gap || "0") || 0;
    resultsItems.removeChild(sample);
    resultsState.rowStride = rowHeight + gap;
  };

  const setResultsSpacer = () => {
    resultsSpacer.style.height = `${resultsState.items.length * resultsState.rowStride}px`;
  };

  const renderResultsRange = (startIndex, endIndex) => {
    resultsItems.style.transform = `translateY(${startIndex * resultsState.rowStride}px)`;
    resultsItems.innerHTML = "";
    const fragment = document.createDocumentFragment();
    for (let i = startIndex; i < endIndex; i += 1) {
      fragment.appendChild(renderResultRow(resultsState.items[i]));
    }
    resultsItems.appendChild(fragment);
  };

  const updateResultsVirtual = () => {
    const scrollTop = resultsList.scrollTop;
    const startIndex = Math.max(0, Math.floor(scrollTop / resultsState.rowStride) - resultsState.overscan);
    const visibleCount = Math.min(
      resultsState.maxVisible,
      Math.ceil(resultsList.clientHeight / resultsState.rowStride) + resultsState.overscan * 2
    );
    const endIndex = Math.min(resultsState.items.length, startIndex + visibleCount);
    if (resultsState.lastRange[0] === startIndex && resultsState.lastRange[1] === endIndex) return;
    resultsState.lastRange = [startIndex, endIndex];
    renderResultsRange(startIndex, endIndex);
  };

  const renderResults = (items) => {
    resultsState.items = items;
    resultsState.lastRange = [0, 0];
    setResultsSpacer();
    if (!items.length) {
      resultsItems.style.transform = "translateY(0)";
      resultsItems.innerHTML = '<div class="no-results">No Results</div>';
      return;
    }
    updateResultsVirtual();
  };

  let pendingSearch = 0;
  let currentTab = "history";
  const runSearch = (commitHistory = false) => {
    const query = queryInput.value.trim();
    const isBookmarks = currentTab === "bookmarks";
    const source = isBookmarks ? getBookmarkEvents() : events;
    if (!query) {
      renderResults(source);
      if (commitHistory && !isBookmarks) addHistory(query, source.length, source[0]?.level);
      return;
    }
    if (LogApp.searchWorker && !isBookmarks) {
      const requestId = ++pendingSearch;
      LogApp.runSearchQuery(LogApp.searchWorker, query, (indices) => {
        if (requestId !== pendingSearch) return;
        const filtered = indices.map((idx) => events[idx]);
        renderResults(filtered);
        if (commitHistory) addHistory(query, filtered.length, filtered[0]?.level);
      });
      return;
    }
    const filtered = source.filter(LogApp.getQueryPredicate(query));
    renderResults(filtered);
    if (commitHistory && !isBookmarks) addHistory(query, filtered.length, filtered[0]?.level);
  };

  runButton.addEventListener("click", () => runSearch(true));
  queryInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") runSearch(true);
  });
  queryInput.addEventListener("input", () => {
    resultsItems.innerHTML = "";
    resultsSpacer.style.height = "0";
    resultsState.items = [];
    resultsState.lastRange = [0, 0];
  });
  // Live search disabled; only Search button or Enter triggers.

  clearHistoryButton.addEventListener("click", clearHistory);

  const setTab = (tab) => {
    const isHistory = tab === "history";
    const isFilters = tab === "filters";
    const isBookmarks = tab === "bookmarks";
    currentTab = tab;
    tabHistory.classList.toggle("is-active", isHistory);
    tabFilters.classList.toggle("is-active", isFilters);
    tabBookmarks.classList.toggle("is-active", isBookmarks);
    historyView.classList.toggle("hidden", !isHistory);
    filterView.classList.toggle("hidden", !isFilters);
    bookmarkView.classList.toggle("hidden", !isBookmarks);
    if (searchSplit) searchSplit.classList.toggle("search-single", isBookmarks);
    runSearch(false);
  };

  tabHistory.addEventListener("click", () => setTab("history"));
  tabFilters.addEventListener("click", () => setTab("filters"));
  tabBookmarks.addEventListener("click", () => setTab("bookmarks"));

  renderPinned();
  renderHistory();
  renderFilters();
  renderBookmarks();
  if (events.length) {
    measureResultRow();
  }
  renderResults(events.slice(0, 200));
  applyFilters();

  if (bus) {
    bus.on("bookmarks:changed", renderBookmarks);
    bus.on("bookmarks:changed", (map) => {
      if (currentTab === "bookmarks") {
        resultsState.lastRange = [0, 0];
        runSearch(false);
        return;
      }
      const rows = resultsList.querySelectorAll(".log-line");
      rows.forEach((row) => {
        const rowId = row.dataset.rowId;
        const colorIndex =
          (map && rowId != null ? Number(map[String(rowId)]) : null) ??
          LogApp.bookmarks?.getColor(rowId) ??
          0;
        row.classList.toggle("is-bookmarked", colorIndex > 0);
        row.dataset.bookmarkColor = String(colorIndex || 0);
      });
    });
  }

  resultsList.addEventListener("scroll", () => {
    requestAnimationFrame(updateResultsVirtual);
  });
};
