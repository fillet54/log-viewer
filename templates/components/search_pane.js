window.LogApp = window.LogApp || {};

LogApp.initSearchPane = (logData, bus) => {
  const pinnedList = document.getElementById("search-pinned");
  const historyList = document.getElementById("search-history");
  const filtersList = document.getElementById("search-filters");
  const resultsList = document.getElementById("search-results");
  const queryInput = document.getElementById("search-query");
  const runButton = document.getElementById("run-search");
  const clearHistoryButton = document.getElementById("clear-history");
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
    !clearHistoryButton ||
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

  const applyFilters = () => {
    const active = filters.filter((item) => item.enabled).map((item) => item.query);
    if (bus) bus.emit("filters:apply", active);
  };

  const MAX_RESULTS = 200;

  const renderResults = (items) => {
    resultsList.innerHTML = "";
    const fragment = document.createDocumentFragment();
    items.slice(0, MAX_RESULTS).forEach((event) => {
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

  let pendingSearch = 0;
  const runSearch = (commitHistory = false) => {
    const query = queryInput.value.trim();
    if (!query) {
      renderResults(events);
      if (commitHistory) addHistory(query, events.length, events[0]?.level);
      return;
    }
    if (LogApp.searchWorker) {
      const requestId = ++pendingSearch;
      LogApp.runSearchQuery(LogApp.searchWorker, query, (indices) => {
        if (requestId !== pendingSearch) return;
        const filtered = indices.map((idx) => events[idx]);
        renderResults(filtered);
        if (commitHistory) addHistory(query, filtered.length, filtered[0]?.level);
      });
      return;
    }
    const filtered = events.filter(LogApp.getQueryPredicate(query));
    renderResults(filtered);
    if (commitHistory) addHistory(query, filtered.length, filtered[0]?.level);
  };

  runButton.addEventListener("click", () => runSearch(true));
  queryInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") runSearch(true);
  });
  let inputDebounce = null;
  queryInput.addEventListener("input", () => {
    if (inputDebounce) window.clearTimeout(inputDebounce);
    inputDebounce = window.setTimeout(() => runSearch(false), 250);
  });

  clearHistoryButton.addEventListener("click", clearHistory);

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
