class LogSearchPanelElement extends LogAppComponentElement {
  constructor() {
    super();
    this.currentTab = "history";
    this.pendingSearch = 0;
    this.resultsState = {
      items: [],
      rowStride: 28,
      overscan: 4,
      maxVisible: 80,
      lastRange: [0, 0],
    };
    this.pinIcon = `
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7">
        <path stroke-linecap="round" stroke-linejoin="round" d="M9 3h6l-1 6 3 3-1.5 1.5L12 10l-3.5 3.5L7 12l3-3-1-6Z" />
        <path stroke-linecap="round" stroke-linejoin="round" d="M12 10v9" />
      </svg>
    `;
    this.filterIcon = `
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7">
        <path stroke-linecap="round" stroke-linejoin="round" d="M4 5h16l-6 7v6l-4 2v-8L4 5Z" />
      </svg>
    `;
    this.removeIcon = `
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7">
        <path stroke-linecap="round" stroke-linejoin="round" d="M6 6l12 12M18 6l-12 12" />
      </svg>
    `;
    this.searchSplitInstance = null;
  }

  getById(id) {
    return this.queryById(id);
  }

  loadStored(key, fallback) {
    try {
      const raw = localStorage.getItem(key);
      if (!raw) return fallback;
      const parsed = JSON.parse(raw);
      return Array.isArray(parsed) ? parsed : fallback;
    } catch (err) {
      return fallback;
    }
  }

  escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  getSearchFieldPaths() {
    const source = Array.isArray(this.events)
      ? this.events
      : Array.isArray(this.getLogData()?.events)
        ? this.getLogData().events
        : [];
    const sample = source.find((event) => event && typeof event === "object") || null;
    if (!sample) return [];
    return Object.entries(sample)
      .map(([key, value]) => ({
        name: key,
        nested:
          value != null &&
          typeof value === "object" &&
          (!Array.isArray(value) ? Object.keys(value).length > 0 : value.length > 0),
      }))
      .sort((a, b) => a.name.localeCompare(b.name));
  }

  buildSearchHelpDialog() {
    const fields = this.getSearchFieldPaths();
    const examples = [
      'name:temp_core',
      'system:Power',
      'color:Red',
      'data.bus.load_pct>=68',
      'description~timeout',
      'system:Power OR system:Thermal',
      'NOT color:Green',
      '$:sensor',
      '$.*:writer',
    ];

    return `
      <dialog id="search-help-dialog" class="search-help-dialog">
        <form method="dialog" class="search-help-card">
          <div class="search-help-header">
            <div>
              <div class="section-label">Search Help</div>
              <div class="search-help-title">Query Syntax</div>
            </div>
            <button class="button button-ghost button-xs" value="close" aria-label="Close search help">Close</button>
          </div>
          <div class="search-help-body">
            <div class="search-help-section">
              <div class="search-help-section-title">Basics</div>
              <div class="support-text">Bare terms search across the main event fields and also prefix-match the event <code>name</code>. For example, typing <code>temp</code> will match names that start with <code>temp</code> and exact matching values in common fields.</div>
              <div class="support-text">Use <code>field:value</code> for exact field matching and <code>field~text</code> for substring matching. Numeric fields support <code>&gt;</code>, <code>&gt;=</code>, <code>&lt;</code>, and <code>&lt;=</code>.</div>
              <div class="support-text">Boolean logic is supported with <code>AND</code>, <code>OR</code>, and <code>NOT</code>. If you omit an operator between filters, it behaves like an <code>AND</code>.</div>
              <div class="support-text">Use <code>*</code> as a wildcard for text matching. Use <code>$:key</code> to search object key names and <code>$.*:value</code> to search deeply across nested object values.</div>
            </div>
            <div class="search-help-section">
              <div class="search-help-section-title">Examples</div>
              <div class="search-help-examples">
                ${examples.map((example) => `<button type="button" class="search-help-example" data-search-example="${this.escapeHtml(example)}">${this.escapeHtml(example)}</button>`).join("")}
              </div>
            </div>
            <div class="search-help-section">
              <div class="search-help-section-title">Searchable Columns</div>
              <div class="support-text">These are top-level event fields. Fields marked <code>map</code> contain nested object data and can still be queried with dotted paths like <code>data.bus.load_pct</code> when needed.</div>
              <div class="search-help-fields">
                ${fields
                  .map(
                    (field) => `<span class="search-help-field"><code>${this.escapeHtml(field.name)}</code>${field.nested ? '<span class="search-help-field-badge">map</span>' : ""}</span>`
                  )
                  .join("")}
              </div>
            </div>
          </div>
        </form>
      </dialog>
    `;
  }

  renderThread(threads, depth = 0) {
    if (!threads.length) return "";
    return `
      <div class="comment-thread">
        ${threads
          .map(
            (comment) => `
            <div class="comment-item" style="margin-left:${depth * 16}px">
              <div class="comment-meta">
                <span class="comment-time">${this.escapeHtml(comment.created_at)}</span>
              </div>
              <div class="comment-body">${this.escapeHtml(comment.body)}</div>
              ${comment.replies?.length ? this.renderThread(comment.replies, depth + 1) : ""}
            </div>
          `
          )
          .join("")}
      </div>
    `;
  }

  initialize() {
    const { logData, bus, bookmarks, comments, searchWorker, rowTemplate, view } = this.getServices();
    this.events = Array.isArray(logData?.events) ? logData.events : [];
    this.bookmarks = bookmarks;
    this.comments = comments;
    this.bus = bus;
    this.searchWorker = searchWorker;
    this.rowTemplate = rowTemplate;
    this.view = view;

    this.pinnedList = this.getById("search-pinned");
    this.historyList = this.getById("search-history");
    this.filtersList = this.getById("search-filters");
    this.bookmarksList = this.getById("search-bookmarks");
    this.resultsList = this.getById("search-results");
    this.resultsSpacer = this.getById("search-results-spacer");
    this.resultsItems = this.getById("search-results-list");
    this.queryInput = this.getById("search-query");
    this.runButton = this.getById("run-search");
    this.clearHistoryButton = this.getById("clear-history");
    this.tabHistory = this.getById("tab-history");
    this.tabFilters = this.getById("tab-filters");
    this.tabBookmarks = this.getById("tab-bookmarks");
    this.historyView = this.getById("search-history-view");
    this.filterView = this.getById("search-filter-view");
    this.bookmarkView = this.getById("search-bookmark-view");
    this.searchSplit = this.getById("search-split");
    this.activityEnabled = this.bookmarks?.enabled !== false || this.comments?.enabled !== false;
    this.helpDialog = this.getById("search-help-dialog");
    this.helpButton = this.getById("open-search-help");

    if (
      !this.pinnedList ||
      !this.historyList ||
      !this.filtersList ||
      !this.bookmarksList ||
      !this.resultsList ||
      !this.resultsSpacer ||
      !this.resultsItems ||
      !this.queryInput ||
      !this.runButton ||
      !this.clearHistoryButton ||
      !this.tabHistory ||
      !this.tabFilters ||
      !this.tabBookmarks ||
      !this.historyView ||
      !this.filterView ||
      !this.bookmarkView ||
      !logData ||
      !rowTemplate
    ) {
      return;
    }

    const keys = STORAGE_KEYS;
    this.history = this.loadStored(keys.searchHistory, []);
    this.pinned = this.loadStored(keys.searchPinned, []);
    this.filters = this.loadStored(keys.searchFilters, []);

    this.runButton.addEventListener("click", () => this.runSearch(true));
    if (this.helpButton && this.helpDialog) {
      this.helpButton.addEventListener("click", () => this.helpDialog.showModal());
      this.helpDialog.querySelectorAll("[data-search-example]").forEach((button) => {
        button.addEventListener("click", () => {
          this.queryInput.value = button.dataset.searchExample || "";
          this.helpDialog.close();
          this.queryInput.focus();
        });
      });
    }
    this.queryInput.addEventListener("keydown", (event) => {
      if (event.key === "Enter") this.runSearch(true);
    });
    this.queryInput.addEventListener("input", () => {
      this.resultsItems.innerHTML = "";
      this.resultsSpacer.style.height = "0";
      this.resultsState.items = [];
      this.resultsState.lastRange = [0, 0];
    });
    this.clearHistoryButton.addEventListener("click", () => this.clearHistory());
    this.tabHistory.addEventListener("click", () => this.setTab("history"));
    this.tabFilters.addEventListener("click", () => this.setTab("filters"));
    if (this.activityEnabled) {
      this.tabBookmarks.addEventListener("click", () => this.setTab("bookmarks"));
    } else {
      this.tabBookmarks.classList.add("hidden");
      this.bookmarkView.classList.add("hidden");
    }
    this.resultsList.addEventListener("scroll", () => {
      requestAnimationFrame(() => this.updateResultsVirtual());
    });

    this.renderPinned();
    this.renderHistory();
    this.renderFilters();
    if (this.activityEnabled) this.renderBookmarks();
    if (this.events.length) this.measureResultRow();
    this.renderResults(this.events.slice(0, 200));
    this.applyFilters();
    this.initializeSearchSplit();

    if (bus && this.activityEnabled) {
      bus.on("bookmarks:changed", () => this.renderBookmarks());
      bus.on("bookmarks:changed", (map) => {
        if (this.currentTab === "bookmarks") {
          this.resultsState.lastRange = [0, 0];
          this.runSearch(false);
          return;
        }
        this.resultsList.querySelectorAll(".log-line").forEach((row) => {
          const rowId = row.dataset.rowId;
          const colorIndex =
            (map && rowId != null ? Number(map[String(rowId)]) : null) ??
            this.bookmarks?.getColor(rowId) ??
            0;
          row.classList.toggle("is-bookmarked", colorIndex > 0);
          row.dataset.bookmarkColor = String(colorIndex || 0);
        });
      });
      bus.on("comments:changed", () => {
        this.renderBookmarks();
        if (this.currentTab === "bookmarks") {
          this.resultsState.lastRange = [0, 0];
          this.runSearch(false);
        }
      });
    }
  }

  initializeSearchSplit() {
    const left = this.getById("search-history-pane");
    const right = this.getById("search-results-pane");
    if (!this.searchSplit || !left || !right || this.searchSplitInstance || typeof Split !== "function") return;
    this.searchSplitInstance = Split([left, right], {
      sizes: loadSizes(STORAGE_KEYS.search, [28, 72]),
      minSize: [160, 320],
      gutterSize: 8,
      elementStyle: (dimension, size, gutterSizeValue) => ({
        "flex-basis": `calc(${size}% - ${gutterSizeValue}px)`,
      }),
      gutterStyle: (dimension, gutterSizeValue) => ({
        "flex-basis": `${gutterSizeValue}px`,
      }),
      onDragEnd: (sizes) => saveSizes(STORAGE_KEYS.search, sizes),
    });
  }

  persist() {
    localStorage.setItem(STORAGE_KEYS.searchHistory, JSON.stringify(this.history));
    localStorage.setItem(STORAGE_KEYS.searchPinned, JSON.stringify(this.pinned));
    localStorage.setItem(STORAGE_KEYS.searchFilters, JSON.stringify(this.filters));
  }

  renderList(items, container, isPinned) {
    container.innerHTML = "";
    const fragment = document.createDocumentFragment();
    items.forEach((item) => {
      const row = document.createElement("div");
      row.className = "search-item search-history-item";
      row.innerHTML = `
        <span class="search-query">${item.label}</span>
        <button class="pin-button ${isPinned ? "is-pinned" : ""}" title="${isPinned ? "Unpin" : "Pin"}">${this.pinIcon}</button>
        <button class="pin-button promote-button" title="Promote to filter">${this.filterIcon}</button>
        <span class="search-time">${item.count}</span>
      `;
      row.querySelector(".pin-button").addEventListener("click", (event) => {
        event.stopPropagation();
        this.togglePin(item.query);
      });
      row.querySelector(".promote-button").addEventListener("click", (event) => {
        event.stopPropagation();
        this.promoteFilter(item.query);
      });
      row.addEventListener("click", () => {
        this.queryInput.value = item.query;
        this.runSearch();
      });
      fragment.appendChild(row);
    });
    container.appendChild(fragment);
  }

  renderPinned() {
    this.renderList(this.pinned.slice(0, 24), this.pinnedList, true);
  }

  renderHistory() {
    this.renderList(this.history.slice(0, 50), this.historyList, false);
  }

  togglePin(query) {
    const index = this.pinned.findIndex((item) => item.query === query);
    if (index >= 0) {
      this.pinned.splice(index, 1);
    } else {
      const item = this.history.find((entry) => entry.query === query) || {
        query,
        count: 0,
        color: "search",
        label: query || "(all events)",
      };
      this.pinned.unshift(item);
    }
    this.persist();
    this.renderPinned();
  }

  promoteFilter(query) {
    if (!query) return;
    const existing = this.filters.find((item) => item.query === query);
    if (existing) existing.enabled = true;
    else this.filters.unshift({ query, enabled: true });
    this.persist();
    this.renderFilters();
    this.applyFilters();
  }

  addHistory(query, count, color) {
    if (query === "" && !count) return;
    const existingIndex = this.history.findIndex((item) => item.query === query);
    const item = { query, count, color: color || "search", label: query || "(all events)" };
    if (existingIndex >= 0) this.history.splice(existingIndex, 1);
    this.history.unshift(item);
    this.history.splice(50);
    this.persist();
    this.renderHistory();
  }

  clearHistory() {
    const pinnedQueries = new Set(this.pinned.map((item) => item.query));
    const filterQueries = new Set(this.filters.map((item) => item.query));
    const remaining = this.history.filter(
      (item) => pinnedQueries.has(item.query) || filterQueries.has(item.query)
    );
    this.history.splice(0, this.history.length, ...remaining);
    this.persist();
    this.renderHistory();
  }

  renderFilters() {
    this.filtersList.innerHTML = "";
    const fragment = document.createDocumentFragment();
    this.filters.forEach((filter) => {
      const row = document.createElement("div");
      row.className = "search-item search-filter-item";
      row.innerHTML = `
        <span>${filter.query}</span>
        <button class="filter-toggle ${filter.enabled ? "is-on" : ""}" aria-pressed="${filter.enabled}"></button>
        <button class="pin-button" title="Remove filter">${this.removeIcon}</button>
      `;
      row.querySelector(".filter-toggle").addEventListener("click", (event) => {
        event.stopPropagation();
        filter.enabled = !filter.enabled;
        this.persist();
        this.renderFilters();
        this.applyFilters();
      });
      row.querySelector(".pin-button").addEventListener("click", (event) => {
        event.stopPropagation();
        const index = this.filters.indexOf(filter);
        if (index >= 0) this.filters.splice(index, 1);
        this.persist();
        this.renderFilters();
        this.applyFilters();
      });
      fragment.appendChild(row);
    });
    this.filtersList.appendChild(fragment);
  }

  getBookmarkEvents() {
    if (!this.activityEnabled) return [];
    const bookmarkIds = new Set(this.bookmarks?.getAll() || []);
    const commentRows = this.comments?.getByRowId() || new Map();
    const ids = new Set([...bookmarkIds, ...Array.from(commentRows.keys())]);
    return Array.from(ids)
      .map((id) => this.events.find((entry) => String(entry.row_id) === String(id)))
      .filter(Boolean)
      .sort((a, b) => (a.norm_time || 0) - (b.norm_time || 0));
  }

  attachRowActions(row, event) {
    if (!row) return row;

    row.querySelector(".bookmark-toggle")?.addEventListener("click", (eventClick) => {
      if (!this.activityEnabled) return;
      eventClick.stopPropagation();
      const next = this.bookmarks?.cycle(event.row_id) || 0;
      row.classList.toggle("is-bookmarked", next > 0);
      row.dataset.bookmarkColor = String(next);
      this.renderBookmarks();
      if (this.bus) this.bus.emit("bookmarks:changed", this.bookmarks?.getAllWithColors() || {});
    });

    row.querySelectorAll(".match-link").forEach((button) => {
      button.addEventListener("click", (eventClick) => {
        eventClick.stopPropagation();
        const linkedRowId = button.dataset.linkedRowId;
        if (!linkedRowId || !this.bus) return;
        const linkedEvent = this.events.find((entry) => String(entry.row_id) === String(linkedRowId)) || null;
        if (linkedEvent) this.bus.emit("event:selected", linkedEvent);
        this.bus.emit("log:jump", { rowId: linkedRowId });
      });
    });

    row.addEventListener("click", () => {
      if (this.bus) this.bus.emit("event:selected", event);
      if (this.bus) this.bus.emit("log:jump", { rowId: event.row_id });
    });

    return row;
  }

  renderBookmarks() {
    if (!this.activityEnabled) {
      this.bookmarksList.innerHTML = "";
      return;
    }
    this.bookmarksList.innerHTML = "";
    const fragment = document.createDocumentFragment();
    this.getBookmarkEvents().forEach((event) => {
      const wrapper = document.createElement("div");
      wrapper.className = "activity-item";
      const row = this.buildRow(event, this.rowTemplate, {
        extraClasses: ["search-result-row"],
        bookmarks: this.bookmarks,
        view: this.view,
      });
      if (!row) return;
      wrapper.appendChild(this.attachRowActions(row, event));
      const threads = this.comments?.buildThreads(event.row_id) || [];
      if (threads.length) {
        const threadContainer = document.createElement("div");
        threadContainer.className = "activity-thread";
        threadContainer.innerHTML = this.renderThread(threads);
        wrapper.appendChild(threadContainer);
      }
      fragment.appendChild(wrapper);
    });
    this.bookmarksList.appendChild(fragment);
  }

  renderResultRow(event) {
    const row = this.buildRow(event, this.rowTemplate, {
      extraClasses: ["search-result-row"],
      bookmarks: this.bookmarks,
      view: this.view,
    });
    if (!row) return null;
    return this.attachRowActions(row, event);
  }

  renderBookmarkResults(items) {
    this.resultsItems.style.position = "relative";
    this.resultsItems.style.transform = "none";
    this.resultsItems.innerHTML = "";
    this.resultsSpacer.style.height = "0";
    if (!items.length) {
      this.resultsItems.innerHTML = '<div class="no-results">No Results</div>';
      return;
    }
    const fragment = document.createDocumentFragment();
    items.forEach((event) => {
      const wrapper = document.createElement("div");
      wrapper.className = "activity-item";
      const row = this.renderResultRow(event);
      if (!row) return;
      wrapper.appendChild(row);
      const threads = this.comments?.buildThreads(event.row_id) || [];
      if (threads.length) {
        const threadContainer = document.createElement("div");
        threadContainer.className = "activity-thread";
        threadContainer.innerHTML = this.renderThread(threads);
        wrapper.appendChild(threadContainer);
      }
      fragment.appendChild(wrapper);
    });
    this.resultsItems.appendChild(fragment);
  }

  measureResultRow() {
    const sample = this.renderResultRow(this.events[0]);
    if (!sample) return;
    sample.style.visibility = "hidden";
    this.resultsItems.appendChild(sample);
    const rowHeight = sample.getBoundingClientRect().height || 28;
    const listStyle = getComputedStyle(this.resultsItems);
    const gap = parseFloat(listStyle.rowGap || listStyle.gap || "0") || 0;
    this.resultsItems.removeChild(sample);
    this.resultsState.rowStride = rowHeight + gap;
  }

  setResultsSpacer() {
    this.resultsSpacer.style.height = `${this.resultsState.items.length * this.resultsState.rowStride}px`;
  }

  renderResultsRange(startIndex, endIndex) {
    this.resultsItems.style.transform = `translateY(${startIndex * this.resultsState.rowStride}px)`;
    this.resultsItems.innerHTML = "";
    const fragment = document.createDocumentFragment();
    for (let i = startIndex; i < endIndex; i += 1) {
      const row = this.renderResultRow(this.resultsState.items[i]);
      if (row) fragment.appendChild(row);
    }
    this.resultsItems.appendChild(fragment);
  }

  updateResultsVirtual() {
    if (this.currentTab === "bookmarks") return;
    const scrollTop = this.resultsList.scrollTop;
    const startIndex = Math.max(0, Math.floor(scrollTop / this.resultsState.rowStride) - this.resultsState.overscan);
    const visibleCount = Math.min(
      this.resultsState.maxVisible,
      Math.ceil(this.resultsList.clientHeight / this.resultsState.rowStride) + this.resultsState.overscan * 2
    );
    const endIndex = Math.min(this.resultsState.items.length, startIndex + visibleCount);
    if (this.resultsState.lastRange[0] === startIndex && this.resultsState.lastRange[1] === endIndex) return;
    this.resultsState.lastRange = [startIndex, endIndex];
    this.renderResultsRange(startIndex, endIndex);
  }

  renderResults(items) {
    if (this.currentTab === "bookmarks") {
      this.renderBookmarkResults(items);
      return;
    }
    this.resultsItems.style.position = "";
    this.resultsItems.style.transform = "";
    this.resultsState.items = items;
    this.resultsState.lastRange = [0, 0];
    this.setResultsSpacer();
    if (!items.length) {
      this.resultsItems.style.transform = "translateY(0)";
      this.resultsItems.innerHTML = '<div class="no-results">No Results</div>';
      return;
    }
    this.updateResultsVirtual();
  }

  applyFilters() {
    if (this.bus) {
      this.bus.emit(
        "filters:apply",
        this.filters.filter((item) => item.enabled).map((item) => item.query)
      );
    }
  }

  runSearch(commitHistory = false) {
    const query = this.queryInput.value.trim();
    const isBookmarks = this.currentTab === "bookmarks";
    const source = isBookmarks ? this.getBookmarkEvents() : this.events;
    if (!query) {
      this.renderResults(source);
      if (commitHistory && !isBookmarks) this.addHistory(query, source.length, source[0]?.color);
      return;
    }
    if (this.searchWorker && !isBookmarks) {
      const requestId = ++this.pendingSearch;
      LogSearch.runQuery(this.searchWorker, query, (indices) => {
        if (requestId !== this.pendingSearch) return;
        const filtered = indices.map((idx) => this.events[idx]);
        this.renderResults(filtered);
        if (commitHistory) this.addHistory(query, filtered.length, filtered[0]?.color);
      });
      return;
    }
    const filtered = source.filter(LogSearch.getQueryPredicate(query));
    this.renderResults(filtered);
    if (commitHistory && !isBookmarks) this.addHistory(query, filtered.length, filtered[0]?.color);
  }

  setTab(tab) {
    if (tab === "bookmarks" && !this.activityEnabled) tab = "history";
    const isHistory = tab === "history";
    const isFilters = tab === "filters";
    const isBookmarks = tab === "bookmarks";
    this.currentTab = tab;
    this.tabHistory.classList.toggle("is-active", isHistory);
    this.tabFilters.classList.toggle("is-active", isFilters);
    this.tabBookmarks.classList.toggle("is-active", isBookmarks);
    this.historyView.classList.toggle("hidden", !isHistory);
    this.filterView.classList.toggle("hidden", !isFilters);
    this.bookmarkView.classList.toggle("hidden", !isBookmarks);
    if (this.searchSplit) this.searchSplit.classList.toggle("search-single", isBookmarks);
    this.runSearch(false);
  }
  getRowTemplate() {
    return this.querySelector('template[data-role="row-template"]');
  }

  renderShell() {
    const rowTemplate = this.getRowTemplate();
    this.innerHTML = `
      <div class="pane-header compact-header pane-header-spread">
        <span id="search-header-text" class="section-label">Search</span>
        <div id="search-header-tabs" class="search-tabs hidden">
          <button id="tab-history" class="button button-ghost button-xs search-tab is-active">Search History</button>
          <button id="tab-filters" class="button button-ghost button-xs search-tab">Filters</button>
          <button id="tab-bookmarks" class="button button-ghost button-xs search-tab">Bookmarks</button>
        </div>
        <button id="toggle-bottom" class="button button-ghost button-xs" title="Toggle search pane">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="tool-icon">
            <path stroke-linecap="round" stroke-linejoin="round" d="M6 10l6 6 6-6" />
          </svg>
        </button>
      </div>
      <div class="pane-body search-pane">
        <div id="search-split" class="search-split">
          <aside id="search-history-pane" class="search-history">
            <div id="search-history-view" class="search-view">
              <div class="search-section">
                <div class="search-section-title">Pinned</div>
                <div id="search-pinned" class="search-list search-list-compact"></div>
              </div>
              <div class="search-section">
                <div class="search-section-title history-header">
                  <span>History</span>
                  <button id="clear-history" class="button button-ghost button-xs">Clear</button>
                </div>
                <div id="search-history" class="search-list search-list-compact"></div>
              </div>
            </div>
            <div id="search-filter-view" class="search-view hidden">
              <div class="search-section">
                <div class="search-section-title">Filters</div>
                <div id="search-filters" class="search-list search-list-compact"></div>
              </div>
            </div>
            <div id="search-bookmark-view" class="search-view hidden">
              <div class="search-section">
                <div class="search-section-title">Bookmarks</div>
                <div id="search-bookmarks" class="search-list search-list-compact"></div>
              </div>
            </div>
          </aside>
          <section id="search-results-pane" class="search-results">
            <div class="search-controls">
              <input id="search-query" class="text-input text-input-small search-input" placeholder="Search logs, faults, codes..." />
              <button type="button" id="open-search-help" class="button button-ghost button-small search-help-button" aria-label="Search syntax help" title="Search syntax help">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true">
                  <path stroke-linecap="round" stroke-linejoin="round" d="M9.5 9a2.5 2.5 0 1 1 4.2 1.8c-.8.6-1.2 1-1.2 2.2" />
                  <path stroke-linecap="round" stroke-linejoin="round" d="M12 17h.01" />
                  <circle cx="12" cy="12" r="9" />
                </svg>
              </button>
              <button id="run-search" class="button button-primary button-small">Search</button>
            </div>
            <div id="search-results" class="search-list search-results-list">
              <div id="search-results-spacer"></div>
              <div id="search-results-list" class="mono-block"></div>
            </div>
          </section>
        </div>
      </div>
      ${this.buildSearchHelpDialog()}
    `;
    if (rowTemplate) this.appendChild(rowTemplate);
  }

  getServices() {
    return {
      bus: this.getBus(),
      logData: this.getLogData(),
      view: this.getView(),
      rowTemplate: this.getRowTemplate(),
      searchWorker: this.getSearchWorker(),
      bookmarks: this.getBookmarks(),
      comments: this.getComments(),
    };
  }

  connectedCallback() {
    this.connectToApp(() => {
      if (!this.querySelector("#search-results")) {
        this.renderShell();
      }
      this.initialize();
      this.dispatchEvent(new CustomEvent("searchpanel:ready", { bubbles: true, composed: true }));
    });
  }
}

if (!customElements.get("log-search-panel")) {
  customElements.define("log-search-panel", LogSearchPanelElement);
}
