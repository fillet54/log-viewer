class LogMainViewElement extends LogAppComponentElement {
  constructor() {
    super();
    this.state = null;
    this.pendingFilter = 0;
    this.resizeObserver = null;
  }

  captureRowTemplate() {
    if (this._rowTemplate) return this._rowTemplate;
    const template = this.querySelector('template[data-role="row-template"]');
    this._rowTemplate = template ? template.cloneNode(true) : null;
    return this._rowTemplate;
  }

  getRowTemplate() {
    return this.captureRowTemplate();
  }

  getServices() {
    return {
      bus: this.getBus(),
      logData: this.getLogData(),
      plugin: this.getPlugin(),
      view: this.getView(),
      rowTemplate: this.getRowTemplate(),
      searchWorker: this.getSearchWorker(),
      bookmarks: this.getBookmarks(),
      comments: this.getComments(),
    };
  }

  renderMountError(message) {
    this.innerHTML = `<div class="empty-panel-message">${String(message || "Unable to load main view.")}</div>`;
  }

  mountWithPreact() {
    const ui = window.EventLog2UI || {};
    const Component = ui.components?.LogMainViewShell || null;

    if (!ui.available) {
      this.renderMountError("Local Preact runtime is required for the main view.");
      return false;
    }

    if (typeof ui.createMountController !== "function" || typeof Component !== "function") {
      this.renderMountError("Main view component is not registered.");
      return false;
    }

    if (!this._mountController) {
      this._mountController = ui.createMountController({
        host: this,
        Component,
        getProps: () => ({
          shellVersion: this._shellVersion || 0,
        }),
        getServices: () => this.getServices(),
        onError: (error) => {
          console.error(error);
          this.renderMountError(error?.message || "Unable to load main view.");
        },
      });
    }

    return this._mountController.render();
  }

  buildLogRow(event) {
    const { bus, bookmarks, rowTemplate, view } = this.getServices();
    const row = this.buildRow(event, rowTemplate, { bookmarks, view });
    if (!row) return null;

    const bookmarkButton = row.querySelector(".bookmark-toggle");
    if (bookmarkButton) {
      bookmarkButton.addEventListener("click", (eventClick) => {
        eventClick.stopPropagation();
        const next = bookmarks?.cycle(event.row_id) || 0;
        row.classList.toggle("is-bookmarked", next > 0);
        row.dataset.bookmarkColor = String(next);
        if (bus) bus.emit("bookmarks:changed", bookmarks?.getAllWithColors() || {});
      });
    }

    row.querySelectorAll(".match-link").forEach((button) => {
      button.addEventListener("click", (eventClick) => {
        eventClick.stopPropagation();
        const linkedRowId = button.dataset.linkedRowId;
        if (!linkedRowId || !bus || !this.state?.eventByRowId) return;
        const linkedEvent = this.state.eventByRowId.get(String(linkedRowId)) || null;
        if (linkedEvent) bus.emit("event:selected", linkedEvent);
        bus.emit("log:jump", { rowId: linkedRowId });
      });
    });

    row.addEventListener("click", () => {
      if (bus) bus.emit("event:selected", event);
    });
    return row;
  }

  rebuildIndex() {
    this.state.indexByRowId.clear();
    this.state.filtered.forEach((event, idx) => {
      this.state.indexByRowId.set(String(event.row_id), idx);
    });
  }

  setSpacer() {
    const logSpacer = this.queryById("log-spacer");
    if (!logSpacer || !this.state) return;
    logSpacer.style.height = `${this.state.filtered.length * this.state.rowStride}px`;
  }

  renderRange(startIndex, endIndex) {
    const logList = this.queryById("log-list");
    if (!logList || !this.state) return;
    logList.style.transform = `translateY(${startIndex * this.state.rowStride}px)`;
    logList.innerHTML = "";
    const fragment = document.createDocumentFragment();
    for (let i = startIndex; i < endIndex; i += 1) {
      const row = this.buildLogRow(this.state.filtered[i]);
      if (!row) continue;
      if (this.state.selectedRowId && row.dataset.rowId === String(this.state.selectedRowId)) {
        row.classList.add("log-selected");
      }
      fragment.appendChild(row);
    }
    logList.appendChild(fragment);
  }

  updateVirtual() {
    const logBody = this.queryById("log-body");
    if (!logBody || !this.state) return;
    const startIndex = Math.max(0, Math.floor(logBody.scrollTop / this.state.rowStride) - this.state.overscan);
    const visibleCount = Math.min(
      this.state.maxVisible,
      Math.ceil(logBody.clientHeight / this.state.rowStride) + this.state.overscan * 2
    );
    const endIndex = Math.min(this.state.filtered.length, startIndex + visibleCount);
    if (this.state.lastRange[0] === startIndex && this.state.lastRange[1] === endIndex) return;
    this.state.lastRange = [startIndex, endIndex];
    this.renderRange(startIndex, endIndex);
  }

  applyFilterQueries(queries) {
    const { bus, searchWorker } = this.getServices();
    const terms = queries.map((q) => q.trim()).filter(Boolean);
    if (!terms.length) {
      this.state.filtered = this.state.events;
      this.rebuildIndex();
      this.setSpacer();
      this.state.lastRange = [0, 0];
      this.updateVirtual();
      if (bus) bus.emit("log:filtered", this.state.filtered);
      return;
    }

    const query = terms.join(" OR ");
    if (searchWorker) {
      const requestId = ++this.pendingFilter;
      LogSearch.runQuery(searchWorker, query, (indices) => {
        if (requestId !== this.pendingFilter) return;
        this.state.filtered = indices.map((idx) => this.state.events[idx]);
        this.rebuildIndex();
        this.setSpacer();
        this.state.lastRange = [0, 0];
        this.updateVirtual();
        if (bus) bus.emit("log:filtered", this.state.filtered);
      });
      return;
    }

    const predicates = terms.map((term) => LogSearch.getQueryPredicate(term));
    this.state.filtered = this.state.events.filter((event) =>
      predicates.some((predicate) => predicate(event))
    );
    this.rebuildIndex();
    this.setSpacer();
    this.state.lastRange = [0, 0];
    this.updateVirtual();
    if (bus) bus.emit("log:filtered", this.state.filtered);
  }

  scrollToIndex(index, duration = 180) {
    const logBody = this.queryById("log-body");
    const logList = this.queryById("log-list");
    const { bus } = this.getServices();
    if (index == null || !logBody || !logList || !this.state) return null;
    const targetTop = index * this.state.rowStride - logBody.clientHeight / 2 + this.state.rowStride / 2;
    const clamped = Math.max(0, Math.min(targetTop, logBody.scrollHeight));
    const selected = this.state.filtered[index];
    smoothScrollTo(logBody, clamped, duration, () => {
      if (!selected) return;
      const row = logList.querySelector(`[data-row-id="${selected.row_id}"]`);
      if (!row) return;
      row.classList.remove("log-highlight");
      void row.offsetWidth;
      row.classList.add("log-highlight");
    });
    if (bus && selected) bus.emit("event:selected", selected);
    return selected;
  }

  findClosestIndexBySeconds(targetSeconds) {
    const list = this.state?.filtered || [];
    if (!list.length) return null;
    let lo = 0;
    let hi = list.length - 1;
    while (lo <= hi) {
      const mid = Math.floor((lo + hi) / 2);
      const seconds = list[mid].norm_time;
      if (seconds === targetSeconds) return mid;
      if (seconds < targetSeconds) lo = mid + 1;
      else hi = mid - 1;
    }
    if (lo >= list.length) return list.length - 1;
    if (hi < 0) return 0;
    return Math.abs(list[lo].norm_time - targetSeconds) < Math.abs(list[hi].norm_time - targetSeconds)
      ? lo
      : hi;
  }

  ensureRowVisible(rowId) {
    if (!this.state.indexByRowId.has(String(rowId))) {
      this.applyFilterQueries([]);
    }
    return this.scrollToIndex(this.state.indexByRowId.get(String(rowId)));
  }

  initializeResizeHandling() {
    if (this.resizeObserver || typeof ResizeObserver !== "function") return;
    const logBody = this.queryById("log-body");
    const schedule = () => {
      requestAnimationFrame(() => {
        if (!this.state) return;
        this.state.lastRange = [0, 0];
        this.updateVirtual();
      });
    };

    this.resizeObserver = new ResizeObserver(() => schedule());
    this.resizeObserver.observe(this);
    if (logBody) this.resizeObserver.observe(logBody);
  }

  initializeMainView() {
    if (this.state) return;

    const { logData, bus, bookmarks, comments } = this.getServices();
    const logBody = this.queryById("log-body");
    const logList = this.queryById("log-list");
    const logSpacer = this.queryById("log-spacer");
    const rowTemplate = this.getRowTemplate();

    if (!logBody || !logList || !logSpacer || !logData || !rowTemplate) return;

    const events = Array.isArray(logData.events) ? logData.events : [];
    if (!events.length) return;

    const listStyle = getComputedStyle(logList);
    const sample = this.buildLogRow(events[0]);
    let rowHeight = 38;
    if (sample) {
      sample.style.visibility = "hidden";
      logList.appendChild(sample);
      rowHeight = sample.getBoundingClientRect().height || rowHeight;
      logList.removeChild(sample);
    }

    this.state = {
      events,
      filtered: events,
      eventByRowId: new Map(events.map((event) => [String(event.row_id), event])),
      rowStride: rowHeight + (parseFloat(listStyle.rowGap || listStyle.gap || "0") || 0),
      overscan: 10,
      maxVisible: 180,
      lastRange: [0, 0],
      indexByRowId: new Map(),
      selectedRowId: null,
    };

    let scrollRaf = 0;
    logBody.addEventListener("scroll", () => {
      if (scrollRaf) return;
      scrollRaf = requestAnimationFrame(() => {
        scrollRaf = 0;
        this.updateVirtual();
        const index = Math.max(
          0,
          Math.min(
            this.state.filtered.length - 1,
            Math.floor((logBody.scrollTop + logBody.clientHeight / 2) / this.state.rowStride)
          )
        );
        const current = this.state.filtered[index];
        if (bus && current) {
          bus.emit("log:scroll", { seconds: current.norm_time, rowId: current.row_id });
        }
      });
    });

    this.rebuildIndex();
    this.setSpacer();
    this.updateVirtual();
    if (bus) bus.emit("log:filtered", this.state.filtered);

    if (bus) {
      bus.on("filters:apply", (queries) => this.applyFilterQueries(queries || []));
      bus.on("log:jump", (payload) => {
        if (!payload) return;
        if (payload.rowId != null) this.ensureRowVisible(payload.rowId);
        if (payload.seconds != null) this.scrollToIndex(this.findClosestIndexBySeconds(payload.seconds));
      });
      bus.on("event:selected", (event) => {
        this.state.selectedRowId = event?.row_id ?? null;
        logList.querySelectorAll(".log-line").forEach((row) => {
          row.classList.toggle("log-selected", row.dataset.rowId === String(this.state.selectedRowId));
        });
      });
      bus.on("bookmarks:changed", (map) => {
        logList.querySelectorAll(".log-line").forEach((row) => {
          const rowId = row.dataset.rowId;
          const colorIndex =
            (map && rowId != null ? Number(map[String(rowId)]) : null) ?? bookmarks?.getColor(rowId) ?? 0;
          row.classList.toggle("is-bookmarked", colorIndex > 0);
          row.dataset.bookmarkColor = String(colorIndex || 0);
        });
      });
    }

    this.initializeResizeHandling();
  }

  connectedCallback() {
    this.connectToApp(() => {
      this.captureRowTemplate();
      if (!this.mountWithPreact()) return;
      this.initializeMainView();
    });
  }

  disconnectedCallback() {
    this._mountController?.destroy?.();
    this.resizeObserver?.disconnect?.();
    this.resizeObserver = null;
  }
}

if (!customElements.get("log-main-view")) {
  customElements.define("log-main-view", LogMainViewElement);
}
