window.LogApp = window.LogApp || {};

LogApp.initLogList = (logData, bus) => {
  const logBody = document.getElementById("log-body");
  const logList = document.getElementById("log-list");
  const logSpacer = document.getElementById("log-spacer");
  const searchInput = document.getElementById("log-search");

  if (!logBody || !logList || !logSpacer || !logData) return null;
  const events = Array.isArray(logData.events) ? logData.events : [];
  if (!events.length) return null;

  const buildLogRow = (event) => {
    const row = document.createElement("div");
    const colorLabel = event.color || "Green";
    const colorClass = String(colorLabel).toLowerCase().replace(/\s+/g, "-");
    const colorIndex = LogApp.bookmarks?.getColor(event.row_id) || 0;
    row.className = `log-line log-${colorClass}${
      colorIndex ? " is-bookmarked" : ""
    }`;
    row.dataset.bookmarkColor = String(colorIndex);
    row.dataset.seconds = event.norm_time;
    row.dataset.rowId = event.row_id;
    const channels = new Set(event.channels || []);
    row.innerHTML = `
      <span class="badge badge-sm level-tag">${colorLabel}</span>
      <span class="log-time text-base-content/60">${event.utctime}</span>
      <span class="log-action font-semibold">${event.set_clear}</span>
      <span class="log-name">${event.name}</span>
      <span class="log-offset text-base-content/60">${event.norm_time}s</span>
      <span class="log-channels">
        <span class="log-channel ${channels.has("A") ? "is-on" : ""}">A</span>
        <span class="log-channel ${channels.has("B") ? "is-on" : ""}">B</span>
        <span class="log-channel ${channels.has("C") ? "is-on" : ""}">C</span>
        <span class="log-channel ${channels.has("D") ? "is-on" : ""}">D</span>
      </span>
      <span class="log-desc text-base-content/70">${event.description}</span>
      <span class="log-code text-base-content/50">${event.system}/${event.subsystem}/${event.unit}/${event.code}</span>
      <button class="bookmark-toggle" title="Toggle bookmark">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6">
          <path stroke-linecap="round" stroke-linejoin="round" d="M6 4h12v16l-6-3-6 3z" />
        </svg>
      </button>
    `;
    const bookmarkButton = row.querySelector(".bookmark-toggle");
    bookmarkButton.addEventListener("click", (eventClick) => {
      eventClick.stopPropagation();
      const next = LogApp.bookmarks?.cycle(event.row_id) || 0;
      row.classList.toggle("is-bookmarked", next > 0);
      row.dataset.bookmarkColor = String(next);
      if (bus) bus.emit("bookmarks:changed", LogApp.bookmarks?.getAllWithColors() || {});
    });
    row.addEventListener("click", () => {
      if (bus) bus.emit("event:selected", event);
    });
    return row;
  };

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
    overscan: 4,
    maxVisible: 120,
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
    const visibleCount = Math.min(
      state.maxVisible,
      Math.ceil(logBody.clientHeight / state.rowStride) + state.overscan * 2
    );
    const endIndex = Math.min(state.filtered.length, startIndex + visibleCount);
    if (state.lastRange[0] === startIndex && state.lastRange[1] === endIndex) return;
    state.lastRange = [startIndex, endIndex];
    renderRange(startIndex, endIndex);
  };

  let pendingFilter = 0;
  const applyFilterQueries = (queries) => {
    const terms = queries.map((q) => q.trim()).filter(Boolean);
    state.filterQuery = terms.join(" | ");
    if (!terms.length) {
      state.filtered = state.events;
      rebuildIndex();
      setSpacer();
      state.lastRange = [0, 0];
      updateVirtual();
      if (bus) bus.emit("log:filtered", state.filtered);
      return;
    }

    const query = terms.join(" OR ");
    if (LogApp.searchWorker) {
      const requestId = ++pendingFilter;
      LogApp.runSearchQuery(LogApp.searchWorker, query, (indices) => {
        if (requestId !== pendingFilter) return;
        state.filtered = indices.map((idx) => state.events[idx]);
        rebuildIndex();
        setSpacer();
        state.lastRange = [0, 0];
        updateVirtual();
        if (bus) bus.emit("log:filtered", state.filtered);
      });
      return;
    }

    const predicates = terms.map((term) => LogApp.getQueryPredicate(term));
    state.filtered = state.events.filter((event) =>
      predicates.some((predicate) => predicate(event))
    );
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
      const seconds = list[mid].norm_time;
      if (seconds === targetSeconds) return mid;
      if (seconds < targetSeconds) {
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    if (lo >= list.length) return list.length - 1;
    if (hi < 0) return 0;
    const loDiff = Math.abs(list[lo].norm_time - targetSeconds);
    const hiDiff = Math.abs(list[hi].norm_time - targetSeconds);
    return loDiff < hiDiff ? lo : hi;
  };

  const scrollToIndex = (index, duration = 180) => {
    if (index == null) return null;
      const targetTop = index * state.rowStride - logBody.clientHeight / 2 + state.rowStride / 2;
    const clamped = Math.max(0, Math.min(targetTop, logBody.scrollHeight));
    const selected = state.filtered[index];
    LogApp.smoothScrollTo(logBody, clamped, duration, () => {
      if (!selected) return;
        const row = logList.querySelector(`[data-row-id="${selected.row_id}"]`);
      if (!row) return;
      row.classList.remove("log-highlight");
      void row.offsetWidth;
      row.classList.add("log-highlight");
    });
    if (bus && selected) bus.emit("event:selected", selected);
    return selected;
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
      applyFilterQueries([]);
      if (searchInput) searchInput.value = "";
    }
    return scrollToRowId(rowId);
  };

  let scrollRaf = 0;
  logBody.addEventListener("scroll", () => {
    if (scrollRaf) return;
    scrollRaf = requestAnimationFrame(() => {
      scrollRaf = 0;
      updateVirtual();
      const index = Math.max(
        0,
        Math.min(
          state.filtered.length - 1,
          Math.floor((logBody.scrollTop + logBody.clientHeight / 2) / state.rowStride)
        )
      );
      const current = state.filtered[index];
      if (bus && current) {
        bus.emit("log:scroll", {
          seconds: current.norm_time,
          rowId: current.row_id,
        });
      }
    });
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
    bus.on("bookmarks:changed", (map) => {
      const rows = logList.querySelectorAll(".log-line");
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

  return {
    scrollToSeconds,
    scrollToRowId,
    ensureRowVisible,
    applyFilters: applyFilterQueries,
    getFilteredEvents: () => state.filtered,
  };
};
