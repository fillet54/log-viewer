(function () {
  const ui = window.EventLog2UI || {};
  const signalFactory = ui.signals?.signal || ((initialValue) => ({ value: initialValue }));
  const computedFactory = ui.signals?.computed || null;
  const effectFactory = ui.signals?.effect || null;

  window.EventLog2 = window.EventLog2 || {};

  const readStorage = (key, fallback, options = {}) => {
    if (typeof ui.appHooks?.readStorage === "function") {
      return ui.appHooks.readStorage(key, fallback, options);
    }

    try {
      const raw = localStorage.getItem(key);
      if (raw == null) return fallback;
      const parse = typeof options.parse === "function" ? options.parse : JSON.parse;
      return parse(raw);
    } catch (error) {
      return fallback;
    }
  };

  const writeStorage = (key, value, options = {}) => {
    if (typeof ui.appHooks?.writeStorage === "function") {
      ui.appHooks.writeStorage(key, value, options);
      return;
    }

    try {
      const serialize = typeof options.serialize === "function" ? options.serialize : JSON.stringify;
      localStorage.setItem(key, serialize(value));
    } catch (error) {
      return;
    }
  };

  const normalizeFilter = (filter) => {
    if (!filter) return null;
    if (typeof filter === "string") {
      const query = filter.trim();
      return query ? { query, enabled: true } : null;
    }

    const query = String(filter.query || "").trim();
    if (!query) return null;
    return {
      query,
      enabled: filter.enabled !== false,
    };
  };

  const normalizeFilters = (filters) => {
    if (!Array.isArray(filters)) return [];

    const byQuery = new Map();
    filters.forEach((filter) => {
      const normalized = normalizeFilter(filter);
      if (!normalized) return;
      byQuery.set(normalized.query, normalized);
    });
    return Array.from(byQuery.values());
  };

  const normalizeJumpTarget = (payload, nonce = 0) => {
    if (!payload || typeof payload !== "object") return null;
    const rowId =
      payload.rowId == null || payload.rowId === ""
        ? null
        : String(payload.rowId);
    const secondsValue = Number(payload.seconds);
    const seconds = Number.isFinite(secondsValue) ? Math.max(0, Math.floor(secondsValue)) : null;
    if (rowId == null && seconds == null) return null;
    return {
      rowId,
      seconds,
      nonce,
    };
  };

  const normalizeScrollState = (payload, nonce = 0) => {
    if (!payload || typeof payload !== "object") return null;
    const rowId =
      payload.rowId == null || payload.rowId === ""
        ? null
        : String(payload.rowId);
    const secondsValue = Number(payload.seconds);
    const seconds = Number.isFinite(secondsValue) ? Math.max(0, secondsValue) : null;
    if (rowId == null && seconds == null) return null;
    return {
      rowId,
      seconds,
      nonce,
    };
  };

  const normalizeSizePair = (value, fallback) => {
    if (!Array.isArray(value) || value.length !== fallback.length) return fallback.slice();
    const normalized = value.map((entry) => Number(entry));
    if (normalized.some((entry) => !Number.isFinite(entry) || entry <= 0)) {
      return fallback.slice();
    }
    return normalized;
  };

  const normalizeSearchEntries = (entries) => {
    if (!Array.isArray(entries)) return [];
    return entries
      .map((entry) => {
        if (!entry || typeof entry !== "object") return null;
        const query = String(entry.query || "").trim();
        if (!query) return null;
        const count = Math.max(0, Number(entry.count) || 0);
        const color = String(entry.color || "search");
        const label = String(entry.label || query || "(all events)");
        return { query, count, color, label };
      })
      .filter(Boolean);
  };

  const mergeFilterQueries = (currentFilters, queries) => {
    const normalizedQueries = normalizeFilters(queries).map((filter) => filter.query);
    const enabled = new Set(normalizedQueries);
    const nextFilters = normalizeFilters(currentFilters).map((filter) => ({
      ...filter,
      enabled: enabled.has(filter.query),
    }));

    normalizedQueries.forEach((query) => {
      if (nextFilters.some((filter) => filter.query === query)) return;
      nextFilters.push({ query, enabled: true });
    });

    return nextFilters;
  };

  const sameEvent = (left, right) => {
    if (left === right) return true;
    if (!left || !right) return false;
    return String(left.row_id) === String(right.row_id);
  };

  const createComputed = (resolver) => {
    if (typeof computedFactory === "function") return computedFactory(resolver);
    return {
      get value() {
        return resolver();
      },
    };
  };

  const BOOLEAN_STORAGE = {
    parse: (raw) => raw === true || raw === "true",
    serialize: (value) => (value ? "true" : "false"),
  };

  const STRING_STORAGE = {
    parse: (raw) => {
      if (typeof raw !== "string") return String(raw || "");
      try {
        const parsed = JSON.parse(raw);
        return typeof parsed === "string" ? parsed : String(parsed || "");
      } catch (error) {
        return String(raw || "");
      }
    },
    serialize: (value) => String(value || ""),
  };

  const ARRAY_STORAGE = {
    parse: (raw) => {
      if (Array.isArray(raw)) return raw;
      if (typeof raw !== "string") return [];
      try {
        const parsed = JSON.parse(raw);
        return Array.isArray(parsed) ? parsed : [];
      } catch (error) {
        return [];
      }
    },
    serialize: (value) => JSON.stringify(Array.isArray(value) ? value : []),
  };

  const syncSignalToStorage = (key, signalValue, options = {}) => {
    if (typeof effectFactory !== "function" || !signalValue) return false;
    effectFactory(() => {
      writeStorage(key, signalValue.value, options);
    });
    return true;
  };

  window.EventLog2.createViewerStore = ({ logData, bus }) => {
    const allEvents = Array.isArray(logData?.events) ? logData.events : [];
    const suppress = {
      selectedEvent: 0,
      searchFilters: 0,
      filteredEvents: 0,
      logJump: 0,
    };
    const defaultTopSplit = [72, 28];
    const defaultRootSplit = [70, 30];
    const defaultSearchSplit = [28, 72];
    const defaultMainViewSplit = [36, 64];

    const searchFilters = signalFactory(
      normalizeFilters(readStorage(STORAGE_KEYS.searchFilters, []))
    );
    const selectedEvent = signalFactory(null);
    const filteredEvents = signalFactory(allEvents);
    const logJump = signalFactory(null);
    const logScroll = signalFactory(null);
    const detailCollapsed = signalFactory(
      readStorage(STORAGE_KEYS.detailCollapsed, false, BOOLEAN_STORAGE)
    );
    const bottomCollapsed = signalFactory(
      readStorage(STORAGE_KEYS.bottomCollapsed, false, BOOLEAN_STORAGE)
    );
    const topSplitSizes = signalFactory(
      normalizeSizePair(readStorage(STORAGE_KEYS.top, defaultTopSplit, ARRAY_STORAGE), defaultTopSplit)
    );
    const rootSplitSizes = signalFactory(
      normalizeSizePair(readStorage(STORAGE_KEYS.root, defaultRootSplit, ARRAY_STORAGE), defaultRootSplit)
    );
    const rootExpandedSizes = signalFactory(
      normalizeSizePair(readStorage(STORAGE_KEYS.rootExpanded, defaultRootSplit, ARRAY_STORAGE), defaultRootSplit)
    );
    const searchSplitSizes = signalFactory(
      normalizeSizePair(readStorage(STORAGE_KEYS.search, defaultSearchSplit, ARRAY_STORAGE), defaultSearchSplit)
    );
    const mainViewMode = signalFactory(
      readStorage(STORAGE_KEYS.mainViewMode, "split", STRING_STORAGE) || "split"
    );
    const mainViewSplitSizes = signalFactory(
      normalizeSizePair(readStorage(STORAGE_KEYS.mainViewSplit, defaultMainViewSplit, ARRAY_STORAGE), defaultMainViewSplit)
    );
    const chartType = signalFactory(
      readStorage(STORAGE_KEYS.chartType, "", STRING_STORAGE)
    );
    const timelineView = signalFactory(
      readStorage(STORAGE_KEYS.chartTimelineView, "events", STRING_STORAGE) || "events"
    );
    const searchTab = signalFactory(
      readStorage(STORAGE_KEYS.searchTab, "history", STRING_STORAGE) || "history"
    );
    const searchHistory = signalFactory(
      normalizeSearchEntries(readStorage(STORAGE_KEYS.searchHistory, [], ARRAY_STORAGE))
    );
    const searchPinned = signalFactory(
      normalizeSearchEntries(readStorage(STORAGE_KEYS.searchPinned, [], ARRAY_STORAGE))
    );
    const bookmarkVersion = signalFactory(0);
    const commentVersion = signalFactory(0);
    const activeFilterQueries = createComputed(() =>
      normalizeFilters(searchFilters.value)
        .filter((filter) => filter.enabled)
        .map((filter) => filter.query)
    );
    let jumpNonce = 0;
    let scrollNonce = 0;

    const persistFilters = () => {
      writeStorage(STORAGE_KEYS.searchFilters, normalizeFilters(searchFilters.value));
    };

    const persistBooleanSignal = (key, signalValue) => {
      writeStorage(key, Boolean(signalValue.value), BOOLEAN_STORAGE);
    };

    const persistStringSignal = (key, signalValue) => {
      writeStorage(key, String(signalValue.value || ""), STRING_STORAGE);
    };

    const autoSyncDetailCollapsed = syncSignalToStorage(
      STORAGE_KEYS.detailCollapsed,
      detailCollapsed,
      BOOLEAN_STORAGE
    );
    const autoSyncBottomCollapsed = syncSignalToStorage(
      STORAGE_KEYS.bottomCollapsed,
      bottomCollapsed,
      BOOLEAN_STORAGE
    );
    const autoSyncTopSplitSizes = syncSignalToStorage(
      STORAGE_KEYS.top,
      topSplitSizes,
      ARRAY_STORAGE
    );
    const autoSyncRootSplitSizes = syncSignalToStorage(
      STORAGE_KEYS.root,
      rootSplitSizes,
      ARRAY_STORAGE
    );
    const autoSyncRootExpandedSizes = syncSignalToStorage(
      STORAGE_KEYS.rootExpanded,
      rootExpandedSizes,
      ARRAY_STORAGE
    );
    const autoSyncSearchSplitSizes = syncSignalToStorage(
      STORAGE_KEYS.search,
      searchSplitSizes,
      ARRAY_STORAGE
    );
    const autoSyncMainViewMode = syncSignalToStorage(
      STORAGE_KEYS.mainViewMode,
      mainViewMode,
      STRING_STORAGE
    );
    const autoSyncMainViewSplitSizes = syncSignalToStorage(
      STORAGE_KEYS.mainViewSplit,
      mainViewSplitSizes,
      ARRAY_STORAGE
    );
    const autoSyncChartType = syncSignalToStorage(
      STORAGE_KEYS.chartType,
      chartType,
      STRING_STORAGE
    );
    const autoSyncTimelineView = syncSignalToStorage(
      STORAGE_KEYS.chartTimelineView,
      timelineView,
      STRING_STORAGE
    );
    const autoSyncSearchTab = syncSignalToStorage(
      STORAGE_KEYS.searchTab,
      searchTab,
      STRING_STORAGE
    );
    const autoSyncSearchHistory = syncSignalToStorage(
      STORAGE_KEYS.searchHistory,
      searchHistory,
      ARRAY_STORAGE
    );
    const autoSyncSearchPinned = syncSignalToStorage(
      STORAGE_KEYS.searchPinned,
      searchPinned,
      ARRAY_STORAGE
    );

    const setSelectedEvent = (event, options = {}) => {
      const nextEvent = event || null;
      if (!sameEvent(selectedEvent.value, nextEvent)) {
        selectedEvent.value = nextEvent;
      }

      if (options.emitBus === false || !bus) return nextEvent;

      suppress.selectedEvent += 1;
      try {
        bus.emit("event:selected", nextEvent);
      } finally {
        suppress.selectedEvent -= 1;
      }
      return nextEvent;
    };

    const setSearchFilters = (nextValue, options = {}) => {
      const nextFilters = normalizeFilters(
        typeof nextValue === "function" ? nextValue(searchFilters.value) : nextValue
      );
      searchFilters.value = nextFilters;
      persistFilters();

      if (options.emitBus === false || !bus) return nextFilters;

      suppress.searchFilters += 1;
      try {
        bus.emit(
          "filters:apply",
          nextFilters.filter((filter) => filter.enabled).map((filter) => filter.query)
        );
      } finally {
        suppress.searchFilters -= 1;
      }
      return nextFilters;
    };

    const setFilteredEvents = (nextEvents, options = {}) => {
      const resolvedEvents = Array.isArray(nextEvents) ? nextEvents : allEvents;
      filteredEvents.value = resolvedEvents;

      if (options.emitBus === false || !bus) return resolvedEvents;

      suppress.filteredEvents += 1;
      try {
        bus.emit("log:filtered", resolvedEvents);
      } finally {
        suppress.filteredEvents -= 1;
      }
      return resolvedEvents;
    };

    const setLogJump = (nextPayload, options = {}) => {
      const normalized = normalizeJumpTarget(nextPayload, ++jumpNonce);
      if (!normalized) return null;
      logJump.value = normalized;

      if (options.emitBus === false || !bus) return normalized;

      suppress.logJump += 1;
      try {
        bus.emit("log:jump", {
          rowId: normalized.rowId,
          seconds: normalized.seconds,
        });
      } finally {
        suppress.logJump -= 1;
      }
      return normalized;
    };

    const setLogScroll = (nextPayload) => {
      const normalized = normalizeScrollState(nextPayload, ++scrollNonce);
      logScroll.value = normalized;
      return normalized;
    };

    const setDetailCollapsed = (nextValue) => {
      const resolved = typeof nextValue === "function" ? nextValue(Boolean(detailCollapsed.value)) : nextValue;
      const nextResolved = Boolean(resolved);
      if (detailCollapsed.value === nextResolved) return detailCollapsed.value;
      detailCollapsed.value = nextResolved;
      if (!autoSyncDetailCollapsed) {
        persistBooleanSignal(STORAGE_KEYS.detailCollapsed, detailCollapsed);
      }
      return detailCollapsed.value;
    };

    const setBottomCollapsed = (nextValue) => {
      const resolved = typeof nextValue === "function" ? nextValue(Boolean(bottomCollapsed.value)) : nextValue;
      const nextResolved = Boolean(resolved);
      if (bottomCollapsed.value === nextResolved) return bottomCollapsed.value;
      bottomCollapsed.value = nextResolved;
      if (!autoSyncBottomCollapsed) {
        persistBooleanSignal(STORAGE_KEYS.bottomCollapsed, bottomCollapsed);
      }
      return bottomCollapsed.value;
    };

    const setTopSplitSizes = (nextValue) => {
      const resolved = typeof nextValue === "function" ? nextValue(topSplitSizes.value.slice()) : nextValue;
      topSplitSizes.value = normalizeSizePair(resolved, defaultTopSplit);
      if (!autoSyncTopSplitSizes) {
        writeStorage(STORAGE_KEYS.top, topSplitSizes.value, ARRAY_STORAGE);
      }
      return topSplitSizes.value;
    };

    const setRootSplitSizes = (nextValue) => {
      const resolved = typeof nextValue === "function" ? nextValue(rootSplitSizes.value.slice()) : nextValue;
      rootSplitSizes.value = normalizeSizePair(resolved, defaultRootSplit);
      if (!autoSyncRootSplitSizes) {
        writeStorage(STORAGE_KEYS.root, rootSplitSizes.value, ARRAY_STORAGE);
      }
      return rootSplitSizes.value;
    };

    const setRootExpandedSizes = (nextValue) => {
      const resolved = typeof nextValue === "function" ? nextValue(rootExpandedSizes.value.slice()) : nextValue;
      rootExpandedSizes.value = normalizeSizePair(resolved, defaultRootSplit);
      if (!autoSyncRootExpandedSizes) {
        writeStorage(STORAGE_KEYS.rootExpanded, rootExpandedSizes.value, ARRAY_STORAGE);
      }
      return rootExpandedSizes.value;
    };

    const setSearchSplitSizes = (nextValue) => {
      const resolved = typeof nextValue === "function" ? nextValue(searchSplitSizes.value.slice()) : nextValue;
      searchSplitSizes.value = normalizeSizePair(resolved, defaultSearchSplit);
      if (!autoSyncSearchSplitSizes) {
        writeStorage(STORAGE_KEYS.search, searchSplitSizes.value, ARRAY_STORAGE);
      }
      return searchSplitSizes.value;
    };

    const setMainViewMode = (nextValue) => {
      const resolved = typeof nextValue === "function" ? nextValue(String(mainViewMode.value || "split")) : nextValue;
      const nextResolved = String(resolved || "split");
      if (mainViewMode.value === nextResolved) return mainViewMode.value;
      mainViewMode.value = nextResolved;
      if (!autoSyncMainViewMode) {
        persistStringSignal(STORAGE_KEYS.mainViewMode, mainViewMode);
      }
      return mainViewMode.value;
    };

    const setMainViewSplitSizes = (nextValue) => {
      const resolved =
        typeof nextValue === "function" ? nextValue(mainViewSplitSizes.value.slice()) : nextValue;
      mainViewSplitSizes.value = normalizeSizePair(resolved, defaultMainViewSplit);
      if (!autoSyncMainViewSplitSizes) {
        writeStorage(STORAGE_KEYS.mainViewSplit, mainViewSplitSizes.value, ARRAY_STORAGE);
      }
      return mainViewSplitSizes.value;
    };

    const setChartType = (nextValue) => {
      const resolved = typeof nextValue === "function" ? nextValue(String(chartType.value || "")) : nextValue;
      const nextResolved = String(resolved || "");
      if (chartType.value === nextResolved) return chartType.value;
      chartType.value = nextResolved;
      if (!autoSyncChartType) {
        persistStringSignal(STORAGE_KEYS.chartType, chartType);
      }
      return chartType.value;
    };

    const setTimelineView = (nextValue) => {
      const resolved = typeof nextValue === "function" ? nextValue(String(timelineView.value || "events")) : nextValue;
      const nextResolved = String(resolved || "events");
      if (timelineView.value === nextResolved) return timelineView.value;
      timelineView.value = nextResolved;
      if (!autoSyncTimelineView) {
        persistStringSignal(STORAGE_KEYS.chartTimelineView, timelineView);
      }
      return timelineView.value;
    };

    const setSearchTab = (nextValue) => {
      const resolved = typeof nextValue === "function" ? nextValue(String(searchTab.value || "history")) : nextValue;
      const nextResolved = String(resolved || "history");
      if (searchTab.value === nextResolved) return searchTab.value;
      searchTab.value = nextResolved;
      if (!autoSyncSearchTab) {
        persistStringSignal(STORAGE_KEYS.searchTab, searchTab);
      }
      return searchTab.value;
    };

    const setSearchHistory = (nextValue) => {
      const resolved = typeof nextValue === "function" ? nextValue(searchHistory.value.slice()) : nextValue;
      searchHistory.value = normalizeSearchEntries(resolved);
      if (!autoSyncSearchHistory) {
        writeStorage(STORAGE_KEYS.searchHistory, searchHistory.value, ARRAY_STORAGE);
      }
      return searchHistory.value;
    };

    const setSearchPinned = (nextValue) => {
      const resolved = typeof nextValue === "function" ? nextValue(searchPinned.value.slice()) : nextValue;
      searchPinned.value = normalizeSearchEntries(resolved);
      if (!autoSyncSearchPinned) {
        writeStorage(STORAGE_KEYS.searchPinned, searchPinned.value, ARRAY_STORAGE);
      }
      return searchPinned.value;
    };

    const bumpBookmarkVersion = () => {
      bookmarkVersion.value = Number(bookmarkVersion.value || 0) + 1;
      return bookmarkVersion.value;
    };

    const bumpCommentVersion = () => {
      commentVersion.value = Number(commentVersion.value || 0) + 1;
      return commentVersion.value;
    };

    if (bus) {
      bus.on("event:selected", (event) => {
        if (suppress.selectedEvent) return;
        if (!sameEvent(selectedEvent.value, event || null)) {
          selectedEvent.value = event || null;
        }
      });

      bus.on("filters:apply", (queries) => {
        if (suppress.searchFilters) return;
        searchFilters.value = mergeFilterQueries(searchFilters.value, queries || []);
        persistFilters();
      });

      bus.on("log:jump", (payload) => {
        if (suppress.logJump) return;
        const normalized = normalizeJumpTarget(payload, ++jumpNonce);
        if (!normalized) return;
        logJump.value = normalized;
      });
    }

    return {
      allEvents,
      selectedEvent,
      searchFilters,
      activeFilterQueries,
      filteredEvents,
      logJump,
      logScroll,
      detailCollapsed,
      bottomCollapsed,
      topSplitSizes,
      rootSplitSizes,
      rootExpandedSizes,
      searchSplitSizes,
      mainViewMode,
      mainViewSplitSizes,
      chartType,
      timelineView,
      searchTab,
      searchHistory,
      searchPinned,
      bookmarkVersion,
      commentVersion,
      setSelectedEvent,
      setSearchFilters,
      setFilteredEvents,
      setLogJump,
      setLogScroll,
      setDetailCollapsed,
      setBottomCollapsed,
      setTopSplitSizes,
      setRootSplitSizes,
      setRootExpandedSizes,
      setSearchSplitSizes,
      setMainViewMode,
      setMainViewSplitSizes,
      setChartType,
      setTimelineView,
      setSearchTab,
      setSearchHistory,
      setSearchPinned,
      bumpBookmarkVersion,
      bumpCommentVersion,
    };
  };
})();
