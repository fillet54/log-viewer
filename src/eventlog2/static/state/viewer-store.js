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

    const searchFilters = signalFactory(
      normalizeFilters(readStorage(STORAGE_KEYS.searchFilters, []))
    );
    const selectedEvent = signalFactory(null);
    const filteredEvents = signalFactory(allEvents);
    const logJump = signalFactory(null);
    const detailCollapsed = signalFactory(
      readStorage(STORAGE_KEYS.detailCollapsed, false, BOOLEAN_STORAGE)
    );
    const bottomCollapsed = signalFactory(
      readStorage(STORAGE_KEYS.bottomCollapsed, false, BOOLEAN_STORAGE)
    );
    const mainViewMode = signalFactory(
      readStorage(STORAGE_KEYS.mainViewMode, "split", STRING_STORAGE) || "split"
    );
    const chartType = signalFactory(
      readStorage(STORAGE_KEYS.chartType, "", STRING_STORAGE)
    );
    const activeFilterQueries = createComputed(() =>
      normalizeFilters(searchFilters.value)
        .filter((filter) => filter.enabled)
        .map((filter) => filter.query)
    );
    let jumpNonce = 0;

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
    const autoSyncMainViewMode = syncSignalToStorage(
      STORAGE_KEYS.mainViewMode,
      mainViewMode,
      STRING_STORAGE
    );
    const autoSyncChartType = syncSignalToStorage(
      STORAGE_KEYS.chartType,
      chartType,
      STRING_STORAGE
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

    const setDetailCollapsed = (nextValue) => {
      const resolved = typeof nextValue === "function" ? nextValue(Boolean(detailCollapsed.value)) : nextValue;
      detailCollapsed.value = Boolean(resolved);
      if (!autoSyncDetailCollapsed) {
        persistBooleanSignal(STORAGE_KEYS.detailCollapsed, detailCollapsed);
      }
      return detailCollapsed.value;
    };

    const setBottomCollapsed = (nextValue) => {
      const resolved = typeof nextValue === "function" ? nextValue(Boolean(bottomCollapsed.value)) : nextValue;
      bottomCollapsed.value = Boolean(resolved);
      if (!autoSyncBottomCollapsed) {
        persistBooleanSignal(STORAGE_KEYS.bottomCollapsed, bottomCollapsed);
      }
      return bottomCollapsed.value;
    };

    const setMainViewMode = (nextValue) => {
      const resolved = typeof nextValue === "function" ? nextValue(String(mainViewMode.value || "split")) : nextValue;
      mainViewMode.value = String(resolved || "split");
      if (!autoSyncMainViewMode) {
        persistStringSignal(STORAGE_KEYS.mainViewMode, mainViewMode);
      }
      return mainViewMode.value;
    };

    const setChartType = (nextValue) => {
      const resolved = typeof nextValue === "function" ? nextValue(String(chartType.value || "")) : nextValue;
      chartType.value = String(resolved || "");
      if (!autoSyncChartType) {
        persistStringSignal(STORAGE_KEYS.chartType, chartType);
      }
      return chartType.value;
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
      detailCollapsed,
      bottomCollapsed,
      mainViewMode,
      chartType,
      setSelectedEvent,
      setSearchFilters,
      setFilteredEvents,
      setLogJump,
      setDetailCollapsed,
      setBottomCollapsed,
      setMainViewMode,
      setChartType,
    };
  };
})();
