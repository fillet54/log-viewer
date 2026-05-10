(function () {
  const ui = window.EventLog2UI || {};
  const signalFactory = ui.signals?.signal || ((initialValue) => ({ value: initialValue }));
  const computedFactory = ui.signals?.computed || null;

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

  window.EventLog2.createViewerStore = ({ logData, bus }) => {
    const allEvents = Array.isArray(logData?.events) ? logData.events : [];
    const suppress = {
      selectedEvent: 0,
      searchFilters: 0,
      filteredEvents: 0,
    };

    const searchFilters = signalFactory(
      normalizeFilters(readStorage(STORAGE_KEYS.searchFilters, []))
    );
    const selectedEvent = signalFactory(null);
    const filteredEvents = signalFactory(allEvents);
    const activeFilterQueries = createComputed(() =>
      normalizeFilters(searchFilters.value)
        .filter((filter) => filter.enabled)
        .map((filter) => filter.query)
    );

    const persistFilters = () => {
      writeStorage(STORAGE_KEYS.searchFilters, normalizeFilters(searchFilters.value));
    };

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
    }

    return {
      allEvents,
      selectedEvent,
      searchFilters,
      activeFilterQueries,
      filteredEvents,
      setSelectedEvent,
      setSearchFilters,
      setFilteredEvents,
    };
  };
})();
