(function () {
  const ui = window.EventLog2UI || {};

  window.EventLog2 = window.EventLog2 || {};
  window.EventLog2.viewerStoreShared = {
    signalFactory: ui.signals?.signal || ((initialValue) => ({ value: initialValue })),
    computedFactory: ui.signals?.computed || null,
    effectFactory: ui.signals?.effect || null,
    readStorage(key, fallback, options = {}) {
      try {
        const raw = localStorage.getItem(key);
        if (raw == null) return fallback;
        const parse = typeof options.parse === "function" ? options.parse : JSON.parse;
        return parse(raw);
      } catch (error) {
        return fallback;
      }
    },
    writeStorage(key, value, options = {}) {
      try {
        const serialize = typeof options.serialize === "function" ? options.serialize : JSON.stringify;
        localStorage.setItem(key, serialize(value));
      } catch (error) {
        return;
      }
    },
    normalizeFilter(filter) {
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
    },
    normalizeFilters(filters) {
      if (!Array.isArray(filters)) return [];

      const byQuery = new Map();
      filters.forEach((filter) => {
        const normalized = window.EventLog2.viewerStoreShared.normalizeFilter(filter);
        if (!normalized) return;
        byQuery.set(normalized.query, normalized);
      });
      return Array.from(byQuery.values());
    },
    normalizeJumpTarget(payload, nonce = 0) {
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
    },
    normalizeScrollState(payload, nonce = 0) {
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
    },
    normalizeSizePair(value, fallback) {
      if (!Array.isArray(value) || value.length !== fallback.length) return fallback.slice();
      const normalized = value.map((entry) => Number(entry));
      if (normalized.some((entry) => !Number.isFinite(entry) || entry <= 0)) {
        return fallback.slice();
      }
      return normalized;
    },
    normalizeSearchEntries(entries) {
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
    },
    sameEvent(left, right) {
      if (left === right) return true;
      if (!left || !right) return false;
      return String(left.row_id) === String(right.row_id);
    },
    sameArray(left, right) {
      if (left === right) return true;
      if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) return false;
      for (let index = 0; index < left.length; index += 1) {
        if (!Object.is(left[index], right[index])) return false;
      }
      return true;
    },
    sameSearchEntries(left, right) {
      if (left === right) return true;
      if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) return false;
      for (let index = 0; index < left.length; index += 1) {
        const leftEntry = left[index];
        const rightEntry = right[index];
        if (
          leftEntry?.query !== rightEntry?.query ||
          leftEntry?.count !== rightEntry?.count ||
          leftEntry?.color !== rightEntry?.color ||
          leftEntry?.label !== rightEntry?.label
        ) {
          return false;
        }
      }
      return true;
    },
    sameFilters(left, right) {
      if (left === right) return true;
      if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) return false;
      for (let index = 0; index < left.length; index += 1) {
        if (
          left[index]?.query !== right[index]?.query ||
          Boolean(left[index]?.enabled) !== Boolean(right[index]?.enabled)
        ) {
          return false;
        }
      }
      return true;
    },
    resolveSignalValue(nextValue, currentValue) {
      return typeof nextValue === "function" ? nextValue(currentValue) : nextValue;
    },
    setSignalValue(signalValue, nextValue, options = {}) {
      const {
        current = (value) => value,
        normalize = (value) => value,
        equals = Object.is,
        afterChange = null,
      } = options;
      const previousValue = signalValue.value;
      const resolved = window.EventLog2.viewerStoreShared.resolveSignalValue(
        nextValue,
        current(previousValue)
      );
      const nextResolved = normalize(resolved);
      if (equals(previousValue, nextResolved)) return previousValue;
      signalValue.value = nextResolved;
      if (typeof afterChange === "function") {
        afterChange(nextResolved, previousValue);
      }
      return signalValue.value;
    },
    createComputed(resolver) {
      const computedFactory = window.EventLog2.viewerStoreShared.computedFactory;
      if (typeof computedFactory === "function") return computedFactory(resolver);
      return {
        get value() {
          return resolver();
        },
      };
    },
    BOOLEAN_STORAGE: {
      parse: (raw) => raw === true || raw === "true",
      serialize: (value) => (value ? "true" : "false"),
    },
    STRING_STORAGE: {
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
    },
    ARRAY_STORAGE: {
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
    },
    syncSignalToStorage(key, signalValue, options = {}) {
      const effectFactory = window.EventLog2.viewerStoreShared.effectFactory;
      if (typeof effectFactory !== "function" || !signalValue) {
        throw new Error("Viewer store requires local @preact/signals effect support.");
      }
      effectFactory(() => {
        window.EventLog2.viewerStoreShared.writeStorage(key, signalValue.value, options);
      });
    },
  };
})();
