import { signal, computed } from "preact/signals";
import { readStorage, writeStorage, syncSignalToStorage, setSignal, STRING_STORAGE, ARRAY_STORAGE, STORAGE_KEYS } from "./storage.js";

const normalizeFilter = (filter) => {
  if (!filter) return null;
  if (typeof filter === "string") {
    const query = filter.trim();
    return query ? { query, enabled: true } : null;
  }
  const query = String(filter.query || "").trim();
  if (!query) return null;
  return { query, enabled: filter.enabled !== false };
};

const normalizeFilters = (filters) => {
  if (!Array.isArray(filters)) return [];
  const byQuery = new Map();
  filters.forEach((filter) => {
    const normalized = normalizeFilter(filter);
    if (normalized) byQuery.set(normalized.query, normalized);
  });
  return Array.from(byQuery.values());
};

const normalizeSearchEntries = (entries) => {
  if (!Array.isArray(entries)) return [];
  return entries.map((entry) => {
    if (!entry || typeof entry !== "object") return null;
    const query = String(entry.query || "").trim();
    if (!query) return null;
    return {
      query,
      count: Math.max(0, Number(entry.count) || 0),
      color: String(entry.color || "search"),
      label: String(entry.label || query || "(all events)"),
    };
  }).filter(Boolean);
};

const sameFilters = (left, right) => {
  if (left === right) return true;
  if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) return false;
  for (let i = 0; i < left.length; i++) {
    if (left[i]?.query !== right[i]?.query || Boolean(left[i]?.enabled) !== Boolean(right[i]?.enabled)) return false;
  }
  return true;
};

const sameSearchEntries = (left, right) => {
  if (left === right) return true;
  if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) return false;
  for (let i = 0; i < left.length; i++) {
    const l = left[i]; const r = right[i];
    if (l?.query !== r?.query || l?.count !== r?.count || l?.color !== r?.color || l?.label !== r?.label) return false;
  }
  return true;
};

export const createSearchState = () => {
  const searchFilters = signal(normalizeFilters(readStorage(STORAGE_KEYS.searchFilters, [])));
  const searchTab = signal(readStorage(STORAGE_KEYS.searchTab, "history", STRING_STORAGE) || "history");
  const searchHistory = signal(normalizeSearchEntries(readStorage(STORAGE_KEYS.searchHistory, [], ARRAY_STORAGE)));
  const searchPinned = signal(normalizeSearchEntries(readStorage(STORAGE_KEYS.searchPinned, [], ARRAY_STORAGE)));
  const activeFilterQueries = computed(() =>
    normalizeFilters(searchFilters.value).filter((f) => f.enabled).map((f) => f.query)
  );

  syncSignalToStorage(STORAGE_KEYS.searchTab, searchTab, STRING_STORAGE);
  syncSignalToStorage(STORAGE_KEYS.searchHistory, searchHistory, ARRAY_STORAGE);
  syncSignalToStorage(STORAGE_KEYS.searchPinned, searchPinned, ARRAY_STORAGE);

  const setSearchFilters = (nextValue) =>
    setSignal(searchFilters, nextValue, {
      normalize: normalizeFilters,
      equals: sameFilters,
      afterChange: (next) => writeStorage(STORAGE_KEYS.searchFilters, next),
    });

  const setSearchTab = (v) => setSignal(searchTab, v, { normalize: (val) => String(val || "history") });
  const setSearchHistory = (v) => setSignal(searchHistory, v, { normalize: normalizeSearchEntries, equals: sameSearchEntries });
  const setSearchPinned = (v) => setSignal(searchPinned, v, { normalize: normalizeSearchEntries, equals: sameSearchEntries });

  return { searchFilters, searchTab, searchHistory, searchPinned, activeFilterQueries, setSearchFilters, setSearchTab, setSearchHistory, setSearchPinned };
};
