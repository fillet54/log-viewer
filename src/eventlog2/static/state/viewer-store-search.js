(function () {
  const shared = window.EventLog2.viewerStoreShared;

  window.EventLog2 = window.EventLog2 || {};
  window.EventLog2.createViewerSearchState = () => {
    const searchFilters = shared.signalFactory(
      shared.normalizeFilters(shared.readStorage(STORAGE_KEYS.searchFilters, []))
    );
    const searchTab = shared.signalFactory(
      shared.readStorage(STORAGE_KEYS.searchTab, "history", shared.STRING_STORAGE) || "history"
    );
    const searchHistory = shared.signalFactory(
      shared.normalizeSearchEntries(shared.readStorage(STORAGE_KEYS.searchHistory, [], shared.ARRAY_STORAGE))
    );
    const searchPinned = shared.signalFactory(
      shared.normalizeSearchEntries(shared.readStorage(STORAGE_KEYS.searchPinned, [], shared.ARRAY_STORAGE))
    );
    const activeFilterQueries = shared.createComputed(() =>
      shared.normalizeFilters(searchFilters.value)
        .filter((filter) => filter.enabled)
        .map((filter) => filter.query)
    );

    shared.syncSignalToStorage(
      STORAGE_KEYS.searchTab,
      searchTab,
      shared.STRING_STORAGE
    );
    shared.syncSignalToStorage(
      STORAGE_KEYS.searchHistory,
      searchHistory,
      shared.ARRAY_STORAGE
    );
    shared.syncSignalToStorage(
      STORAGE_KEYS.searchPinned,
      searchPinned,
      shared.ARRAY_STORAGE
    );

    const persistFilters = (nextFilters) => {
      shared.writeStorage(STORAGE_KEYS.searchFilters, shared.normalizeFilters(nextFilters));
    };

    const setSearchFilters = (nextValue) => {
      return shared.setSignalValue(searchFilters, nextValue, {
        current: (value) => value.slice(),
        normalize: shared.normalizeFilters,
        equals: shared.sameFilters,
        afterChange: persistFilters,
      });
    };

    const setSearchTab = (nextValue) => {
      return shared.setSignalValue(searchTab, nextValue, {
        current: (value) => String(value || "history"),
        normalize: (value) => String(value || "history"),
      });
    };

    const setSearchHistory = (nextValue) => {
      return shared.setSignalValue(searchHistory, nextValue, {
        current: (value) => value.slice(),
        normalize: shared.normalizeSearchEntries,
        equals: shared.sameSearchEntries,
      });
    };

    const setSearchPinned = (nextValue) => {
      return shared.setSignalValue(searchPinned, nextValue, {
        current: (value) => value.slice(),
        normalize: shared.normalizeSearchEntries,
        equals: shared.sameSearchEntries,
      });
    };

    return {
      searchFilters,
      searchTab,
      searchHistory,
      searchPinned,
      activeFilterQueries,
      setSearchFilters,
      setSearchTab,
      setSearchHistory,
      setSearchPinned,
    };
  };
})();
