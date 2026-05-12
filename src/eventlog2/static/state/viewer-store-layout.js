(function () {
  const shared = window.EventLog2.viewerStoreShared;

  window.EventLog2 = window.EventLog2 || {};
  window.EventLog2.createViewerLayoutState = () => {
    const defaultTopSplit = [72, 28];
    const defaultRootSplit = [70, 30];
    const defaultSearchSplit = [28, 72];
    const defaultMainViewSplit = [36, 64];

    const detailCollapsed = shared.signalFactory(
      shared.readStorage(STORAGE_KEYS.detailCollapsed, false, shared.BOOLEAN_STORAGE)
    );
    const bottomCollapsed = shared.signalFactory(
      shared.readStorage(STORAGE_KEYS.bottomCollapsed, false, shared.BOOLEAN_STORAGE)
    );
    const topSplitSizes = shared.signalFactory(
      shared.normalizeSizePair(
        shared.readStorage(STORAGE_KEYS.top, defaultTopSplit, shared.ARRAY_STORAGE),
        defaultTopSplit
      )
    );
    const rootSplitSizes = shared.signalFactory(
      shared.normalizeSizePair(
        shared.readStorage(STORAGE_KEYS.root, defaultRootSplit, shared.ARRAY_STORAGE),
        defaultRootSplit
      )
    );
    const rootExpandedSizes = shared.signalFactory(
      shared.normalizeSizePair(
        shared.readStorage(STORAGE_KEYS.rootExpanded, defaultRootSplit, shared.ARRAY_STORAGE),
        defaultRootSplit
      )
    );
    const searchSplitSizes = shared.signalFactory(
      shared.normalizeSizePair(
        shared.readStorage(STORAGE_KEYS.search, defaultSearchSplit, shared.ARRAY_STORAGE),
        defaultSearchSplit
      )
    );
    const mainViewMode = shared.signalFactory(
      shared.readStorage(STORAGE_KEYS.mainViewMode, "split", shared.STRING_STORAGE) || "split"
    );
    const mainViewSplitSizes = shared.signalFactory(
      shared.normalizeSizePair(
        shared.readStorage(STORAGE_KEYS.mainViewSplit, defaultMainViewSplit, shared.ARRAY_STORAGE),
        defaultMainViewSplit
      )
    );

    shared.syncSignalToStorage(
      STORAGE_KEYS.detailCollapsed,
      detailCollapsed,
      shared.BOOLEAN_STORAGE
    );
    shared.syncSignalToStorage(
      STORAGE_KEYS.bottomCollapsed,
      bottomCollapsed,
      shared.BOOLEAN_STORAGE
    );
    shared.syncSignalToStorage(
      STORAGE_KEYS.top,
      topSplitSizes,
      shared.ARRAY_STORAGE
    );
    shared.syncSignalToStorage(
      STORAGE_KEYS.root,
      rootSplitSizes,
      shared.ARRAY_STORAGE
    );
    shared.syncSignalToStorage(
      STORAGE_KEYS.rootExpanded,
      rootExpandedSizes,
      shared.ARRAY_STORAGE
    );
    shared.syncSignalToStorage(
      STORAGE_KEYS.search,
      searchSplitSizes,
      shared.ARRAY_STORAGE
    );
    shared.syncSignalToStorage(
      STORAGE_KEYS.mainViewMode,
      mainViewMode,
      shared.STRING_STORAGE
    );
    shared.syncSignalToStorage(
      STORAGE_KEYS.mainViewSplit,
      mainViewSplitSizes,
      shared.ARRAY_STORAGE
    );

    const setDetailCollapsed = (nextValue) => {
      return shared.setSignalValue(detailCollapsed, nextValue, {
        current: Boolean,
        normalize: Boolean,
      });
    };

    const setBottomCollapsed = (nextValue) => {
      return shared.setSignalValue(bottomCollapsed, nextValue, {
        current: Boolean,
        normalize: Boolean,
      });
    };

    const setTopSplitSizes = (nextValue) => {
      return shared.setSignalValue(topSplitSizes, nextValue, {
        current: (value) => value.slice(),
        normalize: (value) => shared.normalizeSizePair(value, defaultTopSplit),
        equals: shared.sameArray,
      });
    };

    const setRootSplitSizes = (nextValue) => {
      return shared.setSignalValue(rootSplitSizes, nextValue, {
        current: (value) => value.slice(),
        normalize: (value) => shared.normalizeSizePair(value, defaultRootSplit),
        equals: shared.sameArray,
      });
    };

    const setRootExpandedSizes = (nextValue) => {
      return shared.setSignalValue(rootExpandedSizes, nextValue, {
        current: (value) => value.slice(),
        normalize: (value) => shared.normalizeSizePair(value, defaultRootSplit),
        equals: shared.sameArray,
      });
    };

    const setSearchSplitSizes = (nextValue) => {
      return shared.setSignalValue(searchSplitSizes, nextValue, {
        current: (value) => value.slice(),
        normalize: (value) => shared.normalizeSizePair(value, defaultSearchSplit),
        equals: shared.sameArray,
      });
    };

    const setMainViewMode = (nextValue) => {
      return shared.setSignalValue(mainViewMode, nextValue, {
        current: (value) => String(value || "split"),
        normalize: (value) => String(value || "split"),
      });
    };

    const setMainViewSplitSizes = (nextValue) => {
      return shared.setSignalValue(mainViewSplitSizes, nextValue, {
        current: (value) => value.slice(),
        normalize: (value) => shared.normalizeSizePair(value, defaultMainViewSplit),
        equals: shared.sameArray,
      });
    };

    return {
      detailCollapsed,
      bottomCollapsed,
      topSplitSizes,
      rootSplitSizes,
      rootExpandedSizes,
      searchSplitSizes,
      mainViewMode,
      mainViewSplitSizes,
      setDetailCollapsed,
      setBottomCollapsed,
      setTopSplitSizes,
      setRootSplitSizes,
      setRootExpandedSizes,
      setSearchSplitSizes,
      setMainViewMode,
      setMainViewSplitSizes,
    };
  };
})();
