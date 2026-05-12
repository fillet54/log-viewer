(function () {
  const shared = window.EventLog2.viewerStoreShared;

  window.EventLog2 = window.EventLog2 || {};
  window.EventLog2.createViewerChartState = () => {
    const chartType = shared.signalFactory(
      shared.readStorage(STORAGE_KEYS.chartType, "", shared.STRING_STORAGE)
    );
    const timelineView = shared.signalFactory(
      shared.readStorage(STORAGE_KEYS.chartTimelineView, "events", shared.STRING_STORAGE) || "events"
    );

    shared.syncSignalToStorage(
      STORAGE_KEYS.chartType,
      chartType,
      shared.STRING_STORAGE
    );
    shared.syncSignalToStorage(
      STORAGE_KEYS.chartTimelineView,
      timelineView,
      shared.STRING_STORAGE
    );

    const setChartType = (nextValue) => {
      return shared.setSignalValue(chartType, nextValue, {
        current: (value) => String(value || ""),
        normalize: (value) => String(value || ""),
      });
    };

    const setTimelineView = (nextValue) => {
      return shared.setSignalValue(timelineView, nextValue, {
        current: (value) => String(value || "events"),
        normalize: (value) => String(value || "events"),
      });
    };

    return {
      chartType,
      timelineView,
      setChartType,
      setTimelineView,
    };
  };
})();
