(function () {
  window.EventLog2 = window.EventLog2 || {};

  window.EventLog2.createViewerStore = ({ logData }) => {
    const allEvents = Array.isArray(logData?.events) ? logData.events : [];

    return {
      allEvents,
      ...window.EventLog2.createViewerNavigationState({ allEvents }),
      ...window.EventLog2.createViewerLayoutState(),
      ...window.EventLog2.createViewerSearchState(),
      ...window.EventLog2.createViewerChartState(),
      ...window.EventLog2.createViewerActivityState(),
    };
  };
})();
