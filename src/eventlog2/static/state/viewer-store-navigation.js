(function () {
  const shared = window.EventLog2.viewerStoreShared;

  window.EventLog2 = window.EventLog2 || {};
  window.EventLog2.createViewerNavigationState = ({ allEvents }) => {
    const selectedEvent = shared.signalFactory(null);
    const filteredEvents = shared.signalFactory(allEvents);
    const logJump = shared.signalFactory(null);
    const logScroll = shared.signalFactory(null);
    let jumpNonce = 0;
    let scrollNonce = 0;

    const setSelectedEvent = (event) => {
      return shared.setSignalValue(selectedEvent, event, {
        normalize: (value) => value || null,
        equals: shared.sameEvent,
      });
    };

    const setFilteredEvents = (nextEvents) => {
      return shared.setSignalValue(filteredEvents, nextEvents, {
        normalize: (value) => (Array.isArray(value) ? value : allEvents),
      });
    };

    const setLogJump = (nextPayload) => {
      const normalized = shared.normalizeJumpTarget(nextPayload, ++jumpNonce);
      if (!normalized) return null;
      logJump.value = normalized;
      return normalized;
    };

    const setLogScroll = (nextPayload) => {
      const normalized = shared.normalizeScrollState(nextPayload, ++scrollNonce);
      logScroll.value = normalized;
      return normalized;
    };

    return {
      selectedEvent,
      filteredEvents,
      logJump,
      logScroll,
      setSelectedEvent,
      setFilteredEvents,
      setLogJump,
      setLogScroll,
    };
  };
})();
