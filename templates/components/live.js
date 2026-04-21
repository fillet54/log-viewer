window.LogApp = window.LogApp || {};

LogApp.loadLiveConfig = () => {
  const payload = document.getElementById("live-config");
  if (!payload) return null;
  try {
    return JSON.parse(payload.textContent || "{}");
  } catch (err) {
    return null;
  }
};

LogApp.initLiveStream = (logData, bus, logListController) => {
  const config = LogApp.loadLiveConfig();
  const statusEl = document.getElementById("live-status");
  const countEl = document.getElementById("live-event-count");
  const followEl = document.getElementById("live-follow-bottom");
  const reconnectEl = document.getElementById("live-reconnect");

  if (!config || !logListController || !config.stream_url) return null;

  let eventSource = null;

  const setStatus = (label, className = "") => {
    if (!statusEl) return;
    statusEl.textContent = label;
    statusEl.className = `badge badge-sm ${className}`.trim();
  };

  const refreshCount = () => {
    if (!countEl) return;
    const total = Array.isArray(logData?.events) ? logData.events.length : 0;
    countEl.textContent = `${total} events`;
  };

  const shouldFollow = () => Boolean(followEl?.checked);

  const connect = () => {
    if (eventSource) eventSource.close();
    setStatus("Connecting", "badge-warning");
    eventSource = new EventSource(config.stream_url);

    eventSource.addEventListener("open", () => {
      setStatus("Live", "badge-success");
    });

    eventSource.addEventListener("error", () => {
      setStatus("Reconnecting", "badge-warning");
    });

    eventSource.addEventListener("log-event", (message) => {
      try {
        const payload = JSON.parse(message.data || "{}");
        if (payload?.type !== "upsert" || !payload.event) return;
        const nextEvent = payload.event;
        const nextEvents = Array.isArray(logData?.events) ? logData.events : [];
        const existingIndex = nextEvents.findIndex(
          (entry) => String(entry.row_id) === String(nextEvent.row_id)
        );
        if (existingIndex >= 0) {
          nextEvents.splice(existingIndex, 1, nextEvent);
        } else {
          nextEvents.push(nextEvent);
        }
        nextEvents.sort((left, right) => {
          const leftTime = Number(left?.norm_time) || 0;
          const rightTime = Number(right?.norm_time) || 0;
          if (leftTime !== rightTime) return leftTime - rightTime;
          return (Number(left?.row_id) || 0) - (Number(right?.row_id) || 0);
        });
        logListController.upsertEvent(nextEvent, { stickToBottom: shouldFollow() });
        refreshCount();
        if (bus) bus.emit("live:event", nextEvent);
      } catch (err) {
        return;
      }
    });
  };

  reconnectEl?.addEventListener("click", () => {
    connect();
  });

  if (shouldFollow()) {
    requestAnimationFrame(() => {
      logListController.scrollToBottom();
    });
  }

  refreshCount();
  connect();

  window.addEventListener("beforeunload", () => {
    if (eventSource) eventSource.close();
  });

  return {
    reconnect: connect,
    close: () => eventSource?.close(),
  };
};
