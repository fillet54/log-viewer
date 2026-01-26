window.LogApp = window.LogApp || {};

LogApp.initRightPane = (bus) => {
  const container = document.getElementById("event-detail");
  if (!container || !bus) return;

  const renderValue = (value, indent = 0) => {
    if (value && typeof value === "object" && !Array.isArray(value)) {
      return Object.entries(value)
        .map(
          ([key, val]) =>
            `${" ".repeat(indent)}${key}: ${renderValue(val, indent + 2)}`
        )
        .join("\n");
    }
    return String(value);
  };

  const renderEvent = (event) => {
    if (!event) {
      container.innerHTML = '<div class="text-sm text-base-content/60">Select a log event to view details.</div>';
      return;
    }

    const dataBlock = event.data
      ? `<pre class="event-data">${renderValue(event.data)}</pre>`
      : '<div class="text-xs text-base-content/50">No event data available.</div>';

    container.innerHTML = `
      <div class="space-y-3">
        <div class="text-sm font-semibold">${event.name}</div>
        <div class="text-xs text-base-content/60">${event.utc} • ${event.action}</div>
        <div class="text-xs text-base-content/70">${event.description}</div>
        <div class="text-xs text-base-content/50">${event.system}/${event.subsystem}/${event.unit}/${event.code}</div>
        ${dataBlock}
      </div>
    `;
  };

  bus.on("event:selected", renderEvent);
};
