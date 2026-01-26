window.LogApp = window.LogApp || {};

LogApp.initRightPane = (bus) => {
  const container = document.getElementById("event-detail");
  if (!container || !bus) return;

  const renderRows = (value, prefix = []) => {
    if (value && typeof value === "object" && !Array.isArray(value)) {
      return Object.entries(value).flatMap(([key, val]) =>
        renderRows(val, [...prefix, key])
      );
    }
    return [
      {
        key: prefix.join("."),
        value: String(value),
      },
    ];
  };

  const renderEvent = (event) => {
    if (!event) {
      container.innerHTML = '<div class="text-sm text-base-content/60">Select a log event to view details.</div>';
      return;
    }

    let dataBlock = '<div class="text-xs text-base-content/50">No event data available.</div>';
    if (event.data) {
      const rows = renderRows(event.data);
      const rendered = rows
        .map(
          (row) => `
          <div class="data-row">
            <div class="data-key">${row.key}</div>
            <div class="data-value">${row.value}</div>
          </div>
        `
        )
        .join("");
      dataBlock = `<div class="event-data">${rendered}</div>`;
    }

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
