import { effect as signalEffect } from "preact/signals";
import { EventLog2 } from "static/runtime.js";

const getEvents = (logData) => (Array.isArray(logData?.events) ? logData.events : []);

const buildSystemStatusSnapshots = (events) => {
  const severityRank = {
    Green: 0,
    Yellow: 1,
    Red: 2,
    "Flashing Red": 3,
  };

  const systems = Array.from(
    new Set(events.map((event) => String(event.system || "").trim()).filter(Boolean))
  ).sort((a, b) => a.localeCompare(b));

  const ordered = events
    .slice()
    .sort(
      (a, b) =>
        (Number(a.norm_time) || 0) - (Number(b.norm_time) || 0) ||
        (Number(a.row_id) || 0) - (Number(b.row_id) || 0)
    );

  const openByKey = new Map();
  const countsBySystem = new Map(
    systems.map((system) => [system, { Green: 0, Yellow: 0, Red: 0, "Flashing Red": 0 }])
  );
  const snapshots = new Map();

  const getStatus = (system) => {
    const counts = countsBySystem.get(system);
    if (!counts) return "Green";
    if (counts["Flashing Red"] > 0) return "Flashing Red";
    if (counts.Red > 0) return "Red";
    if (counts.Yellow > 0) return "Yellow";
    return "Green";
  };

  ordered.forEach((event) => {
    const action = String(event.set_clear || "").trim().toLowerCase();
    const channels = Array.isArray(event.channels) ? event.channels : [];
    channels.forEach((channel) => {
      const key = [
        String(event.system || "").trim(),
        String(event.subsystem || "").trim(),
        String(event.unit || "").trim(),
        String(event.code || "").trim(),
        String(channel || "").trim(),
      ].join("|");
      const system = String(event.system || "").trim();
      const color = severityRank[event.color] != null ? event.color : "Green";
      const systemCounts = countsBySystem.get(system);
      if (!systemCounts) return;

      if (action === "set") {
        if (openByKey.has(key)) return;
        openByKey.set(key, { system, color });
        systemCounts[color] += 1;
        return;
      }

      if (action !== "clear" || !openByKey.has(key)) return;
      const open = openByKey.get(key);
      openByKey.delete(key);
      if (open?.color && systemCounts[open.color] > 0) systemCounts[open.color] -= 1;
    });

    snapshots.set(
      String(event.row_id),
      Object.fromEntries(systems.map((system) => [system, getStatus(system)]))
    );
  });

  return { systems, snapshots };
};

const mountSystemsPanel = (panel, context) => {
  const events = getEvents(context.logData);
  const { systems, snapshots } = buildSystemStatusSnapshots(events);
  const systemStatusClass = {
    Green: "status-green",
    Yellow: "status-yellow",
    Red: "status-red",
    "Flashing Red": "status-flashing-red",
  };

  panel.innerHTML = '<div class="system-status-board"></div>';
  const board = panel.querySelector(".system-status-board");

  const renderSystemStatus = (rowId = events[0]?.row_id ?? null) => {
    const snapshot =
      (rowId != null ? snapshots.get(String(rowId)) : null) ||
      Object.fromEntries(systems.map((system) => [system, "Green"]));
    board.innerHTML = "";
    const fragment = document.createDocumentFragment();
    systems.forEach((system) => {
      const state = snapshot[system] || "Green";
      const item = document.createElement("div");
      item.className = `system-status-card ${systemStatusClass[state] || "status-green"}`;
      item.title = `${system}: ${state}`;
      item.innerHTML = `<div class="system-status-name">${system}</div>`;
      fragment.appendChild(item);
    });
    board.appendChild(fragment);
  };

  renderSystemStatus();

  const off = [];
  const resolveCurrentRowId = () =>
    context.viewerStore?.selectedEvent?.value?.row_id ??
    context.viewerStore?.logScroll?.value?.rowId ??
    events[0]?.row_id ??
    null;

  if (context.viewerStore) {
    off.push(
      signalEffect(() => {
        const selectedRowId = context.viewerStore.selectedEvent?.value?.row_id ?? null;
        const scrollRowId = context.viewerStore.logScroll?.value?.rowId ?? null;
        renderSystemStatus(selectedRowId ?? scrollRowId ?? events[0]?.row_id ?? null);
      })
    );
  }

  return {
    activate() {
      renderSystemStatus(resolveCurrentRowId());
    },
    destroy() {
      off.forEach((unsubscribe) => unsubscribe && unsubscribe());
    },
  };
};

EventLog2.registerLogTimelineView("core_event", {
  id: "severity",
  label: "Severity",
  kind: "histogram",
  stacked: true,
  datasets: [
    {
      label: "Green",
      backgroundColor: "rgba(34, 197, 94, 0.75)",
      borderColor: "rgba(22, 163, 74, 1)",
      filter: (event) => event.color === "Green",
    },
    {
      label: "Yellow",
      backgroundColor: "rgba(250, 204, 21, 0.8)",
      borderColor: "rgba(234, 179, 8, 1)",
      filter: (event) => event.color === "Yellow",
    },
    {
      label: "Red",
      backgroundColor: "rgba(239, 68, 68, 0.8)",
      borderColor: "rgba(220, 38, 38, 1)",
      filter: (event) => event.color === "Red",
    },
    {
      label: "Flashing Red",
      backgroundColor: "rgba(127, 29, 29, 0.85)",
      borderColor: "rgba(88, 28, 28, 1)",
      filter: (event) => event.color === "Flashing Red",
    },
  ],
});

EventLog2.registerLogTimelineView("core_event", {
  id: "bus-load",
  label: "Bus Load",
  kind: "line",
  datasets: [
    {
      label: "Bus Load %",
      borderColor: "rgba(245, 158, 11, 0.95)",
      backgroundColor: "rgba(245, 158, 11, 0.18)",
      pointRadius: 2,
      tension: 0.2,
      filter: (event) => event.id === "pwr_bus" && typeof event?.data?.bus?.load_pct === "number",
      value: (event) => event.data.bus.load_pct,
    },
  ],
  configureChart(chart) {
    chart.options.scales.y.suggestedMin = 60;
    chart.options.scales.y.suggestedMax = 72;
  },
});

EventLog2.registerLogChartType("core_event", {
  id: "systems",
  label: "Subsystem Status",
  renderPanel(panel, context) {
    return mountSystemsPanel(panel, context);
  },
  buildCommands(container) {
    const hint = document.createElement("div");
    hint.className = "chart-command-hint";
    hint.textContent = "System status follows the selected or centered event.";
    container.appendChild(hint);
  },
});
