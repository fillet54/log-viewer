// core_event charts: a severity histogram, line views from the header's `series`, and a status board
// showing each system's worst open fault at the selected row. All of it comes from `config`
// (core_event/config.py), so a log can rename severities or add series without code.
import { signals, TONE_COLORS } from "cinc";

const signalEffect = signals.effect;
const getEvents = (logData) => (Array.isArray(logData?.events) ? logData.events : []);
const readPath = (value, path) =>
  String(path || "")
    .split(".")
    .reduce((current, key) => (current == null || typeof current !== "object" ? undefined : current[key]), value);
const matchesWhere = (event, where) =>
  Object.entries(where || {}).every(([path, expected]) => readPath(event, path) === expected);

// For each row: system -> tone of its worst open (SET, not yet CLEARed) fault, per channel.
const buildSystemSnapshots = (events, severities) => {
  const systems = Array.from(new Set(events.map((event) => String(event.system || "").trim()).filter(Boolean))).sort(
    (a, b) => a.localeCompare(b)
  );
  const ordered = events
    .slice()
    .sort(
      (a, b) =>
        Date.parse(a.time || "") - Date.parse(b.time || "") || (Number(a.row_id) || 0) - (Number(b.row_id) || 0)
    );
  const openByKey = new Map();
  const countsBySystem = new Map(systems.map((system) => [system, severities.map(() => 0)]));
  const statusOf = (system) => {
    const counts = countsBySystem.get(system) || [];
    for (let rank = counts.length - 1; rank > 0; rank -= 1) if (counts[rank] > 0) return severities[rank].tone;
    return severities[0]?.tone || "green";
  };
  const snapshots = new Map();
  const timeline = []; // [{t, snapshot}] in time order, for rows of other types and scroll positions
  ordered.forEach((event) => {
    const system = String(event.system || "").trim();
    const counts = countsBySystem.get(system);
    const rank = Number(event.severity_rank) || 0;
    const action = String(event.set_clear || "").trim().toLowerCase();
    (Array.isArray(event.channels) ? event.channels : []).forEach((channel) => {
      if (!counts) return;
      const key = [system, event.subsystem, event.unit, event.code, channel].map((v) => String(v || "").trim()).join("|");
      if (action === "set") {
        if (openByKey.has(key)) return;
        openByKey.set(key, rank);
        counts[rank] += 1;
      } else if (openByKey.has(key)) {
        const openRank = openByKey.get(key);
        openByKey.delete(key);
        if (counts[openRank] > 0) counts[openRank] -= 1;
      }
    });
    const snapshot = Object.fromEntries(systems.map((name) => [name, statusOf(name)]));
    snapshots.set(String(event.row_id), snapshot);
    const t = Date.parse(event.time || "");
    if (Number.isFinite(t)) timeline.push({ t, snapshot });
  });
  return { systems, snapshots, timeline };
};

const mountSystemsPanel = (panel, context, logTypeId, severities) => {
  const events = getEvents(context.logData).filter((event) => !event.log_type || event.log_type === logTypeId);
  const { systems, snapshots, timeline } = buildSystemSnapshots(events, severities);
  // The state after the last of this type's rows at or before `timeMs`.
  const snapshotAt = (timeMs) => {
    let lo = 0;
    let hi = timeline.length - 1;
    let found = null;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      if (timeline[mid].t <= timeMs) {
        found = timeline[mid].snapshot;
        lo = mid + 1;
      } else hi = mid - 1;
    }
    return found;
  };
  const labelOf = Object.fromEntries(severities.map((item) => [item.tone, item.name]));
  const lowest = severities[0]?.tone || "green";

  // Tall and narrow in the side panel: one row per system; a wrapping grid in the chart area.
  const vertical = context.slot === "side";
  panel.innerHTML = `<div class="ce-status-board${vertical ? " is-list" : ""}"></div>`;
  const board = panel.querySelector(".ce-status-board");
  // target: {rowId, timeMs}. A row of this type gives its exact state (rows can share a time);
  // anything else (another type's row, a scroll position) uses the time.
  const render = (target) => {
    const snapshot =
      (target?.rowId != null ? snapshots.get(String(target.rowId)) : null) ||
      (Number.isFinite(target?.timeMs) ? snapshotAt(target.timeMs) : null) ||
      {};
    board.innerHTML = "";
    systems.forEach((system) => {
      const tone = snapshot[system] || lowest;
      const card = document.createElement("div");
      card.className = `ce-status-card ce-status-${tone}`;
      card.title = `${system}: ${labelOf[tone] || tone}`;
      const name = document.createElement("div");
      name.className = "ce-status-name";
      name.textContent = system;
      card.appendChild(name);
      if (vertical) {
        const state = document.createElement("div");
        state.className = "ce-status-state";
        state.textContent = labelOf[tone] || tone;
        card.appendChild(state);
      }
      board.appendChild(card);
    });
  };
  // Like the Map: show the state at whichever moved last, the selected row or the log's scroll
  // position. Selecting a row jumps there; scrolling afterwards takes over again.
  const store = context.viewerStore;
  const fromEvent = (event) => (event ? { rowId: event.row_id, timeMs: Date.parse(event.time || "") } : null);
  const fromScroll = (scroll) => (scroll ? { rowId: scroll.rowId, timeMs: scroll.timeMs } : null);
  let target = fromEvent(store?.selectedEvent?.value) || fromScroll(store?.logScroll?.value) || fromEvent(events[0]);
  const show = (next) => {
    if (!next) return;
    target = next;
    render(target);
  };
  render(target);
  const off = store
    ? [
        signalEffect(() => show(fromScroll(store.logScroll?.value))),
        signalEffect(() => show(fromEvent(store.selectedEvent?.value))),
      ]
    : [];
  return {
    activate() {
      render(target);
    },
    destroy() {
      off.forEach((unsubscribe) => unsubscribe());
    },
  };
};

export default function setup({ logTypeId, config, registerChart, registerTimelineView }) {
  const severities = Array.isArray(config.severities) && config.severities.length
    ? config.severities
    : [{ name: "Green", tone: "green" }];

  registerTimelineView({
    id: "severity",
    label: "Severity",
    kind: "histogram",
    stacked: true,
    datasets: severities.map(({ name, tone }) => ({
      label: name,
      color: TONE_COLORS[tone] || TONE_COLORS.green,
      filter: (event) => event.log_type === logTypeId && event.color === name,
    })),
  });

  (Array.isArray(config.series) ? config.series : []).forEach((series, index) => {
    const color = `var(--series-${((index + 2) % 6) + 1})`; // series-3, 4, 5, 6, 1, 2
    registerTimelineView({
      id: series.id,
      label: series.label,
      kind: "line",
      datasets: [
        {
          label: series.unit ? `${series.label} (${series.unit})` : series.label,
          borderColor: color,
          backgroundColor: `color-mix(in srgb, ${color} 18%, transparent)`,
          pointRadius: 2,
          tension: 0.2,
          filter: (event) =>
            event.log_type === logTypeId &&
            typeof readPath(event, series.field) === "number" &&
            matchesWhere(event, series.where),
          value: (event) => readPath(event, series.field),
        },
      ],
      // Declared bounds are the axis range (e.g. 0-100 %); otherwise the axis fits the data.
      configureChart(chart) {
        const y = chart.options.scales.y;
        if (series.min != null) y.min = series.min;
        if (series.max != null) y.max = series.max;
        if (series.min != null || series.max != null) y.grace = 0;
        if (series.min != null) y.beginAtZero = false;
      },
    });
  });

  registerChart({
    id: "systems",
    label: "Subsystem Status",
    slots: ["side", "main"], // a list that grows with the systems: starts in the side panel
    renderPanel(panel, context) {
      return mountSystemsPanel(panel, context, logTypeId, severities);
    },
    buildCommands(container) {
      const hint = document.createElement("div");
      hint.className = "chart-command-hint";
      hint.textContent = "System status follows the selected or centered event.";
      container.appendChild(hint);
    },
  });
}
