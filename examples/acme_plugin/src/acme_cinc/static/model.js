// Fault intervals from core_event rows, shared by the Overview and Fault Timeline charts.
//
// A fault is one system/subsystem/unit/code. A SET opens it (if it is not already open) and the next
// CLEAR closes it, so each fault becomes a list of intervals plus the SET/CLEAR marks along the way.
// core_event has already given every row `tone` (green/yellow/red/flashing-red) and `severity_rank`.

export const timeOf = (event) => Date.parse(event?.time || "");

export const faultKey = (event) =>
  [event.system, event.subsystem, event.unit, event.code || event.name].map((part) => String(part ?? "")).join("|");

export const buildFaults = (events) => {
  const faults = new Map();
  const ordered = events
    .filter((event) => Number.isFinite(timeOf(event)))
    .sort((a, b) => timeOf(a) - timeOf(b) || a.row_id - b.row_id);
  ordered.forEach((event) => {
    const key = faultKey(event);
    let fault = faults.get(key);
    if (!fault) {
      fault = {
        key,
        name: event.name || event.code || "Fault",
        code: event.code || "",
        system: event.system || "—",
        subsystem: event.subsystem || "",
        unit: event.unit || "",
        intervals: [],
        marks: [],
        open: null,
        worstRank: -1,
        worstTone: "green",
        worstColor: "",
      };
      faults.set(key, fault);
    }
    const t = timeOf(event);
    const isClear = String(event.set_clear).toLowerCase() === "clear";
    fault.marks.push({ t, kind: isClear ? "clear" : "set", rowId: event.row_id, tone: event.tone, color: event.color });
    if (!isClear) {
      if ((event.severity_rank ?? 0) > fault.worstRank) {
        fault.worstRank = event.severity_rank ?? 0;
        fault.worstTone = event.tone || "green";
        fault.worstColor = event.color || "";
      }
      if (!fault.open) {
        fault.open = { start: t, end: null, tone: event.tone || "green", color: event.color, setRowId: event.row_id };
      }
    } else if (fault.open) {
      fault.open.end = t;
      fault.open.clearRowId = event.row_id;
      fault.intervals.push(fault.open);
      fault.open = null;
    }
  });
  faults.forEach((fault) => {
    if (fault.open) fault.intervals.push(fault.open); // still set at the end of the log
    delete fault.open;
  });
  return Array.from(faults.values());
};

// Faults grouped into "System / Subsystem" sections, sorted by name.
export const groupBySubsystem = (faults) => {
  const groups = new Map();
  faults.forEach((fault) => {
    const id = `${fault.system}|${fault.subsystem}`;
    if (!groups.has(id)) groups.set(id, { id, system: fault.system, subsystem: fault.subsystem, faults: [] });
    groups.get(id).faults.push(fault);
  });
  return Array.from(groups.values())
    .map((group) => ({ ...group, faults: group.faults.sort((a, b) => a.name.localeCompare(b.name)) }))
    .sort((a, b) => `${a.system} ${a.subsystem}`.localeCompare(`${b.system} ${b.subsystem}`));
};

// [start, end] in ms over every event in the log (all types) and the tracks.
export const logSpan = (logData) => {
  let start = Infinity;
  let end = -Infinity;
  (logData?.events || []).forEach((event) => {
    const t = timeOf(event);
    if (Number.isFinite(t)) {
      start = Math.min(start, t);
      end = Math.max(end, t);
    }
  });
  (logData?.tracks || []).forEach((track) =>
    (track.samples || []).forEach(([t]) => {
      start = Math.min(start, t);
      end = Math.max(end, t);
    })
  );
  return Number.isFinite(start) ? { start, end: Math.max(end, start + 1) } : null;
};

export const intervalEnd = (interval, span) => interval.end ?? span.end;
export const isOpenAt = (interval, t) => interval.start <= t && (interval.end == null || t < interval.end);

const pad = (value) => String(value).padStart(2, "0");
export const formatClock = (ms) => {
  const d = new Date(ms);
  return `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
};
export const formatDateTime = (ms) => new Date(ms).toISOString().replace("T", " ").replace(/\.\d+Z$/, "Z");
export const formatDuration = (ms) => {
  const seconds = Math.max(0, Math.round(ms / 1000));
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  return h ? `${h}h ${pad(m)}m ${pad(s)}s` : m ? `${m}m ${pad(s)}s` : `${s}s`;
};

// The time the rest of the viewer is looking at: the selected row, else the log's scroll position.
export const currentTime = (viewerStore) => {
  const selected = timeOf(viewerStore?.selectedEvent?.value);
  if (Number.isFinite(selected)) return selected;
  const scroll = viewerStore?.logScroll?.value?.timeMs;
  return Number.isFinite(scroll) ? scroll : null;
};

// Jump the log to a row (selecting it) or to a time.
export const jumpTo = (context, { rowId = null, timeMs = null } = {}) => {
  const store = context.viewerStore;
  if (rowId != null) {
    const event = (context.logData?.events || []).find((item) => item.row_id === rowId);
    if (event) store?.setSelectedEvent?.(event);
    store?.setLogJump?.({ rowId });
  } else if (Number.isFinite(timeMs)) {
    store?.setLogJump?.({ timeMs });
  }
};

export const element = (tag, className, text) => {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text != null) node.textContent = text;
  return node;
};
