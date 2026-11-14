// "Fault Timeline": one section per system/subsystem, one row per fault.
//
// Each row shows when the fault was set (a bar in its severity colour), with a ▲ at every SET and a ▽
// at every CLEAR. A section's header sums up its faults: counts, how many are set at the cursor, the
// worst severity, and a strip of when any of them was set. A cursor line follows the row selected in
// the log; clicking a mark selects that row, clicking elsewhere on a track moves the log to that time.
import { signals, TONE_COLORS } from "cinc";
import {
  buildFaults,
  currentTime,
  element,
  formatClock,
  formatDuration,
  groupBySubsystem,
  intervalEnd,
  isOpenAt,
  jumpTo,
  logSpan,
} from "./model.js";

const TONE_RANK = { green: 0, yellow: 1, red: 2, "flashing-red": 3 };
const LABEL_WIDTH = 260; // px; keep in sync with --acme-label-w in faults.css
const ALL = "__all__";

const niceStep = (spanMs, target = 6) => {
  const steps = [1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 21600, 43200, 86400].map((s) => s * 1000);
  return steps.find((step) => spanMs / step <= target) || steps[steps.length - 1];
};

// Where any fault of a group was set, split into pieces coloured by the worst tone set at the time.
const coverage = (faults, span) => {
  const intervals = faults.flatMap((fault) =>
    fault.intervals.map((interval) => ({ start: interval.start, end: intervalEnd(interval, span), tone: interval.tone }))
  );
  const edges = Array.from(new Set(intervals.flatMap((item) => [item.start, item.end]))).sort((a, b) => a - b);
  const pieces = [];
  for (let i = 0; i < edges.length - 1; i += 1) {
    const [start, end] = [edges[i], edges[i + 1]];
    const mid = (start + end) / 2;
    const covering = intervals.filter((item) => item.start <= mid && mid < item.end);
    if (!covering.length) continue;
    const tone = covering.reduce((best, item) => (TONE_RANK[item.tone] > TONE_RANK[best] ? item.tone : best), "green");
    const last = pieces[pieces.length - 1];
    if (last && last.end === start && last.tone === tone) last.end = end;
    else pieces.push({ start, end, tone });
  }
  return pieces;
};

const createController = (panel, context, logTypeId) => {
  const store = context.viewerStore;
  const root = element("div", "acme-timeline");
  root.style.setProperty("--acme-label-w", `${LABEL_WIDTH}px`);
  panel.appendChild(root);

  let span = null;
  let groups = [];
  let shown = ALL;
  const collapsed = new Set();
  let lastTime = null;
  let refreshCommands = null;
  const off = [];

  const pct = (t) => (100 * (t - span.start)) / (span.end - span.start);
  const place = (node, start, end = null) => {
    node.style.left = `${pct(start)}%`;
    if (end != null) node.style.width = `${Math.max(0.15, pct(end) - pct(start))}%`;
    return node;
  };

  // Clicking empty track space moves the log to that time.
  const trackNode = () => {
    const track = element("div", "acme-tl-track");
    track.addEventListener("click", (event) => {
      const box = track.getBoundingClientRect();
      const fraction = Math.min(1, Math.max(0, (event.clientX - box.left) / box.width));
      jumpTo(context, { timeMs: span.start + fraction * (span.end - span.start) });
    });
    return track;
  };

  const axisRow = () => {
    const row = element("div", "acme-tl-row acme-tl-axis");
    row.appendChild(element("div", "acme-tl-label", "UTC"));
    const track = element("div", "acme-tl-track");
    const step = niceStep(span.end - span.start);
    for (let t = Math.ceil(span.start / step) * step; t <= span.end; t += step) {
      track.appendChild(place(element("span", "acme-tl-tick", formatClock(t)), t));
    }
    row.appendChild(track);
    return row;
  };

  const faultRow = (fault) => {
    const row = element("div", "acme-tl-row acme-tl-fault");
    row.dataset.fault = fault.key;
    const label = element("div", "acme-tl-label");
    label.append(
      element("span", "acme-tl-name", fault.name),
      element("span", "acme-tl-meta", [fault.code, fault.unit].filter(Boolean).join(" · "))
    );
    label.title = `${fault.name} (${[fault.system, fault.subsystem, fault.unit].filter(Boolean).join(":")})`;
    const track = trackNode();
    fault.intervals.forEach((interval) => {
      const end = intervalEnd(interval, span);
      const bar = place(element("span", `acme-tl-bar${interval.end == null ? " is-open" : ""}`), interval.start, end);
      bar.style.setProperty("--tone", TONE_COLORS[interval.tone] || TONE_COLORS.green);
      bar.title =
        `${fault.name}: ${interval.color || interval.tone}, set ${formatClock(interval.start)}, ` +
        (interval.end == null ? "still set at the end" : `cleared ${formatClock(interval.end)}`) +
        ` (${formatDuration(end - interval.start)})`;
      track.appendChild(bar);
    });
    fault.marks.forEach((mark) => {
      const node = place(element("button", `acme-tl-mark is-${mark.kind}`, mark.kind === "set" ? "▲" : "▽"), mark.t);
      node.type = "button";
      node.style.setProperty("--tone", TONE_COLORS[mark.tone] || TONE_COLORS.green);
      node.title = `${mark.kind === "set" ? "SET" : "CLEAR"} ${fault.name} at ${formatClock(mark.t)}`;
      node.addEventListener("click", (event) => {
        event.stopPropagation();
        jumpTo(context, { rowId: mark.rowId });
      });
      track.appendChild(node);
    });
    row.append(label, track);
    return row;
  };

  const sectionHeader = (group) => {
    const row = element("div", `acme-tl-row acme-tl-section${collapsed.has(group.id) ? " is-collapsed" : ""}`);
    row.dataset.group = group.id;
    const label = element("button", "acme-tl-label acme-tl-section-label");
    label.type = "button";
    label.title = "Show or hide this subsystem's faults";
    const text = element("span", "acme-tl-section-text");
    text.append(
      element("span", "acme-tl-name", group.subsystem ? `${group.system} / ${group.subsystem}` : group.system),
      element("span", "acme-tl-summary")
    );
    label.append(element("span", "acme-tl-caret", "▾"), text);
    label.addEventListener("click", () => {
      if (collapsed.has(group.id)) collapsed.delete(group.id);
      else collapsed.add(group.id);
      rebuild();
    });
    const track = trackNode();
    coverage(group.faults, span).forEach((piece) => {
      const strip = place(element("span", "acme-tl-strip"), piece.start, piece.end);
      strip.style.setProperty("--tone", TONE_COLORS[piece.tone]);
      track.appendChild(strip);
    });
    row.append(label, track);
    return row;
  };

  // Section summaries depend on the cursor ("n set now"), so they update with it.
  const updateSummaries = () => {
    root.querySelectorAll(".acme-tl-section").forEach((row) => {
      const group = groups.find((item) => item.id === row.dataset.group);
      if (!group) return;
      const sets = group.faults.reduce((sum, fault) => sum + fault.intervals.length, 0);
      const worst = group.faults.reduce((best, fault) => (!best || fault.worstRank > best.worstRank ? fault : best), null);
      const now =
        lastTime == null
          ? null
          : group.faults.filter((fault) => fault.intervals.some((interval) => isOpenAt(interval, lastTime))).length;
      row.querySelector(".acme-tl-summary").textContent = [
        `${group.faults.length} fault${group.faults.length === 1 ? "" : "s"}`,
        `${sets} set${sets === 1 ? "" : "s"}`,
        now == null ? null : `${now} set now`,
        worst ? `worst ${worst.worstColor || worst.worstTone}` : null,
      ]
        .filter(Boolean)
        .join(" · ");
    });
    root.querySelectorAll(".acme-tl-fault").forEach((row) => {
      const fault = groups.flatMap((group) => group.faults).find((item) => item.key === row.dataset.fault);
      row.classList.toggle("is-set-now", !!fault && lastTime != null && fault.intervals.some((i) => isOpenAt(i, lastTime)));
    });
  };

  const cursor = element("div", "acme-tl-cursor");
  const moveCursor = () => {
    if (!span || lastTime == null || lastTime < span.start || lastTime > span.end) {
      cursor.hidden = true;
    } else {
      cursor.hidden = false;
      cursor.style.left = `calc(var(--acme-label-w) + (100% - var(--acme-label-w)) * ${pct(lastTime) / 100})`;
    }
    updateSummaries();
  };

  const rebuild = () => {
    const events = (context.logData?.events || []).filter((event) => event.log_type === logTypeId);
    span = logSpan({
      events: context.logData?.events || [],
      tracks: store?.tracksSignal?.value || context.logData?.tracks || [],
    });
    groups = groupBySubsystem(buildFaults(events));
    if (shown !== ALL && !groups.some((group) => group.id === shown)) shown = ALL;
    root.innerHTML = "";
    if (!span || !groups.length) {
      root.appendChild(element("p", "acme-empty", "No faults in this log."));
      refreshCommands?.();
      return;
    }
    const body = element("div", "acme-tl-body");
    body.appendChild(axisRow());
    groups
      .filter((group) => shown === ALL || group.id === shown)
      .forEach((group) => {
        body.appendChild(sectionHeader(group));
        if (!collapsed.has(group.id)) group.faults.forEach((fault) => body.appendChild(faultRow(fault)));
      });
    body.appendChild(cursor);
    root.appendChild(body);
    moveCursor();
    refreshCommands?.();
  };

  if (store) {
    off.push(
      signals.effect(() => {
        store.allEventsSignal?.value; // live logs: rebuild when rows arrive
        rebuild();
      })
    );
    off.push(
      signals.effect(() => {
        lastTime = currentTime(store);
        moveCursor();
      })
    );
  } else {
    rebuild();
  }

  return {
    // Toolbar: pick one subsystem or all; expand or collapse every section.
    renderCommands(container) {
      const bar = element("div", "acme-tl-commands");
      const select = element("select", "text-input text-input-small");
      select.setAttribute("aria-label", "Subsystem");
      const toggle = element("button", "button button-ghost button-xs");
      toggle.type = "button";
      const refresh = () => {
        select.innerHTML = "";
        select.appendChild(new Option("All subsystems", ALL));
        groups.forEach((group) =>
          select.appendChild(new Option(group.subsystem ? `${group.system} / ${group.subsystem}` : group.system, group.id))
        );
        select.value = shown;
        toggle.textContent = collapsed.size ? "Expand all" : "Collapse all";
      };
      select.addEventListener("change", () => {
        shown = select.value;
        rebuild();
      });
      toggle.addEventListener("click", () => {
        if (collapsed.size) collapsed.clear();
        else groups.forEach((group) => collapsed.add(group.id));
        rebuild();
      });
      bar.append(select, toggle);
      container.appendChild(bar);
      refreshCommands = refresh;
      refresh();
      return () => {
        if (refreshCommands === refresh) refreshCommands = null;
      };
    },
    destroy() {
      off.forEach((unsubscribe) => unsubscribe());
    },
  };
};

export default function setup({ logTypeId, registerChart }) {
  registerChart({
    id: "acme-faults",
    label: "Fault Timeline",
    renderPanel(panel, context) {
      return createController(panel, context, logTypeId);
    },
    buildCommands(container, context) {
      return context.panelController?.renderCommands?.(container) || null;
    },
  });
}
