// "Overview": the log at a glance. Start, end and duration; rows per log type; fault facts (worst
// severity, peak, longest, most frequent, still open); a per-system table; and the tracks. Items with
// a time are links that move the log there. "Open now" follows the row selected in the log.
import { signals, TONE_COLORS } from "cinc";
import {
  buildFaults,
  currentTime,
  element,
  formatClock,
  formatDateTime,
  formatDuration,
  intervalEnd,
  isOpenAt,
  jumpTo,
  logSpan,
  timeOf,
} from "./model.js";

const EARTH_RADIUS_KM = 6371.0088;
const toRad = (deg) => (deg * Math.PI) / 180;
const distanceKm = (samples) => {
  let total = 0;
  for (let i = 1; i < samples.length; i += 1) {
    const [, lat1, lon1] = samples[i - 1];
    const [, lat2, lon2] = samples[i];
    const a =
      Math.sin(toRad(lat2 - lat1) / 2) ** 2 +
      Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(toRad(lon2 - lon1) / 2) ** 2;
    total += 2 * EARTH_RADIUS_KM * Math.asin(Math.min(1, Math.sqrt(a)));
  }
  return total;
};

const toneDot = (tone) => {
  const dot = element("span", "acme-tone-dot");
  dot.style.background = TONE_COLORS[tone] || TONE_COLORS.green;
  return dot;
};

// A fact card: label, value, optional detail; `onClick` makes the value a link.
const card = (label, value, detail = "", onClick = null, tone = null) => {
  const node = element("div", "acme-card");
  node.appendChild(element("div", "acme-card-label", label));
  const valueNode = element(onClick ? "button" : "div", "acme-card-value");
  if (onClick) {
    valueNode.type = "button";
    valueNode.addEventListener("click", onClick);
    valueNode.title = "Show in the log";
  }
  if (tone) valueNode.appendChild(toneDot(tone));
  valueNode.appendChild(document.createTextNode(value));
  node.appendChild(valueNode);
  if (detail) node.appendChild(element("div", "acme-card-detail", detail));
  return node;
};

const table = (headings, rows) => {
  const node = element("table", "acme-table");
  const head = node.createTHead().insertRow();
  headings.forEach((heading) => head.appendChild(element("th", null, heading)));
  const body = node.createTBody();
  rows.forEach((cells) => {
    const row = body.insertRow();
    cells.forEach((cell) => {
      const td = row.insertCell();
      if (cell instanceof Node) td.appendChild(cell);
      else td.textContent = cell;
    });
  });
  return node;
};

// Most faults open at once: sweep interval starts and ends in time order.
const peakOpen = (faults, span) => {
  const edges = [];
  faults.forEach((fault) =>
    fault.intervals.forEach((interval) => {
      edges.push([interval.start, 1]);
      edges.push([intervalEnd(interval, span), -1]);
    })
  );
  edges.sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  let open = 0;
  let best = { count: 0, t: null };
  edges.forEach(([t, delta]) => {
    open += delta;
    if (open > best.count) best = { count: open, t };
  });
  return best;
};

// Share of the log with at least one fault open.
const faultedShare = (faults, span) => {
  const intervals = faults
    .flatMap((fault) => fault.intervals.map((interval) => [interval.start, intervalEnd(interval, span)]))
    .sort((a, b) => a[0] - b[0]);
  let covered = 0;
  let current = null;
  intervals.forEach(([start, end]) => {
    if (!current || start > current[1]) {
      if (current) covered += current[1] - current[0];
      current = [start, end];
    } else current[1] = Math.max(current[1], end);
  });
  if (current) covered += current[1] - current[0];
  return covered / (span.end - span.start);
};

const render = (root, context, logTypeId) => {
  const events = context.logData?.events || [];
  const tracks = context.viewerStore?.tracksSignal?.value || context.logData?.tracks || [];
  const span = logSpan({ events, tracks });
  root.innerHTML = "";
  if (!span) {
    root.appendChild(element("p", "acme-empty", "No rows yet."));
    return null;
  }
  const faultEvents = events.filter((event) => event.log_type === logTypeId);
  const faults = buildFaults(faultEvents);
  const intervals = faults.flatMap((fault) => fault.intervals.map((interval) => ({ fault, interval })));
  const go = (options) => () => jumpTo(context, options);

  // When and how much
  const counts = new Map();
  events.forEach((event) => counts.set(event.log_type, (counts.get(event.log_type) || 0) + 1));
  const cards = element("div", "acme-cards");
  cards.append(
    card("Start", formatDateTime(span.start), "", go({ timeMs: span.start })),
    card("End", formatDateTime(span.end), "", go({ timeMs: span.end })),
    card("Duration", formatDuration(span.end - span.start)),
    card(
      "Rows",
      String(events.length),
      Array.from(counts, ([type, count]) => `${type} ${count}`).join(" · ")
    )
  );

  // Faults
  const sets = faultEvents.filter((event) => String(event.set_clear).toLowerCase() !== "clear");
  const worst = sets.reduce((best, event) => (!best || event.severity_rank > best.severity_rank ? event : best), null);
  const stillOpen = faults.filter((fault) => fault.intervals.some((interval) => interval.end == null));
  const peak = peakOpen(faults, span);
  const longest = intervals.reduce(
    (best, item) => {
      const length = intervalEnd(item.interval, span) - item.interval.start;
      return length > best.length ? { ...item, length } : best;
    },
    { length: -1 }
  );
  const frequent = faults.reduce((best, fault) => (!best || fault.intervals.length > best.intervals.length ? fault : best), null);
  const faultCards = element("div", "acme-cards");
  faultCards.append(
    card("Faults", String(faults.length), `${sets.length} sets · ${Math.round(100 * faultedShare(faults, span))}% of the log with a fault open`),
    worst
      ? card("Worst severity", worst.color, `${worst.name} at ${formatClock(timeOf(worst))}`, go({ rowId: worst.row_id }), worst.tone)
      : card("Worst severity", "—"),
    peak.count
      ? card("Most open at once", String(peak.count), `at ${formatClock(peak.t)}`, go({ timeMs: peak.t }))
      : card("Most open at once", "0"),
    longest.fault
      ? card(
          "Longest",
          formatDuration(longest.length),
          `${longest.fault.name}${longest.interval.end == null ? " (still set)" : ""}`,
          go({ rowId: longest.interval.setRowId }),
          longest.interval.tone
        )
      : card("Longest", "—"),
    frequent
      ? card("Most frequent", `${frequent.intervals.length}×`, frequent.name, null, frequent.worstTone)
      : card("Most frequent", "—"),
    card(
      "Still set at end",
      String(stillOpen.length),
      stillOpen.map((fault) => fault.name).join(", ") || "none",
      stillOpen.length ? go({ rowId: stillOpen[0].intervals.at(-1).setRowId }) : null
    )
  );
  const openNow = card("Open now", "—", "follows the selected row");
  faultCards.appendChild(openNow);

  // Per system
  const systems = new Map();
  faults.forEach((fault) => {
    const row = systems.get(fault.system) || { faults: 0, sets: 0, timeSet: 0, open: 0, worst: null, owner: "" };
    row.faults += 1;
    row.sets += fault.intervals.length;
    row.timeSet += fault.intervals.reduce((sum, interval) => sum + intervalEnd(interval, span) - interval.start, 0);
    row.open += fault.intervals.some((interval) => interval.end == null) ? 1 : 0;
    if (!row.worst || fault.worstRank > row.worst.worstRank) row.worst = fault;
    systems.set(fault.system, row);
  });
  faultEvents.forEach((event) => {
    if (event.owner && systems.has(event.system)) systems.get(event.system).owner = event.owner;
  });
  const worstCell = (fault) => {
    const cell = element("span", "acme-worst");
    cell.append(toneDot(fault.worstTone), document.createTextNode(fault.worstColor || fault.worstTone));
    return cell;
  };
  const systemTable = table(
    ["System", "Owner", "Faults", "Sets", "Time set", "Set at end", "Worst"],
    Array.from(systems, ([name, row]) => [
      name,
      row.owner || "—",
      String(row.faults),
      String(row.sets),
      formatDuration(row.timeSet),
      String(row.open),
      worstCell(row.worst),
    ])
  );

  root.append(element("h3", "acme-heading", "Log"), cards, element("h3", "acme-heading", "Faults"), faultCards);
  if (systems.size) root.append(element("h3", "acme-heading", "By system"), systemTable);

  if (tracks.length) {
    root.append(
      element("h3", "acme-heading", "Tracks"),
      table(
        ["Track", "Samples", "Distance", "Max altitude", "From", "To"],
        tracks.map((track) => {
          const samples = track.samples || [];
          return [
            track.label || track.id,
            String(samples.length),
            `${distanceKm(samples).toFixed(1)} km`,
            `${Math.round(Math.max(...samples.map((sample) => sample[3]))).toLocaleString("en-US")} ft`,
            samples.length ? formatClock(samples[0][0]) : "—",
            samples.length ? formatClock(samples[samples.length - 1][0]) : "—",
          ];
        })
      )
    );
  }

  // "Open now" changes with the selected row, without rebuilding the rest.
  return (t) => {
    const value = openNow.querySelector(".acme-card-value");
    const detail = openNow.querySelector(".acme-card-detail");
    if (t == null) {
      value.textContent = "—";
      return;
    }
    const open = intervals.filter(({ interval }) => isOpenAt(interval, t));
    value.textContent = String(open.length);
    detail.textContent = `at ${formatClock(t)}${open.length ? `: ${open.map(({ fault }) => fault.name).join(", ")}` : ""}`;
  };
};

export default function setup({ logTypeId, registerChart }) {
  registerChart({
    id: "acme-overview",
    label: "Overview",
    slots: ["main", "side"],
    renderPanel(panel, context) {
      const root = element("div", "acme-overview");
      panel.appendChild(root);
      const store = context.viewerStore;
      let updateOpenNow = null;
      let lastTime = null;
      const off = [];
      if (store) {
        // Rebuild when rows arrive (live logs); update "Open now" when the selection moves.
        off.push(
          signals.effect(() => {
            store.allEventsSignal?.value;
            store.tracksSignal?.value;
            updateOpenNow = render(root, context, logTypeId);
            updateOpenNow?.(lastTime);
          })
        );
        off.push(
          signals.effect(() => {
            lastTime = currentTime(store);
            updateOpenNow?.(lastTime);
          })
        );
      } else {
        updateOpenNow = render(root, context, logTypeId);
      }
      return { destroy: () => off.forEach((unsubscribe) => unsubscribe()) };
    },
  });
}
