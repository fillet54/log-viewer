import { html } from "logview/lib";
import { Cinc } from "../../../runtime.js";

const normalizePluginId = (plugin) =>
  plugin && typeof plugin === "object"
    ? String(plugin.id || "").trim()
    : String(plugin || "").trim();

export const formatZuluTime = (event) => {
  const value = event?.time || event?.utctime || "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  // The page says UTC once; rows drop the date and the Z.
  return date.toISOString().replace(/^\d{4}-\d{2}-\d{2}T/, "").replace(/Z$/, "");
};

const formatRelative = (seconds) =>
  seconds == null ? "—" : `${seconds >= 0 ? "+" : "−"}${Math.abs(seconds).toFixed(3)}s`;

// Rows of a log type with no row component of its own: time, type, then the first text-like field.
const TEXT_FIELDS = ["message", "name", "text", "description", "summary"];
const GenericRow = ({ event, view, selected, highlighted, bookmarkColor, extraClasses, onSelect, onBookmark, onSetTimeReference, timeText, timeTitle, isTimeReference }) => {
  const field = TEXT_FIELDS.find((key) => typeof event?.[key] === "string" && event[key]);
  const { row_id: _rowId, time: _time, log_type: _logType, ...rest } = event || {};
  const text = field ? event[field] : JSON.stringify(rest);
  const className = [
    "log-line generic-row",
    bookmarkColor ? "is-bookmarked" : "",
    selected ? "log-selected" : "",
    highlighted ? "log-highlight" : "",
    ...(extraClasses || []),
  ].filter(Boolean).join(" ");
  return html`
    <div class=${className} data-row-id=${event?.row_id} data-bookmark-color=${String(bookmarkColor || 0)} onClick=${() => onSelect?.(event)}>
      <button
        class=${`log-time-column${isTimeReference ? " is-time-reference" : ""}`}
        type="button"
        title=${timeTitle}
        onClick=${(click) => { click.stopPropagation(); onSetTimeReference?.(); }}
      >${timeText}</button>
      <span class="generic-row-type" title=${event?.log_type || ""}>${view?.name || event?.log_type || ""}</span>
      <span></span>
      <span class="generic-row-text" title=${text}>${text}</span>
      <span></span>
      <button type="button" class="bookmark-toggle" title="Toggle bookmark" onClick=${(click) => { click.stopPropagation(); onBookmark?.(event); }}>
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6">
          <path stroke-linecap="round" stroke-linejoin="round" d="M6 4h12v16l-6-3-6 3z" />
        </svg>
      </button>
    </div>`;
};

export const PluginLogRow = ({
  event,
  services,
  events = [],
  eventByRowId = null,
  selected = false,
  highlighted = false,
  highlightNonce = 0,
  extraClasses = [],
  activityEnabled = true,
  jumpOnSelect = false,
}) => {
  const pluginId = String(event?.log_type || "");
  const RowComponent = Cinc.resolveRowComponent(pluginId);
  const viewerStore = services?.viewerStore || null;
  const bookmarkColor = viewerStore?.getColor?.(event?.row_id) || 0;
  const classList = Array.isArray(extraClasses) ? extraClasses.filter(Boolean) : [];
  const timeMode = viewerStore?.timeMode?.value === "relative" ? "relative" : "absolute";
  const relativeTime = viewerStore?.getRelativeTime?.(event);
  // Read the signal so rows re-render when the reference changes.
  void viewerStore?.timeReferenceRowId?.value;
  const isTimeReference = !!viewerStore?.isTimeReference?.(event);
  // One time column: absolute or relative to the reference row (the toolbar switches).
  const timeText = timeMode === "relative" ? formatRelative(relativeTime) : formatZuluTime(event);
  const timeTitle =
    timeMode === "relative"
      ? `${event?.time || ""} · relative to the reference row (click to ${isTimeReference ? "reset to the first row" : "make this the reference"})`
      : `${event?.time || ""} (click to measure times from this row)`;

  const resolveEventByRowId = (rowId) => {
    if (eventByRowId?.get) return eventByRowId.get(String(rowId)) || null;
    return events.find((entry) => String(entry?.row_id) === String(rowId)) || null;
  };

  const onSelect = (selectedEvent = event) => {
    if (!selectedEvent) return;
    // Clicking the selected row again clears the selection, so charts follow the scroll again.
    const current = viewerStore?.selectedEvent?.value;
    if (current && String(current.row_id) === String(selectedEvent.row_id)) {
      viewerStore?.setSelectedEvent?.(null);
      return;
    }
    viewerStore?.setSelectedEvent?.(selectedEvent);
    if (jumpOnSelect) viewerStore?.setLogJump?.({ rowId: selectedEvent.row_id });
  };

  const onBookmark = (bookmarkEvent = event) => {
    if (!activityEnabled || !bookmarkEvent) return 0;
    return viewerStore?.cycle?.(bookmarkEvent.row_id) || 0;
  };

  const onJump = (rowId) => {
    if (rowId == null || rowId === "") return;
    const linkedEvent = resolveEventByRowId(rowId);
    if (linkedEvent) viewerStore?.setSelectedEvent?.(linkedEvent);
    viewerStore?.setLogJump?.({ rowId });
  };

  return html`
    <${RowComponent || GenericRow}
      event=${event}
      selected=${selected}
      highlighted=${highlighted}
      highlightNonce=${highlightNonce}
      bookmarkColor=${bookmarkColor}
      extraClasses=${classList}
      view=${services?.logTypes?.[pluginId] || services?.view || null}
      onSelect=${onSelect}
      onBookmark=${onBookmark}
      onJump=${onJump}
      timeReferenceRowId=${viewerStore?.timeReferenceRowId?.value}
      relativeTime=${relativeTime}
      onSetTimeReference=${() => {
        viewerStore?.setTimeReference?.(event);
        // Picking a reference only shows up in relative mode, so switch to it.
        if (timeMode !== "relative") viewerStore?.setTimeMode?.("relative");
      }}
      zuluTime=${formatZuluTime(event)}
      zuluTimeTitle=${event?.time || event?.utctime || ""}
      timeText=${timeText}
      timeTitle=${timeTitle}
      timeMode=${timeMode}
      isTimeReference=${isTimeReference}
    />
  `;
};
