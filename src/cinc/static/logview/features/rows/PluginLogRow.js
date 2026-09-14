import { html } from "logview/lib";
import { Cinc } from "../../../runtime.js";

const normalizePluginId = (plugin) =>
  plugin && typeof plugin === "object"
    ? String(plugin.id || "").trim()
    : String(plugin || "").trim();

const formatZuluTime = (event) => {
  const value = event?.time || event?.utctime || "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toISOString().replace(/^\d{4}-\d{2}-\d{2}T/, "");
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

  const resolveEventByRowId = (rowId) => {
    if (eventByRowId?.get) return eventByRowId.get(String(rowId)) || null;
    return events.find((entry) => String(entry?.row_id) === String(rowId)) || null;
  };

  const onSelect = (selectedEvent = event) => {
    if (!selectedEvent) return;
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

  if (!RowComponent) {
    return html`
      <div class="log-line log-row-error ${classList.join(" ")}" data-row-id=${event?.row_id ?? ""}>
        Missing row component for ${pluginId || "unknown plugin"}.
      </div>
    `;
  }

  return html`
    <${RowComponent}
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
      relativeTime=${viewerStore?.getRelativeTime?.(event)}
      onSetTimeReference=${() => viewerStore?.setTimeReference?.(event)}
      zuluTime=${formatZuluTime(event)}
      zuluTimeTitle=${event?.time || event?.utctime || ""}
      isTimeReference=${String(viewerStore?.timeReferenceRowId?.value) === String(event?.row_id)}
    />
  `;
};
