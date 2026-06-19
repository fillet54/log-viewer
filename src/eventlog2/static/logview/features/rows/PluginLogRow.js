import { html } from "logview/lib";
import { EventLog2 } from "../../../runtime.js";

const normalizePluginId = (plugin) =>
  plugin && typeof plugin === "object"
    ? String(plugin.id || "").trim()
    : String(plugin || "").trim();

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
  const pluginId = normalizePluginId(services?.plugin || services?.logData?.pluginId);
  const RowComponent = EventLog2.resolveRowComponent(pluginId);
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
      view=${services?.view || null}
      onSelect=${onSelect}
      onBookmark=${onBookmark}
      onJump=${onJump}
    />
  `;
};
