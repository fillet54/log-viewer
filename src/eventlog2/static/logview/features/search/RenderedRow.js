import { html, hooks } from "logview/lib";

const useLayoutEffect = hooks.useLayoutEffect || null;
const useRef = hooks.useRef || null;

export const createRenderedRow = ({ event, services, extraClasses = [] }) => {
    const rowTemplate = services?.rowTemplate || null;
    const renderRow = window.EventLog2?.resolveRowRenderer
      ? window.EventLog2.resolveRowRenderer(services?.plugin || null)
      : null;
    if (!rowTemplate || typeof renderRow !== "function") return null;
    return renderRow(event, rowTemplate, {
      extraClasses,
      bookmarks: services?.viewerStore || null,
      view: services?.view || null,
    });
};

const attachRowActions = ({ row, event, services, events, activityEnabled }) => {
    if (!row) return null;

    row.querySelector(".bookmark-toggle")?.addEventListener("click", (eventClick) => {
      if (!activityEnabled) return;
      eventClick.stopPropagation();
      const next = services?.viewerStore?.cycle(event.row_id) || 0;
      row.classList.toggle("is-bookmarked", next > 0);
      row.dataset.bookmarkColor = String(next);
    });

    row.querySelectorAll(".match-link").forEach((button) => {
      button.addEventListener("click", (eventClick) => {
        eventClick.stopPropagation();
        const linkedRowId = button.dataset.linkedRowId;
        if (!linkedRowId) return;
        const linkedEvent = events.find((entry) => String(entry.row_id) === String(linkedRowId)) || null;
        if (linkedEvent) {
          services?.viewerStore?.setSelectedEvent(linkedEvent);
        }
        services?.viewerStore?.setLogJump({ rowId: linkedRowId });
      });
    });

    row.addEventListener("click", () => {
      services?.viewerStore?.setSelectedEvent(event);
      services?.viewerStore?.setLogJump({ rowId: event.row_id });
    });

    return row;
};

export const RenderedRow = ({
    event,
    services,
    events,
    activityEnabled,
    version,
    bookmarkState,
    commentState,
    className = "",
  }) => {
    const ref = useRef();

    if (typeof useLayoutEffect === "function") {
      useLayoutEffect(() => {
        const host = ref.current;
        if (!host) return;
        host.innerHTML = "";
        const row = createRenderedRow({
          event,
          services,
          extraClasses: ["search-result-row"],
        });
        if (!row) return;
        const mountedRow = attachRowActions({ row, event, services, events, activityEnabled });
        if (mountedRow) host.appendChild(mountedRow);
      }, [event?.row_id, services, events, activityEnabled, version, bookmarkState, commentState]);
    }

    return html`<div ref=${ref} class=${className}></div>`;
};

window.EventLog2UI = window.EventLog2UI || {};
window.EventLog2UI.components = window.EventLog2UI.components || {};
window.EventLog2UI.utils = window.EventLog2UI.utils || {};
window.EventLog2UI.components.RenderedRow = RenderedRow;
window.EventLog2UI.utils.createRenderedRow = createRenderedRow;
