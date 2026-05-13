import { html, hooks } from "logview/lib";

const useLayoutEffect = hooks.useLayoutEffect || null;
const useRef = hooks.useRef || null;

export const buildEventByRowId = (events) => {
    const map = new Map();
    events.forEach((event) => {
      map.set(String(event.row_id), event);
    });
    return map;
};

export const buildIndexByRowId = (events) => {
    const map = new Map();
    events.forEach((event, index) => {
      map.set(String(event.row_id), index);
    });
    return map;
};

export const findClosestIndexBySeconds = (events, targetSeconds) => {
    if (!events.length) return null;
    let lo = 0;
    let hi = events.length - 1;

    while (lo <= hi) {
      const mid = Math.floor((lo + hi) / 2);
      const seconds = events[mid].norm_time;
      if (seconds === targetSeconds) return mid;
      if (seconds < targetSeconds) lo = mid + 1;
      else hi = mid - 1;
    }

    if (lo >= events.length) return events.length - 1;
    if (hi < 0) return 0;
    return Math.abs(events[lo].norm_time - targetSeconds) < Math.abs(events[hi].norm_time - targetSeconds)
      ? lo
      : hi;
};

export const createRenderedLogRow = ({ event, services, selectedRowId }) => {
    const rowTemplate = services?.rowTemplate || null;
    const renderRow = window.EventLog2?.resolveRowRenderer
      ? window.EventLog2.resolveRowRenderer(services?.plugin || null)
      : null;

    if (!rowTemplate || typeof renderRow !== "function") return null;

    const row = renderRow(event, rowTemplate, {
      bookmarks: services?.viewerStore || null,
      view: services?.view || null,
    });

    if (!row) return null;
    row.classList.toggle("log-selected", String(selectedRowId ?? "") === String(event.row_id));
    return row;
};

const attachRenderedLogRow = ({ row, event, services, eventByRowId }) => {
    if (!row) return null;

    row.querySelector(".bookmark-toggle")?.addEventListener("click", (eventClick) => {
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
        const linkedEvent = eventByRowId.get(String(linkedRowId)) || null;
        if (linkedEvent) {
          services?.viewerStore?.setSelectedEvent(linkedEvent);
        }
        services?.viewerStore?.setLogJump({ rowId: linkedRowId });
      });
    });

    row.addEventListener("click", () => {
      services?.viewerStore?.setSelectedEvent(event);
    });

    return row;
};

export const MainLogRow = ({
    event,
    services,
    eventByRowId,
    selectedRowId,
    highlightRowId,
    highlightNonce,
    bookmarkState,
  }) => {
    const ref = useRef ? useRef(null) : { current: null };

    if (typeof useLayoutEffect === "function") {
      useLayoutEffect(() => {
        const host = ref.current;
        if (!host) return;

        host.innerHTML = "";
        const row = createRenderedLogRow({ event, services, selectedRowId });
        if (!row) return;

        if (String(highlightRowId ?? "") === String(event.row_id)) {
          row.classList.remove("log-highlight");
          void row.offsetWidth;
          row.classList.add("log-highlight");
        }

        const mountedRow = attachRenderedLogRow({
          row,
          event,
          services,
          eventByRowId,
        });

        if (mountedRow) host.appendChild(mountedRow);
      }, [
        event?.row_id,
        services,
        eventByRowId,
        selectedRowId,
        highlightRowId,
        highlightNonce,
        bookmarkState,
      ]);
    }

    return html`<div ref=${ref}></div>`;
};

export const MainLogContent = ({
    filteredEvents,
    rowStride,
    virtual,
    visibleItems,
    services,
    eventByRowId,
    selectedRowId,
    highlightState,
    bookmarkState,
    logListRef,
  }) => {
    if (!filteredEvents.length) {
      return html`
        <div id="log-spacer"></div>
        <div id="log-list" ref=${logListRef} class="mono-block">
          <div class="no-results">No Results</div>
        </div>
      `;
    }

    return html`
      <div id="log-spacer" style=${{ height: `${filteredEvents.length * rowStride}px` }}></div>
      <div
        id="log-list"
        ref=${logListRef}
        class="mono-block"
        style=${{ transform: `translateY(${virtual.offsetY}px)` }}
      >
        ${visibleItems.map(
          (event) => html`
            <${MainLogRow}
              event=${event}
              services=${services}
              eventByRowId=${eventByRowId}
              selectedRowId=${selectedRowId}
              highlightRowId=${highlightState.rowId}
              highlightNonce=${highlightState.nonce}
              bookmarkState=${bookmarkState}
            />
          `
        )}
      </div>
    `;
};

export const MainLogPane = ({
    logRegionRef,
    logBodyRef,
    measureRef,
    viewMode,
    filteredEvents,
    rowStride,
    virtual,
    visibleItems,
    services,
    eventByRowId,
    selectedRowId,
    highlightState,
    bookmarkState,
    logListRef,
  }) => {
    const VIEW_MODE_LIST = "list";
    const VIEW_MODE_CHART = "chart";

    return html`
      <section
        class="main-view-region main-view-log-region"
        id="log-region"
        ref=${logRegionRef}
        style=${viewMode === VIEW_MODE_LIST
          ? { flex: "1 1 auto" }
          : viewMode === VIEW_MODE_CHART
            ? { flex: "0 0 0" }
            : {}}
      >
        <div class="pane-body log-body" id="log-body" ref=${logBodyRef}>
          <${MainLogContent}
            filteredEvents=${filteredEvents}
            rowStride=${rowStride}
            virtual=${virtual}
            visibleItems=${visibleItems}
            services=${services}
            eventByRowId=${eventByRowId}
            selectedRowId=${selectedRowId}
            highlightState=${highlightState}
            bookmarkState=${bookmarkState}
            logListRef=${logListRef}
          />
        </div>
        <div ref=${measureRef} style=${{ position: "absolute", visibility: "hidden", pointerEvents: "none" }}></div>
      </section>
    `;
};
