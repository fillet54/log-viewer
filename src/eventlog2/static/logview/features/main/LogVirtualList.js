import { html } from "logview/lib";
import { PluginLogRow } from "../rows/PluginLogRow.js";

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

export const MainLogRow = ({
    event,
    services,
    eventByRowId,
    selectedRowId,
    highlightRowId,
    highlightNonce,
    bookmarkState,
  }) => {
    return html`
      <${PluginLogRow}
        key=${`${event?.row_id ?? ""}:${highlightNonce}`}
        event=${event}
        services=${services}
        eventByRowId=${eventByRowId}
        selected=${String(selectedRowId ?? "") === String(event?.row_id ?? "")}
        highlighted=${String(highlightRowId ?? "") === String(event?.row_id ?? "")}
        highlightNonce=${highlightNonce}
      />
    `;
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
        <div ref=${measureRef} style=${{ position: "absolute", visibility: "hidden", pointerEvents: "none" }}>
          ${filteredEvents[0]
            ? html`<${PluginLogRow} event=${filteredEvents[0]} services=${services} />`
            : null}
        </div>
      </section>
    `;
};
