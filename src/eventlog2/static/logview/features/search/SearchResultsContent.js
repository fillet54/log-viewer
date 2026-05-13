import { html } from "logview/lib";
import { ActivityItem } from "./ActivityItem.js";
import { RenderedRow } from "./RenderedRow.js";

export const SearchResultsContent = ({
    currentTab,
    results,
    resultRowStride,
    virtual,
    visibleItems,
    services,
    events,
    comments,
    activityEnabled,
    rowVersion,
    bookmarkState,
    commentState,
  }) => {
    const SEARCH_TAB_BOOKMARKS = "bookmarks";

    if (currentTab === SEARCH_TAB_BOOKMARKS) {
      return html`
        <div id="search-results-spacer"></div>
        <div id="search-results-list" class="mono-block">
          ${results.length
            ? results.map(
                (event) => html`
                  <${ActivityItem}
                    event=${event}
                    services=${services}
                    events=${events}
                    comments=${comments}
                    activityEnabled=${activityEnabled}
                    version=${rowVersion}
                    bookmarkState=${bookmarkState}
                    commentState=${commentState}
                  />
                `
              )
            : html`<div class="no-results">No Results</div>`}
        </div>
      `;
    }

    return html`
      <div id="search-results-spacer" style=${{ height: `${results.length * resultRowStride}px` }}></div>
      <div
        id="search-results-list"
        class="mono-block"
        style=${{ transform: `translateY(${virtual.startIndex * resultRowStride}px)` }}
      >
        ${results.length
          ? visibleItems.map(
              (event) => html`
                <${RenderedRow}
                  event=${event}
                  services=${services}
                  events=${events}
                  activityEnabled=${activityEnabled}
                  version=${rowVersion}
                  bookmarkState=${bookmarkState}
                  commentState=${commentState}
                />
              `
            )
          : html`<div class="no-results">No Results</div>`}
      </div>
    `;
  };

window.EventLog2UI = window.EventLog2UI || {};
window.EventLog2UI.components = window.EventLog2UI.components || {};
window.EventLog2UI.components.SearchResultsContent = SearchResultsContent;
