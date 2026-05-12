(function () {
  const ui = window.EventLog2UI || {};
  const html = ui.html;

  ui.components = ui.components || {};

  const SearchResultsContent = ({
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
    const SEARCH_TAB_BOOKMARKS = ui.constants?.SEARCH_TAB_BOOKMARKS || "bookmarks";
    const ActivityItem = ui.components.ActivityItem;
    const RenderedRow = ui.components.RenderedRow;

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

  ui.components.SearchResultsContent = SearchResultsContent;
})();
