(function () {
  const ui = window.EventLog2UI || {};
  const html = ui.html;

  ui.components = ui.components || {};

  const SearchBookmarksView = ({
    currentTab,
    activityEnabled,
    bookmarkEvents,
    services,
    events,
    comments,
    rowVersion,
    bookmarkState,
    commentState,
  }) => {
    const SEARCH_TAB_BOOKMARKS = ui.constants?.SEARCH_TAB_BOOKMARKS || "bookmarks";
    const ActivityItem = ui.components.ActivityItem;

    return html`
      <div
        id="search-bookmark-view"
        class=${`search-view${currentTab === SEARCH_TAB_BOOKMARKS ? "" : " hidden"}${activityEnabled ? "" : " hidden"}`}
      >
        <div class="search-section">
          <div class="search-section-title">Bookmarks</div>
          <div id="search-bookmarks" class="search-list search-list-compact">
            ${bookmarkEvents.map(
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
            )}
          </div>
        </div>
      </div>
    `;
  };

  ui.components.SearchBookmarksView = SearchBookmarksView;
})();
