import { html } from "logview/lib";
import { ActivityItem } from "./ActivityItem.js";

export const SearchBookmarksView = ({
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
    const SEARCH_TAB_BOOKMARKS = "bookmarks";

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

window.EventLog2UI = window.EventLog2UI || {};
window.EventLog2UI.components = window.EventLog2UI.components || {};
window.EventLog2UI.components.SearchBookmarksView = SearchBookmarksView;
