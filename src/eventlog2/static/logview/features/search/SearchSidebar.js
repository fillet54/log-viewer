import { html } from "logview/lib";
import { SearchHistoryView } from "./SearchHistoryView.js";
import { SearchFilterView } from "./SearchFilterView.js";
import { SearchBookmarksView } from "./SearchBookmarksView.js";

export const SearchSidebar = ({
    currentTab,
    activityEnabled,
    splitLeftRef,
    pinned,
    history,
    filters,
    bookmarkEvents,
    services,
    events,
    comments,
    rowVersion,
    bookmarkState,
    commentState,
    onTogglePin,
    onPromoteFilter,
    onSelectQuery,
    onClearHistory,
    onToggleFilter,
    onRemoveFilter,
  }) => {
    return html`
      <aside id="search-history-pane" ref=${splitLeftRef} class="search-history">
        <${SearchHistoryView}
          currentTab=${currentTab}
          pinned=${pinned}
          history=${history}
          onTogglePin=${onTogglePin}
          onPromoteFilter=${onPromoteFilter}
          onSelectQuery=${onSelectQuery}
          onClearHistory=${onClearHistory}
        />
        <${SearchFilterView}
          currentTab=${currentTab}
          filters=${filters}
          onToggleFilter=${onToggleFilter}
          onRemoveFilter=${onRemoveFilter}
        />
        <${SearchBookmarksView}
          currentTab=${currentTab}
          activityEnabled=${activityEnabled}
          bookmarkEvents=${bookmarkEvents}
          services=${services}
          events=${events}
          comments=${comments}
          rowVersion=${rowVersion}
          bookmarkState=${bookmarkState}
          commentState=${commentState}
        />
      </aside>
    `;
  };
