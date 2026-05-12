(function () {
  const ui = window.EventLog2UI || {};
  const html = ui.html;

  ui.components = ui.components || {};

  const SearchSidebar = ({
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
    const SearchHistoryView = ui.components.SearchHistoryView;
    const SearchFilterView = ui.components.SearchFilterView;
    const SearchBookmarksView = ui.components.SearchBookmarksView;

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

  ui.components.SearchSidebar = SearchSidebar;
})();
