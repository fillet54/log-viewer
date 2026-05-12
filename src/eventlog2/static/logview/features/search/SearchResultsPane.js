(function () {
  const ui = window.EventLog2UI || {};
  const html = ui.html;

  ui.components = ui.components || {};

  const SearchResultsPane = ({
    splitRightRef,
    query,
    queryInputRef,
    onQueryInput,
    onQueryKeyDown,
    onOpenHelp,
    onRunSearch,
    resultsRef,
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
    measureRef,
  }) => {
    const SearchControls = ui.components.SearchControls;
    const SearchResultsContent = ui.components.SearchResultsContent;

    return html`
      <section id="search-results-pane" ref=${splitRightRef} class="search-results">
        <${SearchControls}
          query=${query}
          queryInputRef=${queryInputRef}
          onQueryInput=${onQueryInput}
          onQueryKeyDown=${onQueryKeyDown}
          onOpenHelp=${onOpenHelp}
          onRunSearch=${onRunSearch}
        />
        <div id="search-results" ref=${resultsRef} class="search-list search-results-list">
          <${SearchResultsContent}
            currentTab=${currentTab}
            results=${results}
            resultRowStride=${resultRowStride}
            virtual=${virtual}
            visibleItems=${visibleItems}
            services=${services}
            events=${events}
            comments=${comments}
            activityEnabled=${activityEnabled}
            rowVersion=${rowVersion}
            bookmarkState=${bookmarkState}
            commentState=${commentState}
          />
        </div>
        <div ref=${measureRef} style=${{ position: "absolute", visibility: "hidden", pointerEvents: "none" }}></div>
      </section>
    `;
  };

  ui.components.SearchResultsPane = SearchResultsPane;
})();
