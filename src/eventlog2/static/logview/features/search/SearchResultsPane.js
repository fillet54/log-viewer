import { html } from "logview/lib";
import { SearchControls } from "./SearchControls.js";
import { SearchResultsContent } from "./SearchResultsContent.js";

export const SearchResultsPane = ({
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
