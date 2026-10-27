import { html } from "logview/lib";
import { SearchResultsContent } from "./SearchResultsContent.js";
import { PluginLogRow } from "../rows/PluginLogRow.js";

export const SearchResultsPane = ({
    splitRightRef,
    emptyMessage,
    resultsRef,
    currentTab,
    results,
    resultRowStride,
    offsets,
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
        <div id="search-results" ref=${resultsRef} class="search-list search-results-list">
          <${SearchResultsContent}
            currentTab=${currentTab}
            emptyMessage=${emptyMessage}
            results=${results}
            resultRowStride=${resultRowStride}
            offsets=${offsets}
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
        <div ref=${measureRef} style=${{ position: "absolute", visibility: "hidden", pointerEvents: "none" }}>
          ${events[0]
            ? html`
                <${PluginLogRow}
                  event=${events[0]}
                  services=${services}
                  events=${events}
                  extraClasses=${["search-result-row"]}
                  activityEnabled=${activityEnabled}
                />
              `
            : null}
        </div>
      </section>
    `;
  };
