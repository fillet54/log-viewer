import { html } from "logview/lib";
import { SearchItemRow } from "./SearchItemRow.js";

export const SearchHistoryView = ({
    currentTab,
    pinned,
    history,
    onTogglePin,
    onPromoteFilter,
    onSelectQuery,
    onClearHistory,
  }) => {
    const SEARCH_TAB_HISTORY = "history";

    return html`
      <div id="search-history-view" class=${`search-view${currentTab === SEARCH_TAB_HISTORY ? "" : " hidden"}`}>
        <div class="search-section">
          <div class="search-section-title">Pinned</div>
          <div id="search-pinned" class="search-list search-list-compact">
            ${pinned.slice(0, 24).map(
              (item) => html`
                <${SearchItemRow}
                  item=${item}
                  isPinned=${true}
                  onPinToggle=${() => onTogglePin(item.query)}
                  onPromote=${() => onPromoteFilter(item.query)}
                  onSelect=${() => onSelectQuery(item.query)}
                />
              `
            )}
          </div>
        </div>
        <div class="search-section">
          <div class="search-section-title history-header">
            <span>History</span>
            <button id="clear-history" class="button button-ghost button-xs" onClick=${onClearHistory}>
              Clear
            </button>
          </div>
          <div id="search-history" class="search-list search-list-compact">
            ${history.slice(0, 50).map(
              (item) => html`
                <${SearchItemRow}
                  item=${item}
                  isPinned=${false}
                  onPinToggle=${() => onTogglePin(item.query)}
                  onPromote=${() => onPromoteFilter(item.query)}
                  onSelect=${() => onSelectQuery(item.query)}
                />
              `
            )}
          </div>
        </div>
      </div>
    `;
  };
