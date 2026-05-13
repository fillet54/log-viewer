import { html } from "logview/lib";
import { FilterItemRow } from "./FilterItemRow.js";

export const SearchFilterView = ({ currentTab, filters, onToggleFilter, onRemoveFilter }) => {
    const SEARCH_TAB_FILTERS = "filters";

    return html`
      <div id="search-filter-view" class=${`search-view${currentTab === SEARCH_TAB_FILTERS ? "" : " hidden"}`}>
        <div class="search-section">
          <div class="search-section-title">Filters</div>
          <div id="search-filters" class="search-list search-list-compact">
            ${filters.map(
              (filter) => html`
                <${FilterItemRow}
                  filter=${filter}
                  onToggle=${() => onToggleFilter(filter.query)}
                  onRemove=${() => onRemoveFilter(filter.query)}
                />
              `
            )}
          </div>
        </div>
      </div>
    `;
  };
