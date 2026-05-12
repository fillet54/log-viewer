(function () {
  const ui = window.EventLog2UI || {};
  const html = ui.html;

  ui.components = ui.components || {};

  const SearchFilterView = ({ currentTab, filters, onToggleFilter, onRemoveFilter }) => {
    const SEARCH_TAB_FILTERS = ui.constants?.SEARCH_TAB_FILTERS || "filters";
    const FilterItemRow = ui.components.FilterItemRow;

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

  ui.components.SearchFilterView = SearchFilterView;
})();
