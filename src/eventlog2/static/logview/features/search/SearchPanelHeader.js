import { html } from "logview/lib";

export const SEARCH_TAB_HISTORY = "history";
export const SEARCH_TAB_FILTERS = "filters";
export const SEARCH_TAB_BOOKMARKS = "bookmarks";

export const SearchPanelHeader = ({ currentTab, activityEnabled, onSelectTab }) => html`
    <div class="pane-header compact-header pane-header-spread">
      <span id="search-header-text" class="section-label">Search</span>
      <div id="search-header-tabs" class="search-tabs hidden">
        <button
          id="tab-history"
          class=${`button button-ghost button-xs search-tab${currentTab === SEARCH_TAB_HISTORY ? " is-active" : ""}`}
          onClick=${() => onSelectTab(SEARCH_TAB_HISTORY)}
        >
          Search History
        </button>
        <button
          id="tab-filters"
          class=${`button button-ghost button-xs search-tab${currentTab === SEARCH_TAB_FILTERS ? " is-active" : ""}`}
          onClick=${() => onSelectTab(SEARCH_TAB_FILTERS)}
        >
          Filters
        </button>
        <button
          id="tab-bookmarks"
          class=${`button button-ghost button-xs search-tab${currentTab === SEARCH_TAB_BOOKMARKS ? " is-active" : ""}${activityEnabled ? "" : " hidden"}`}
          onClick=${() => onSelectTab(SEARCH_TAB_BOOKMARKS)}
        >
          Bookmarks
        </button>
      </div>
      <button id="toggle-bottom" class="button button-ghost button-xs" title="Toggle search pane">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="tool-icon">
          <path stroke-linecap="round" stroke-linejoin="round" d="M6 10l6 6 6-6" />
        </svg>
      </button>
    </div>
`;
