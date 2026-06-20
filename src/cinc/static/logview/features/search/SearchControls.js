import { html } from "logview/lib";

export const SearchControls = ({
    query,
    queryInputRef,
    onQueryInput,
    onQueryKeyDown,
    onOpenHelp,
    onRunSearch,
  }) => html`
    <div class="search-controls">
      <input
        id="search-query"
        ref=${queryInputRef}
        class="text-input text-input-small search-input"
        placeholder="Search logs, faults, codes..."
        value=${query}
        onInput=${onQueryInput}
        onKeyDown=${onQueryKeyDown}
      />
      <button
        type="button"
        id="open-search-help"
        class="button button-ghost button-small search-help-button"
        aria-label="Search syntax help"
        title="Search syntax help"
        onClick=${onOpenHelp}
      >
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true">
          <path stroke-linecap="round" stroke-linejoin="round" d="M9.5 9a2.5 2.5 0 1 1 4.2 1.8c-.8.6-1.2 1-1.2 2.2" />
          <path stroke-linecap="round" stroke-linejoin="round" d="M12 17h.01" />
          <circle cx="12" cy="12" r="9" />
        </svg>
      </button>
      <button id="run-search" class="button button-primary button-small" onClick=${onRunSearch}>
        Search
      </button>
    </div>
`;
