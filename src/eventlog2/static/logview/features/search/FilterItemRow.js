import { html } from "logview/lib";

export const FilterItemRow = ({ filter, onToggle, onRemove }) => html`
    <div class="search-item search-filter-item">
      <span>${filter.query}</span>
      <button
        type="button"
        class=${`filter-toggle${filter.enabled ? " is-on" : ""}`}
        aria-pressed=${String(filter.enabled)}
        onClick=${onToggle}
      ></button>
      <button type="button" class="pin-button" title="Remove filter" onClick=${onRemove}>
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7">
          <path stroke-linecap="round" stroke-linejoin="round" d="M6 6l12 12M18 6l-12 12" />
        </svg>
      </button>
    </div>
`;
