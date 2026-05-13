import { html } from "logview/lib";

export const SearchItemRow = ({ item, isPinned, onPinToggle, onPromote, onSelect }) => html`
    <div class="search-item search-history-item" onClick=${onSelect}>
      <span class="search-query">${item.label}</span>
      <button
        type="button"
        class=${`pin-button${isPinned ? " is-pinned" : ""}`}
        title=${isPinned ? "Unpin" : "Pin"}
        onClick=${(event) => {
          event.stopPropagation();
          onPinToggle();
        }}
      >
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7">
          <path stroke-linecap="round" stroke-linejoin="round" d="M9 3h6l-1 6 3 3-1.5 1.5L12 10l-3.5 3.5L7 12l3-3-1-6Z" />
          <path stroke-linecap="round" stroke-linejoin="round" d="M12 10v9" />
        </svg>
      </button>
      <button
        type="button"
        class="pin-button promote-button"
        title="Promote to filter"
        onClick=${(event) => {
          event.stopPropagation();
          onPromote();
        }}
      >
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7">
          <path stroke-linecap="round" stroke-linejoin="round" d="M4 5h16l-6 7v6l-4 2v-8L4 5Z" />
        </svg>
      </button>
      <span class="search-time">${item.count}</span>
    </div>
`;
