import { html } from "logview/lib";

export const BookmarkSection = ({ event, colorIndex, onSetColor }) => html`
  <div class="bookmark-notes">
    <div class="section-label">Bookmark Color</div>
    <div class="bookmark-colors" data-row-id=${event.row_id}>
      ${[1, 2, 3, 4, 5].map(
        (idx) => html`
          <button
            type="button"
            class=${`bookmark-color${colorIndex === idx ? " is-active" : ""}`}
            data-color=${idx}
            title=${`Color ${idx}`}
            onClick=${() => onSetColor(idx)}
          ></button>
        `
      )}
    </div>
  </div>
`;

window.EventLog2UI = window.EventLog2UI || {};
window.EventLog2UI.components = window.EventLog2UI.components || {};
window.EventLog2UI.components.BookmarkSection = BookmarkSection;
