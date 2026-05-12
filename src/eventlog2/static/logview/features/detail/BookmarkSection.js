(function () {
  const ui = window.EventLog2UI || {};
  const html = ui.html;

  ui.components = ui.components || {};

  const BookmarkSection = ({ event, colorIndex, onSetColor }) => html`
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

  ui.components.BookmarkSection = BookmarkSection;
})();
