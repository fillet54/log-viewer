import { html } from "cinc";

// Severity drives the row's left stripe and the level badge's colour; INFO and DEBUG stay neutral.
const LEVEL_TONE = { WARN: "warn", WARNING: "warn", ERROR: "error", FATAL: "fatal", CRITICAL: "fatal" };

const TextRow = ({
  event,
  selected = false,
  highlighted = false,
  bookmarkColor = 0,
  extraClasses = [],
  onSelect,
  onBookmark,
  onSetTimeReference,
  timeText = "",
  timeTitle = "",
  isTimeReference = false,
}) => {
  const level = String(event?.level || "INFO").toUpperCase();
  const tone = LEVEL_TONE[level] || "";
  const className = [
    "log-line tl-row",
    tone ? `sev-${tone}` : "",
    level === "DEBUG" ? "is-quiet" : "",
    bookmarkColor ? "is-bookmarked" : "",
    selected ? "log-selected" : "",
    highlighted ? "log-highlight" : "",
    ...(Array.isArray(extraClasses) ? extraClasses : []),
  ].filter(Boolean).join(" ");
  return html`
    <div class=${className} data-row-id=${event?.row_id} data-bookmark-color=${String(bookmarkColor || 0)} onClick=${() => onSelect?.(event)}>
      <button
        class=${`log-time-column${isTimeReference ? " is-time-reference" : ""}`}
        type="button"
        title=${timeTitle}
        onClick=${(click) => { click.stopPropagation(); onSetTimeReference?.(); }}
      >${timeText}</button>
      <span class="tl-logger" title=${event?.logger || ""}>${event?.logger || "—"}</span>
      <span class=${`row-level row-level-${tone || (level === "DEBUG" ? "debug" : "info")}`} title=${level}>${level}</span>
      <span class="tl-message" title=${event?.message || ""}>${event?.message || ""}</span>
      <span></span>
      <button
        type="button"
        class="bookmark-toggle"
        title="Toggle bookmark"
        onClick=${(click) => { click.stopPropagation(); onBookmark?.(event); }}
      >
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6">
          <path stroke-linecap="round" stroke-linejoin="round" d="M6 4h12v16l-6-3-6 3z" />
        </svg>
      </button>
    </div>`;
};

export default function setup({ registerRow, registerDetail }) {
  registerRow(TextRow);
  registerDetail(({ event }) => html`<div class="detail-title">${event?.message}</div><div class="detail-meta">${event?.time} · ${event?.level} · ${event?.logger}</div>`);
}
