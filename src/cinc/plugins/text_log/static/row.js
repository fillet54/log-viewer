import { html } from "htm/preact";
import { Cinc } from "static/runtime.js";
const TextRow = ({ event, selected, onSelect, onBookmark }) => html`
  <div class=${`log-line tl-row tl-level-${String(event?.level || "INFO").toLowerCase()} ${selected ? "log-selected" : ""}`} onClick=${() => onSelect?.(event)}>
    <div><div class="tl-message">${event?.message || ""}</div><div class="tl-meta">${event?.time} · ${event?.level} · ${event?.logger}</div></div>
    <button class="bookmark-toggle" onClick=${(click) => { click.stopPropagation(); onBookmark?.(event); }}>☆</button>
  </div>`;
Cinc.registerLogRowComponent("text_log", TextRow);
Cinc.registerDetailSummary("text_log", ({ event }) => html`<div class="detail-title">${event?.message}</div><div class="detail-meta">${event?.time} · ${event?.level} · ${event?.logger}</div>`);
