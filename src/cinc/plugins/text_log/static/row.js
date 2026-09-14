import { html } from "htm/preact";
import { Cinc } from "static/runtime.js";
const TextRow = ({ event, selected, onSelect, onBookmark, relativeTime, onSetTimeReference, zuluTime, zuluTimeTitle, isTimeReference }) => html`
  <div class=${`log-line tl-row ${selected ? "log-selected" : ""}`} onClick=${() => onSelect?.(event)}>
    <button class="log-time-column" type="button" onClick=${(click) => { click.stopPropagation(); onSetTimeReference?.(); }} title=${zuluTimeTitle}>${zuluTime}</button>
    <button class=${`log-time-column log-relative-time${isTimeReference ? " is-time-reference" : ""}`} type="button" onClick=${(click) => { click.stopPropagation(); onSetTimeReference?.(); }} title="Set as time reference">${relativeTime == null ? "—" : `${relativeTime >= 0 ? "+" : ""}${relativeTime.toFixed(3)}s`}</button>
    <span class="tl-logger" title=${event?.logger || ""}>${event?.logger || "—"}</span>
    <span class=${`tl-level-badge tl-level-${String(event?.level || "INFO").toLowerCase()}`}>${event?.level || "INFO"}</span>
    <span class="tl-match-placeholder" aria-hidden="true"></span>
    <div class="tl-message">${event?.message || ""}</div>
    <button class="bookmark-toggle" onClick=${(click) => { click.stopPropagation(); onBookmark?.(event); }}>☆</button>
  </div>`;
Cinc.registerLogRowComponent("text_log", TextRow);
Cinc.registerDetailSummary("text_log", ({ event }) => html`<div class="detail-title">${event?.message}</div><div class="detail-meta">${event?.time} · ${event?.level} · ${event?.logger}</div>`);
