import { html, Fragment } from "logview/lib";

export const EventSummary = ({ event }) => html`
  <${Fragment}>
    <div class="detail-title">${event.name}</div>
    <div class="detail-meta">${event.utctime} • ${event.set_clear}</div>
    <div class="detail-summary">${event.description}</div>
    <div class="detail-path">${event.system}/${event.subsystem}/${event.unit}/${event.code}</div>
  </${Fragment}>
`;
