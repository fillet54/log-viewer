import { html } from "logview/lib";
import { Cinc } from "../../../runtime.js";

export const EventSummary = ({ event, view }) => {
  const Component = Cinc.resolveDetailSummary(event?.log_type);
  if (Component) return html`<${Component} event=${event} view=${view} />`;
  return html`
    <div class="detail-title">${event?.name ?? event?.row_id}</div>
    <div class="detail-meta">${event?.time}</div>
  `;
};
