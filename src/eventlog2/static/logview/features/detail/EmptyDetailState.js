import { html } from "logview/lib";

export const EmptyDetailState = () => html`<div class="empty-panel-message">Select a log event to view details.</div>`;

window.EventLog2UI = window.EventLog2UI || {};
window.EventLog2UI.components = window.EventLog2UI.components || {};
window.EventLog2UI.components.EmptyDetailState = EmptyDetailState;
