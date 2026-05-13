import { html } from "logview/lib";

export const DetailPanelHeader = () => html`
  <div class="pane-header pane-header-spread">
    <div class="pane-header-title">
      <span class="icon">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
          <path stroke-linecap="round" stroke-linejoin="round" d="M3 12h6l3-3 6 6" />
        </svg>
      </span>
      Insights
    </div>
    <button id="toggle-detail" class="button button-ghost button-xs" title="Hide info pane" aria-label="Hide info pane">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" class="tool-icon">
        <path stroke-linecap="round" stroke-linejoin="round" d="M9 6l6 6-6 6" />
      </svg>
    </button>
  </div>
`;

window.EventLog2UI = window.EventLog2UI || {};
window.EventLog2UI.components = window.EventLog2UI.components || {};
window.EventLog2UI.components.DetailPanelHeader = DetailPanelHeader;
