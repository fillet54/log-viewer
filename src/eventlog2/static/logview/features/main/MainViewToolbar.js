import { html } from "logview/lib";

export const ViewModeButton = ({ id, active, title, label, icon, onClick }) => html`
    <button
      id=${id}
      class=${`button button-ghost button-xs view-mode-button${active ? " is-active" : ""}`}
      role="tab"
      aria-selected=${String(active)}
      title=${title}
      onClick=${onClick}
    >
      ${icon}
      <span class="sr-only">${label}</span>
    </button>
`;

export const MainViewToolbar = ({
    chartTypes,
    selectedChartType,
    commandBarRef,
    viewMode,
    lockToBottom,
    onToggleLockToBottom,
    onChartTypeChange,
    onViewModeChange,
  }) => {
    const VIEW_MODE_SPLIT = "split";
    const VIEW_MODE_CHART = "chart";
    const VIEW_MODE_LIST = "list";

    return html`
      <div class="main-view-toolbar">
        <div class="chart-type-picker">
          <select
            id="chart-type-select"
            class="text-input text-input-small chart-type-select"
            aria-label="Chart type"
            value=${selectedChartType}
            disabled=${chartTypes.length === 0}
            onChange=${(event) => onChartTypeChange(event.currentTarget.value)}
          >
            ${chartTypes.map(
              (type) => html`<option value=${type.id}>${type.label}</option>`
            )}
          </select>
        </div>
        <div id="chart-command-bar" ref=${commandBarRef} class="chart-command-bar"></div>
        <button
          type="button"
          class=${`button button-ghost button-xs${lockToBottom ? " is-active" : ""}`}
          title=${lockToBottom ? "Unlock scroll (following new events)" : "Lock to bottom (follow new events)"}
          onClick=${onToggleLockToBottom}
          aria-pressed=${String(!!lockToBottom)}
        >
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="tool-icon" aria-hidden="true">
            <path stroke-linecap="round" stroke-linejoin="round" d="M12 5v10M8 11l4 4 4-4" />
            ${lockToBottom
              ? html`<path stroke-linecap="round" stroke-linejoin="round" d="M6 19h12" />`
              : html`<path stroke-linecap="round" stroke-linejoin="round" d="M6 19h12" stroke-dasharray="3 2" />`
            }
          </svg>
          <span class="sr-only">${lockToBottom ? "Unlock scroll" : "Lock to bottom"}</span>
        </button>
        <div class="view-mode-toggle" role="tablist" aria-label="Main view layout">
          <${ViewModeButton}
            id="view-mode-split"
            active=${viewMode === VIEW_MODE_SPLIT}
            title="Show chart and log"
            label="Chart and log"
            onClick=${() => onViewModeChange(VIEW_MODE_SPLIT)}
            icon=${html`
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="tool-icon" aria-hidden="true">
                <rect x="4" y="5" width="16" height="6" rx="1.5" />
                <rect x="4" y="13" width="16" height="6" rx="1.5" />
              </svg>
            `}
          />
          <${ViewModeButton}
            id="view-mode-chart"
            active=${viewMode === VIEW_MODE_CHART}
            title="Show chart only"
            label="Chart only"
            onClick=${() => onViewModeChange(VIEW_MODE_CHART)}
            icon=${html`
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="tool-icon" aria-hidden="true">
                <path stroke-linecap="round" stroke-linejoin="round" d="M4 19h16" />
                <path stroke-linecap="round" stroke-linejoin="round" d="M7 16V9m5 7V5m5 11v-4" />
              </svg>
            `}
          />
          <${ViewModeButton}
            id="view-mode-list"
            active=${viewMode === VIEW_MODE_LIST}
            title="Show log only"
            label="Log only"
            onClick=${() => onViewModeChange(VIEW_MODE_LIST)}
            icon=${html`
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="tool-icon" aria-hidden="true">
                <path stroke-linecap="round" stroke-linejoin="round" d="M7 7h13M7 12h13M7 17h13" />
                <circle cx="4" cy="7" r="1" fill="currentColor" stroke="none" />
                <circle cx="4" cy="12" r="1" fill="currentColor" stroke="none" />
                <circle cx="4" cy="17" r="1" fill="currentColor" stroke="none" />
              </svg>
            `}
          />
        </div>
      </div>
    `;
};
