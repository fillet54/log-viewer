import { html } from "logview/lib";
import { formatZuluTime } from "../rows/PluginLogRow.js";

const Icon = ({ children }) => html`
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="tool-icon" aria-hidden="true">${children}</svg>
`;

// A row of mutually exclusive buttons; `items` are { id, label, title?, icon? }.
const Segmented = ({ label, items, active, onSelect, iconOnly = false }) => html`
  <div class=${`seg${iconOnly ? " seg-icons" : ""}`} role="group" aria-label=${label}>
    ${items.map((item) => html`
      <button
        type="button"
        class=${`seg-button${item.id === active ? " is-active" : ""}`}
        aria-pressed=${String(item.id === active)}
        title=${item.title || item.label}
        onClick=${() => onSelect?.(item.id)}
      >
        ${item.icon || null}
        ${iconOnly ? html`<span class="sr-only">${item.label}</span>` : item.label}
      </button>
    `)}
  </div>
`;

const LAYOUTS = [
  {
    id: "split",
    label: "Chart and log",
    icon: html`<${Icon}><rect x="4" y="5" width="16" height="5" rx="1" /><rect x="4" y="13" width="16" height="6" rx="1" /></${Icon}>`,
  },
  {
    id: "chart",
    label: "Chart only",
    icon: html`<${Icon}><path stroke-linecap="round" d="M4 19h16M7 16V9m5 7V5m5 11v-4" /></${Icon}>`,
  },
  {
    id: "list",
    label: "Log only",
    icon: html`<${Icon}><path stroke-linecap="round" d="M5 7h14M5 12h14M5 17h14" /></${Icon}>`,
  },
];

export const MainViewToolbar = ({
    chartTypes,
    timelineViews = [],
    activeTimelineView = "events",
    selectedChartType,
    commandBarRef,
    viewMode,
    lockToBottom,
    density = "compact",
    timeMode = "absolute",
    onToggleLockToBottom,
    onChartTypeChange,
    onTimelineViewChange,
    onViewModeChange,
    onDensityChange,
    onTimeModeChange,
    timeReference = null,
    onJumpToReference,
    onClearReference,
  }) => {
    const chartPanels = (chartTypes || []).filter(
      (type) => !String(type?.id || "").startsWith("timeline-")
    );

    return html`
      <div class="main-view-toolbar">
        ${chartPanels.length > 1
          ? html`<select
              id="chart-type-select"
              class="text-input text-input-small chart-type-select"
              aria-label="Chart"
              value=${selectedChartType}
              onChange=${(event) => onChartTypeChange(event.currentTarget.value)}
            >
              ${chartPanels.map((type) => html`<option value=${type.id}>${type.label}</option>`)}
            </select>`
          : null}
        ${selectedChartType === "timeline" && timelineViews.length && viewMode !== "list"
          ? html`<${Segmented}
              label="Timeline view"
              items=${timelineViews}
              active=${activeTimelineView}
              onSelect=${onTimelineViewChange}
            />`
          : null}
        <div id="chart-command-bar" ref=${commandBarRef} class="chart-command-bar"></div>
        <div class="toolbar-end">
          ${timeMode === "relative" && timeReference?.event
            ? html`<span class=${`time-ref${timeReference.pinned ? " is-pinned" : ""}`}>
                <button
                  type="button"
                  class="time-ref-jump"
                  title="Relative times count from this row. Click to go to it."
                  onClick=${onJumpToReference}
                >
                  <span class="time-ref-label">from</span>
                  ${timeReference.pinned ? formatZuluTime(timeReference.event) : "first row"}
                </button>
                ${timeReference.pinned
                  ? html`<button
                      type="button"
                      class="time-ref-clear"
                      title="Measure from the first row again"
                      aria-label="Clear time reference"
                      onClick=${onClearReference}
                    >×</button>`
                  : null}
              </span>`
            : null}
          <${Segmented}
            label="Time column"
            items=${[
              { id: "absolute", label: "UTC", title: "Show UTC times" },
              { id: "relative", label: "Relative", title: "Show seconds from the reference row (click a time to set it)" },
            ]}
            active=${timeMode}
            onSelect=${onTimeModeChange}
          />
          <${Segmented}
            label="Row density"
            iconOnly=${true}
            items=${[
              {
                id: "compact",
                label: "Compact rows",
                icon: html`<${Icon}><path stroke-linecap="round" d="M5 6h14M5 10h14M5 14h14M5 18h14" /></${Icon}>`,
              },
              {
                id: "comfortable",
                label: "Comfortable rows",
                icon: html`<${Icon}><path stroke-linecap="round" d="M5 7h14M5 12h14M5 17h14" /></${Icon}>`,
              },
            ]}
            active=${density}
            onSelect=${onDensityChange}
          />
          <button
            type="button"
            class=${`seg-button seg-solo${lockToBottom ? " is-active" : ""}`}
            title=${lockToBottom ? "Following new events (click to stop)" : "Follow new events"}
            onClick=${onToggleLockToBottom}
            aria-pressed=${String(!!lockToBottom)}
          >
            <${Icon}>
              <path stroke-linecap="round" stroke-linejoin="round" d="M12 5v10M8 11l4 4 4-4" />
              <path stroke-linecap="round" d="M6 19h12" stroke-dasharray=${lockToBottom ? null : "3 2"} />
            </${Icon}>
            <span class="sr-only">Follow new events</span>
          </button>
          <${Segmented}
            label="Layout"
            iconOnly=${true}
            items=${LAYOUTS}
            active=${viewMode}
            onSelect=${onViewModeChange}
          />
        </div>
      </div>
    `;
};
