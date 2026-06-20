import { html } from "htm/preact";
import { EventLog2 } from "static/runtime.js";

const ACTION_TEXT_COLORS = {
  green: "dark",
  yellow: "dark",
  red: "light",
  "dark-red": "light",
  "flashing-red": "light",
};

const hasEventData = (value) => {
  if (value == null) return false;
  if (Array.isArray(value)) return value.length > 0;
  if (typeof value === "object") return Object.keys(value).length > 0;
  if (typeof value === "string") return value.trim().length > 0;
  return true;
};

const resolveRowDisplay = (event) => {
  const provided = event?.rowDisplay;
  if (provided && typeof provided === "object") {
    return {
      utctime: String(provided.utctime ?? ""),
      actionLabel: String(provided.actionLabel ?? event?.set_clear ?? ""),
      name: String(provided.name ?? event?.name ?? ""),
      prefix: String(provided.prefix ?? ""),
      offset: String(provided.offset ?? ""),
      description: String(provided.description ?? event?.description ?? ""),
      location: String(provided.location ?? ""),
      hasData: provided.hasData ?? hasEventData(event?.data),
      dataLabel: String(provided.dataLabel ?? ""),
    };
  }

  const offsetValue = Number(event?.norm_time);
  const fallbackHasData = hasEventData(event?.data);
  return {
    utctime: String(event?.utctime ?? ""),
    actionLabel: String(event?.set_clear ?? ""),
    name: String(event?.name ?? ""),
    prefix: "",
    offset: Number.isFinite(offsetValue) ? `${offsetValue.toFixed(3)}s` : "",
    description: String(event?.description ?? ""),
    location: "",
    hasData: fallbackHasData,
    dataLabel: fallbackHasData ? "Event has data" : "No event data",
  };
};

const resolveConfiguredChannels = (event, view) => {
  const configured = Array.isArray(view?.rowSettings?.channels) ? view.rowSettings.channels : [];
  const normalizedConfigured = configured
    .map((channel) => String(channel || "").trim())
    .filter(Boolean);
  if (normalizedConfigured.length) return normalizedConfigured;
  return Array.isArray(event?.channels)
    ? event.channels.map((channel) => String(channel || "").trim()).filter(Boolean)
    : [];
};

const DataIcon = ({ present }) => html`
  ${present
    ? html`
        <svg class="data-icon data-icon-present" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true">
          <path stroke-linecap="round" stroke-linejoin="round" d="M4 7.5C4 5.567 7.582 4 12 4s8 1.567 8 3.5S16.418 11 12 11 4 9.433 4 7.5Z" />
          <path stroke-linecap="round" stroke-linejoin="round" d="M4 7.5v4C4 13.433 7.582 15 12 15s8-1.567 8-3.5v-4" />
          <path stroke-linecap="round" stroke-linejoin="round" d="M4 11.5v4C4 17.433 7.582 19 12 19s8-1.567 8-3.5v-4" />
        </svg>
      `
    : html`
        <svg class="data-icon data-icon-empty" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true">
          <path stroke-linecap="round" stroke-linejoin="round" d="M4 7.5C4 5.567 7.582 4 12 4s8 1.567 8 3.5S16.418 11 12 11 4 9.433 4 7.5Z" />
          <path stroke-linecap="round" stroke-linejoin="round" d="M4 7.5v4C4 13.433 7.582 15 12 15s8-1.567 8-3.5v-4" />
          <path stroke-linecap="round" stroke-linejoin="round" d="M4 11.5v4C4 17.433 7.582 19 12 19s8-1.567 8-3.5v-4" />
          <path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6" />
        </svg>
      `}
`;

const MatchButton = ({ item, onJump }) => html`
  <button
    type="button"
    class="match-link"
    data-linked-row-id=${String(item.linkedRowId)}
    data-link-direction=${item.direction || ""}
    title=${item.title || ""}
    aria-label=${item.title || item.label || "Jump to matched event"}
    onClick=${(eventClick) => {
      eventClick.stopPropagation();
      onJump?.(item.linkedRowId);
    }}
  >
    <span class="match-value">${item.label}</span>
  </button>
`;

const MatchLinks = ({ event, actionClassName, onJump }) => {
  const summary = event?.matchSummary || { items: [] };
  const items = summary.items || [];
  const emptyClass = items.length ? "" : " is-empty";

  if (summary.collapsed || items.length <= 1) {
    return html`
      <span class=${`log-match${emptyClass}`} data-field="match-links">
        ${items.map((item) => html`<${MatchButton} item=${item} onJump=${onJump} />`)}
      </span>
    `;
  }

  const minItem = items.reduce((best, item) =>
    best == null || Number(item.durationSeconds) < Number(best.durationSeconds) ? item : best
  );

  return html`
    <span class=${`log-match${emptyClass}`} data-field="match-links">
      <span class="match-preview" title="Hover to show all channel durations">${minItem.label}*</span>
      <span class="match-detail">
        ${items.map(
          (item, index) => html`
            ${index > 0 ? html`<span class="match-separator">/</span>` : null}
            <${MatchButton} item=${item} onJump=${onJump} />
          `
        )}
      </span>
    </span>
  `;
};

const CoreEventRow = ({
  event,
  selected = false,
  highlighted = false,
  bookmarkColor = 0,
  extraClasses = [],
  view = null,
  onSelect,
  onBookmark,
  onJump,
}) => {
  const colorLabel = event?.color || "Green";
  const colorClass = String(colorLabel).toLowerCase().replace(/\s+/g, "-");
  const display = resolveRowDisplay(event);
  const channels = new Set((event?.channels || []).map((channel) => String(channel)));
  const configuredChannels = resolveConfiguredChannels(event, view);
  const summary = event?.matchSummary || { items: [] };
  const className = [
    `log-line log-${colorClass}`,
    bookmarkColor ? "is-bookmarked" : "",
    selected ? "log-selected" : "",
    highlighted ? "log-highlight" : "",
    ...(Array.isArray(extraClasses) ? extraClasses : []),
  ].filter(Boolean).join(" ");
  const actionClassName = `log-action${!summary.collapsed && summary.items?.length > 1 ? " is-mixed" : ""}`;
  const dataLabel = display.dataLabel || (display.hasData ? "Event has data" : "No event data");

  return html`
    <div
      class=${className}
      data-bookmark-color=${String(bookmarkColor || 0)}
      data-set-clear=${String(event?.set_clear || "")}
      data-seconds=${event?.norm_time}
      data-row-id=${event?.row_id}
      onClick=${() => onSelect?.(event)}
    >
      <span class="log-channels">
        ${configuredChannels.map(
          (channel) => html`
            <span class=${`log-channel${channels.has(String(channel)) ? " is-on" : ""}`} data-channel=${channel}>
              ${channel}
            </span>
          `
        )}
      </span>
      <span class="log-time log-muted" data-field="utctime">${display.utctime}</span>
      <span class="log-offset log-muted" data-field="offset">${display.offset}</span>
      <span
        class=${actionClassName}
        data-field="action"
        data-event-color=${colorClass}
        data-contrast=${ACTION_TEXT_COLORS[colorClass] || "light"}
      >
        <span class="log-action-label" data-field="action-label">${display.actionLabel}</span>
        <${MatchLinks} event=${event} onJump=${onJump} />
      </span>
      <span class="log-details">
        <span class="log-title-row">
          <span class="log-name" data-field="name">${display.name}</span>
          <span class=${`log-prefix${display.prefix ? "" : " is-hidden"}`} data-field="fault-prefix">${display.prefix}</span>
          <span class="log-meta" data-field="location">${display.location}</span>
        </span>
        <span class="log-desc" data-field="description">${display.description}</span>
      </span>
      <span
        class="data-indicator"
        data-field="data-indicator"
        data-has-data=${display.hasData ? "true" : "false"}
        title=${dataLabel}
        aria-label=${dataLabel}
      >
        <${DataIcon} present=${display.hasData} />
      </span>
      <button
        type="button"
        class="bookmark-toggle"
        title="Toggle bookmark"
        onClick=${(eventClick) => {
          eventClick.stopPropagation();
          onBookmark?.(event);
        }}
      >
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6">
          <path stroke-linecap="round" stroke-linejoin="round" d="M6 4h12v16l-6-3-6 3z" />
        </svg>
      </button>
    </div>
  `;
};

EventLog2.registerLogRowComponent("core_event", CoreEventRow);
