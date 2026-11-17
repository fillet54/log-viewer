import { html } from "cinc";

// Severity drives the row's left stripe on SETs (green is normal: no stripe). The action badge always carries
// the colour: filled for SET, a light tint for CLEAR. Events carry `tone`, their severity mapped onto the
// viewer's tones (see core_event/config.py), so custom severity names style the same way.
const SEVERITY_TONE = { yellow: "warn", red: "error", "flashing-red": "fatal" };

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
      time: String(provided.time ?? event?.time ?? ""),
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
    time: String(event?.time ?? ""),
    actionLabel: String(event?.set_clear ?? ""),
    name: String(event?.name ?? ""),
    prefix: "",
    offset: Number.isFinite(offsetValue) ? `${offsetValue.toFixed(3)}s` : "",
    description: String(event?.description ?? ""),
    location: [event?.system, event?.subsystem, event?.unit].filter(Boolean).join(":"),
    hasData: fallbackHasData,
    dataLabel: fallbackHasData ? "Event has data" : "No event data",
  };
};

const resolveConfiguredChannels = (event, view) => {
  const configured = Array.isArray(view?.config?.channels) ? view.config.channels : [];
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
    <span class="match-direction" aria-hidden="true">${item.direction === "back" ? "↑" : "↓"}</span>
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
  onSetTimeReference,
  timeText = "",
  timeTitle = "",
  isTimeReference = false,
}) => {
  const colorLabel = event?.color || "";
  const colorClass = event?.tone || "green";
  const display = resolveRowDisplay(event);
  const channels = new Set((event?.channels || []).map((channel) => String(channel)));
  const configuredChannels = resolveConfiguredChannels(event, view);
  const summary = event?.matchSummary || { items: [] };
  const isClear = String(event?.set_clear || "").toLowerCase() === "clear";
  const tone = isClear ? "" : SEVERITY_TONE[colorClass] || "";
  const className = [
    `log-line ce-row ce-sev-${colorClass}`,
    tone ? `sev-${tone}` : "",
    bookmarkColor ? "is-bookmarked" : "",
    selected ? "log-selected" : "",
    highlighted ? "log-highlight" : "",
    ...(Array.isArray(extraClasses) ? extraClasses : []),
  ].filter(Boolean).join(" ");
  const actionClassName = [
    "row-level ce-action",
    !summary.collapsed && summary.items?.length > 1 ? "is-mixed" : "",
  ].filter(Boolean).join(" ");
  const dataLabel = display.dataLabel || (display.hasData ? "Event has data" : "No event data");

  return html`
    <div
      class=${className}
      data-bookmark-color=${String(bookmarkColor || 0)}
      data-ce-action=${String(event?.set_clear || "")}
      data-row-id=${event?.row_id}
      onClick=${() => onSelect?.(event)}
    >
      <button
        class=${`log-time-column${isTimeReference ? " is-time-reference" : ""}`}
        type="button"
        title=${timeTitle}
        onClick=${(click) => { click.stopPropagation(); onSetTimeReference?.(); }}
      >${timeText}</button>
      <span class="ce-channels" style=${`--ce-channel-count:${configuredChannels.length}`}>
        ${configuredChannels.map(
          (channel) => html`
            <span
              class=${`ce-channel${channels.has(String(channel)) ? " is-on" : ""}`}
              data-channel=${channel}
              title=${`Channel ${channel}${channels.has(String(channel)) ? "" : " (not seen)"}`}
            >${channel}</span>
          `
        )}
      </span>
      <span class="ce-action-group">
        <span
          class=${actionClassName}
          data-field="action"
          data-ce-color=${colorClass}
          title=${`${colorLabel} ${display.actionLabel}`.trim()}
        >
          <span class="ce-action-label" data-field="action-label">${display.actionLabel}</span>
        </span>
        <${MatchLinks} event=${event} onJump=${onJump} />
      </span>
      <span class="ce-details">
        <span class="ce-title-row">
          <span class="ce-name" data-field="name">${display.name}</span>
          <span class=${`ce-prefix${display.prefix ? "" : " is-hidden"}`} data-field="fault-prefix">${display.prefix}</span>
          <span class="ce-meta" data-field="location" title="system:subsystem:unit">${display.location}</span>
        </span>
        <span class="ce-desc" data-field="description" title=${display.description}>${display.description}</span>
      </span>
      <span
        class="ce-data-indicator"
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

const DetailField = ({ label, children }) =>
  children ? html`<div class="detail-field"><dt>${label}</dt><dd>${children}</dd></div>` : null;

const CoreEventDetail = ({ event }) => {
  const location = [event?.system, event?.subsystem, event?.unit].filter(Boolean).join(":");
  const channels = Array.isArray(event?.channels) ? event.channels.join(" ") : "";
  return html`
    <div class="detail-title">${event?.name ?? event?.row_id}</div>
    ${event?.description ? html`<p class="detail-summary">${event.description}</p>` : null}
    <dl class="detail-fields">
      <${DetailField} label="Action">${[event?.color, event?.set_clear].filter(Boolean).join(" ")}</${DetailField}>
      <${DetailField} label="Time">${event?.time}</${DetailField}>
      <${DetailField} label="Location">${location}</${DetailField}>
      <${DetailField} label="Code">${event?.rowDisplay?.prefix || event?.code}</${DetailField}>
      <${DetailField} label="Channels">${channels}</${DetailField}>
    </dl>
  `;
};

export default function setup({ registerRow, registerDetail }) {
  registerRow(CoreEventRow);
  registerDetail(CoreEventDetail);
}
