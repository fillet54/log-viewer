window.EventLog2PluginViews = window.EventLog2PluginViews || {};
window.EventLog2PluginViews.coreEvent = window.EventLog2PluginViews.coreEvent || {};

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

window.EventLog2.registerPluginRowRenderer("core-event", (event, templateEl, options = {}) => {
  if (!templateEl?.content?.firstElementChild || !event) return null;
  const { extraClasses = [], bookmarks = null } = options;
  const row = templateEl.content.firstElementChild.cloneNode(true);
  const colorLabel = event.color || "Green";
  const colorClass = String(colorLabel).toLowerCase().replace(/\s+/g, "-");
  const colorIndex = bookmarks?.getColor(event.row_id) || 0;
  const extra = Array.isArray(extraClasses) ? extraClasses.filter(Boolean) : [];
  const display = resolveRowDisplay(event);
  row.className = [`log-line log-${colorClass}${colorIndex ? " is-bookmarked" : ""}`, ...extra].join(" ");
  row.dataset.bookmarkColor = String(colorIndex);
  row.dataset.seconds = event.norm_time;
  row.dataset.rowId = event.row_id;

  const setText = (selector, value) => {
    const el = row.querySelector(selector);
    if (el) el.textContent = value ?? "";
  };
  setText('[data-field="utctime"]', display.utctime);
  setText('[data-field="action-label"]', display.actionLabel);
  setText('[data-field="name"]', display.name);
  setText('[data-field="fault-prefix"]', display.prefix);
  setText('[data-field="offset"]', display.offset);
  setText('[data-field="description"]', display.description);
  setText('[data-field="location"]', display.location);

  const actionEl = row.querySelector('[data-field="action"]');
  if (actionEl) {
    actionEl.dataset.eventColor = colorClass;
    actionEl.dataset.contrast = ACTION_TEXT_COLORS[colorClass] || "light";
  }
  const matchContainer = row.querySelector('[data-field="match-links"]');

  const dataIndicatorEl = row.querySelector('[data-field="data-indicator"]');
  if (dataIndicatorEl) {
    dataIndicatorEl.dataset.hasData = display.hasData ? "true" : "false";
    const label = display.dataLabel || (display.hasData ? "Event has data" : "No event data");
    dataIndicatorEl.title = label;
    dataIndicatorEl.setAttribute("aria-label", label);
  }

  const prefixEl = row.querySelector('[data-field="fault-prefix"]');
  if (prefixEl) {
    prefixEl.classList.toggle("is-hidden", !display.prefix);
  }

  if (matchContainer) {
    const summary = event.matchSummary || { items: [] };
    matchContainer.innerHTML = "";
    matchContainer.classList.toggle("is-empty", !summary.items?.length);
    const items = summary.items || [];
    if (actionEl) {
      actionEl.classList.toggle("is-mixed", !summary.collapsed && items.length > 1);
    }
    if (summary.collapsed || items.length <= 1) {
      items.forEach((item) => {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "match-link";
        button.dataset.linkedRowId = String(item.linkedRowId);
        button.dataset.linkDirection = item.direction || "";
        button.title = item.title || "";
        button.setAttribute("aria-label", item.title || item.label || "Jump to matched event");
        button.innerHTML = `<span class="match-value">${item.label}</span>`;
        matchContainer.appendChild(button);
      });
    } else if (items.length) {
      const minItem = items.reduce((best, item) =>
        best == null || Number(item.durationSeconds) < Number(best.durationSeconds) ? item : best
      );
      const preview = document.createElement("span");
      preview.className = "match-preview";
      preview.textContent = `${minItem.label}*`;
      preview.title = "Hover to show all channel durations";
      matchContainer.appendChild(preview);

      const detail = document.createElement("span");
      detail.className = "match-detail";
      items.forEach((item, index) => {
        if (index > 0) {
          const separator = document.createElement("span");
          separator.className = "match-separator";
          separator.textContent = "/";
          detail.appendChild(separator);
        }
        const button = document.createElement("button");
        button.type = "button";
        button.className = "match-link";
        button.dataset.linkedRowId = String(item.linkedRowId);
        button.dataset.linkDirection = item.direction || "";
        button.title = item.title || "";
        button.setAttribute("aria-label", item.title || item.label || "Jump to matched event");
        button.innerHTML = `<span class="match-value">${item.label}</span>`;
        detail.appendChild(button);
      });
      matchContainer.appendChild(detail);
    }
  }

  const channels = new Set((event.channels || []).map((channel) => String(channel)));
  row.querySelectorAll("[data-channel]").forEach((el) => {
    const channelId = String(el.dataset.channel || "");
    el.classList.toggle("is-on", channels.has(channelId));
  });

  return row;
});
