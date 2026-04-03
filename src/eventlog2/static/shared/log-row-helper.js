window.LogRowHelper = window.LogRowHelper || {};

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

const COLOR_PREFIX = {
  green: "G",
  yellow: "Y",
  red: "R",
  "dark-red": "R",
  "flashing-red": "F",
};

const buildFaultPrefix = (event, colorClass) => {
  const rawCode = String(event.code || "").trim();
  if (!rawCode) return "";

  const parts = rawCode.split("-").filter(Boolean);
  const severity = COLOR_PREFIX[colorClass] || "G";

  if (parts.length >= 2) {
    return `${parts[0]}-${severity}-${parts.slice(1).join("-")}`;
  }

  const systemPrefix = String(event.system || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, "")
    .slice(0, 3);

  return systemPrefix ? `${systemPrefix}-${severity}-${rawCode}` : `${severity}-${rawCode}`;
};

LogRowHelper.buildRow = (event, templateEl, options = {}) => {
  if (!templateEl?.content?.firstElementChild || !event) return null;
  const { extraClasses = [], bookmarks = null } = options;
  const row = templateEl.content.firstElementChild.cloneNode(true);
  const colorLabel = event.color || "Green";
  const colorClass = String(colorLabel).toLowerCase().replace(/\s+/g, "-");
  const colorIndex = bookmarks?.getColor(event.row_id) || 0;
  const extra = Array.isArray(extraClasses) ? extraClasses.filter(Boolean) : [];
  row.className = [`log-line log-${colorClass}${colorIndex ? " is-bookmarked" : ""}`, ...extra].join(
    " "
  );
  row.dataset.bookmarkColor = String(colorIndex);
  row.dataset.seconds = event.norm_time;
  row.dataset.rowId = event.row_id;

  const setText = (selector, value) => {
    const el = row.querySelector(selector);
    if (el) el.textContent = value ?? "";
  };
  setText('[data-field="utctime"]', event.utctime);
  setText('[data-field="action-label"]', event.set_clear);
  setText('[data-field="name"]', event.name);
  const prefixValue = buildFaultPrefix(event, colorClass);
  setText('[data-field="fault-prefix"]', prefixValue ? `[${prefixValue}]` : "");
  const offsetValue = Number(event.norm_time);
  setText('[data-field="offset"]', Number.isFinite(offsetValue) ? `${offsetValue.toFixed(3)}s` : "");
  setText('[data-field="description"]', event.description);
  const location = [event.system, event.subsystem, event.unit].filter(Boolean).join("/");
  setText('[data-field="location"]', location ? `(${location})` : "");

  const actionEl = row.querySelector('[data-field="action"]');
  if (actionEl) {
    actionEl.dataset.eventColor = colorClass;
    actionEl.dataset.contrast = ACTION_TEXT_COLORS[colorClass] || "light";
  }
  const matchContainer = row.querySelector('[data-field="match-links"]');

  const hasData = hasEventData(event.data);
  const dataIndicatorEl = row.querySelector('[data-field="data-indicator"]');
  if (dataIndicatorEl) {
    dataIndicatorEl.dataset.hasData = hasData ? "true" : "false";
    const label = hasData ? "Event has data" : "No event data";
    dataIndicatorEl.title = label;
    dataIndicatorEl.setAttribute("aria-label", label);
  }

  const prefixEl = row.querySelector('[data-field="fault-prefix"]');
  if (prefixEl) {
    prefixEl.classList.toggle("is-hidden", !prefixValue);
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

  const channels = new Set(event.channels || []);
  ["A", "B", "C", "D"].forEach((channelId) => {
    const el = row.querySelector(`[data-channel="${channelId}"]`);
    if (el) el.classList.toggle("is-on", channels.has(channelId));
  });

  return row;
};
