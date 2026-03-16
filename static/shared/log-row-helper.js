window.LogRowHelper = window.LogRowHelper || {};

const ACTION_TEXT_COLORS = {
  green: "dark",
  yellow: "dark",
  red: "light",
  "dark-red": "light",
  "flashing-red": "light",
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
  setText('[data-field="action"]', event.set_clear);
  setText('[data-field="name"]', event.name);
  const offsetValue = Number(event.norm_time);
  setText('[data-field="offset"]', Number.isFinite(offsetValue) ? `${offsetValue.toFixed(3)}s` : "");
  setText('[data-field="description"]', event.description);
  setText(
    '[data-field="code"]',
    `${event.system}/${event.subsystem}/${event.unit}/${event.code}`
  );

  const actionEl = row.querySelector('[data-field="action"]');
  if (actionEl) {
    actionEl.dataset.eventColor = colorClass;
    actionEl.dataset.contrast = ACTION_TEXT_COLORS[colorClass] || "light";
  }

  const channels = new Set(event.channels || []);
  ["A", "B", "C", "D"].forEach((channelId) => {
    const el = row.querySelector(`[data-channel="${channelId}"]`);
    if (el) el.classList.toggle("is-on", channels.has(channelId));
  });

  return row;
};
