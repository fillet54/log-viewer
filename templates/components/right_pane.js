window.LogApp = window.LogApp || {};

LogApp.initRightPane = (bus) => {
  const container = document.getElementById("event-detail");
  if (!container || !bus) return;

  const renderRows = (value, prefix = []) => {
    if (value && typeof value === "object" && !Array.isArray(value)) {
      return Object.entries(value).flatMap(([key, val]) =>
        renderRows(val, [...prefix, key])
      );
    }
    return [
      {
        key: prefix.join("."),
        value: String(value),
      },
    ];
  };

  const loadNotes = () => {
    try {
      const raw = localStorage.getItem(LogApp.STORAGE_KEYS.bookmarkNotes);
      const parsed = raw ? JSON.parse(raw) : {};
      return parsed && typeof parsed === "object" ? parsed : {};
    } catch (err) {
      return {};
    }
  };

  const saveNotes = (notes) => {
    localStorage.setItem(LogApp.STORAGE_KEYS.bookmarkNotes, JSON.stringify(notes));
  };

  const renderEvent = (event) => {
    if (!event) {
      container.innerHTML = '<div class="text-sm text-base-content/60">Select a log event to view details.</div>';
      return;
    }

    const isBookmarked = LogApp.bookmarks?.isBookmarked(event.row_id);
    const colorIndex = LogApp.bookmarks?.getColor(event.row_id) || 0;
    const notes = loadNotes();
    const noteValue = notes[String(event.row_id)] || "";
    let dataBlock = '<div class="text-xs text-base-content/50">No event data available.</div>';
    if (event.data) {
      const rows = renderRows(event.data);
      const rendered = rows
        .map(
          (row) => `
          <div class="data-row">
            <div class="data-key">${row.key}</div>
            <div class="data-value">${row.value}</div>
          </div>
        `
        )
        .join("");
      dataBlock = `<div class="event-data">${rendered}</div>`;
    }

    container.innerHTML = `
      <div class="space-y-3">
        <div class="text-sm font-semibold">${event.name}</div>
        <div class="text-xs text-base-content/60">${event.utctime} • ${event.set_clear}</div>
        <div class="text-xs text-base-content/70">${event.description}</div>
        <div class="text-xs text-base-content/50">${event.system}/${event.subsystem}/${event.unit}/${event.code}</div>
        ${dataBlock}
        ${isBookmarked ? `
          <div class="bookmark-notes">
            <div class="text-xs uppercase tracking-wide text-base-content/60">Bookmark Color</div>
            <div class="bookmark-colors" data-row-id="${event.row_id}">
              ${[1,2,3,4,5].map((idx) => `
                <button class="bookmark-color ${colorIndex === idx ? "is-active" : ""}" data-color="${idx}" title="Color ${idx}"></button>
              `).join("")}
            </div>
            <div class="text-xs uppercase tracking-wide text-base-content/60">Bookmark Note</div>
            <textarea id="bookmark-note" class="textarea textarea-bordered textarea-sm w-full" rows="3" placeholder="Add a note...">${noteValue}</textarea>
          </div>
        ` : ""}
      </div>
    `;

    const noteInput = container.querySelector("#bookmark-note");
    if (noteInput) {
      noteInput.addEventListener("input", () => {
        const updated = loadNotes();
        updated[String(event.row_id)] = noteInput.value;
        saveNotes(updated);
      });
    }

    const colorContainer = container.querySelector(".bookmark-colors");
    if (colorContainer) {
      colorContainer.addEventListener("click", (clickEvent) => {
        const button = clickEvent.target.closest(".bookmark-color");
        if (!button) return;
        const color = Number(button.dataset.color) || 1;
        LogApp.bookmarks?.setColor(event.row_id, color);
        if (bus) bus.emit("bookmarks:changed", LogApp.bookmarks?.getAllWithColors() || {});
        renderEvent(event);
      });
    }
  };

  bus.on("event:selected", renderEvent);
};
