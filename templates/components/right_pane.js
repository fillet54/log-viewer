window.LogApp = window.LogApp || {};

LogApp.initRightPane = (bus) => {
  const container = document.getElementById("event-detail");
  if (!container || !bus) return;
  let currentEvent = null;
  let activeReply = null;

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

  const escapeHtml = (value) => {
    const text = String(value ?? "");
    return text
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  };

  const formatUser = (comment) => {
    return escapeHtml(comment.user_name || comment.user_email || "User");
  };

  const renderThread = (threads, depth = 0) => {
    if (!threads.length) return '<div class="text-xs text-base-content/50">No comments yet.</div>';
    return `
      <div class="comment-thread">
        ${threads
          .map(
            (comment) => `
            <div class="comment-item" data-comment-id="${comment.id}" style="margin-left:${depth * 16}px">
              <div class="comment-meta">
                <span class="comment-author">${formatUser(comment)}</span>
                <span class="comment-time">${escapeHtml(comment.created_at)}</span>
                ${LogApp.isLoggedIn ? `<button class="btn btn-ghost btn-xs comment-reply" data-comment-id="${comment.id}">Reply</button>` : ""}
              </div>
              <div class="comment-body">${escapeHtml(comment.body)}</div>
              ${comment.replies?.length ? renderThread(comment.replies, depth + 1) : ""}
            </div>
          `
          )
          .join("")}
      </div>
    `;
  };

  const renderEvent = (event) => {
    currentEvent = event || null;
    if (!event) {
      container.innerHTML = '<div class="text-sm text-base-content/60">Select a log event to view details.</div>';
      return;
    }

    const isBookmarked = LogApp.bookmarks?.isBookmarked(event.row_id);
    const colorIndex = LogApp.bookmarks?.getColor(event.row_id) || 0;
    const threads = LogApp.comments?.buildThreads(event.row_id) || [];
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
          </div>
        ` : ""}
        <div class="comment-section">
          <div class="text-xs uppercase tracking-wide text-base-content/60">Comments</div>
          ${activeReply ? `<div class="comment-replying text-xs">Replying to #${activeReply} <button class="btn btn-ghost btn-xs" id="cancel-reply">Cancel</button></div>` : ""}
          <div class="comment-list">${renderThread(threads)}</div>
          ${LogApp.isLoggedIn ? `
            <div class="comment-form">
              <textarea id="comment-body" class="textarea textarea-bordered textarea-sm w-full" rows="3" placeholder="Add a comment..."></textarea>
              <button class="btn btn-primary btn-sm mt-2" id="submit-comment">Post</button>
            </div>
          ` : `
            <div class="text-xs text-base-content/50">Log in to comment.</div>
          `}
        </div>
      </div>
    `;

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

    const replyButtons = container.querySelectorAll(".comment-reply");
    replyButtons.forEach((button) => {
      button.addEventListener("click", () => {
        activeReply = Number(button.dataset.commentId) || null;
        renderEvent(event);
      });
    });

    const cancelReplyButton = container.querySelector("#cancel-reply");
    if (cancelReplyButton) {
      cancelReplyButton.addEventListener("click", () => {
        activeReply = null;
        renderEvent(event);
      });
    }

    const submitButton = container.querySelector("#submit-comment");
    if (submitButton) {
      submitButton.addEventListener("click", async () => {
        const input = container.querySelector("#comment-body");
        const body = input?.value.trim();
        if (!body) return;
        const created = await LogApp.comments?.addComment(event.row_id, body, activeReply);
        if (created && input) {
          input.value = "";
          activeReply = null;
          renderEvent(event);
        }
      });
    }
  };

  bus.on("event:selected", renderEvent);
  bus.on("comments:changed", () => {
    if (currentEvent) renderEvent(currentEvent);
  });
};
