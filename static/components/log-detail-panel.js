class DetailPanelController {
  constructor(element, services) {
    this.element = element;
    this.services = services;
    this.container = queryById(element, "event-detail");
    this.currentEvent = null;
    this.activeReply = null;
  }

  renderRows(value, prefix = []) {
    if (value && typeof value === "object" && !Array.isArray(value)) {
      return Object.entries(value).flatMap(([key, val]) => this.renderRows(val, [...prefix, key]));
    }
    return [{ key: prefix.join("."), value: String(value) }];
  }

  escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  formatUser(comment) {
    return this.escapeHtml(comment.user_name || comment.user_email || "User");
  }

  renderThread(threads, depth = 0) {
    const { isLoggedIn } = this.services;
    if (!threads.length) return '<div class="text-xs text-base-content/50">No comments yet.</div>';
    return `
      <div class="comment-thread">
        ${threads
          .map(
            (comment) => `
            <div class="comment-item" data-comment-id="${comment.id}" style="margin-left:${depth * 16}px">
              <div class="comment-meta">
                <span class="comment-author">${this.formatUser(comment)}</span>
                <span class="comment-time">${this.escapeHtml(comment.created_at)}</span>
                ${isLoggedIn ? `<button class="btn btn-ghost btn-xs comment-reply" data-comment-id="${comment.id}">Reply</button>` : ""}
              </div>
              <div class="comment-body">${this.escapeHtml(comment.body)}</div>
              ${comment.replies?.length ? this.renderThread(comment.replies, depth + 1) : ""}
            </div>
          `
          )
          .join("")}
      </div>
    `;
  }

  renderEvent(event) {
    const { bus, bookmarks, comments, isLoggedIn } = this.services;
    this.currentEvent = event || null;
    if (!this.container) return;
    if (!event) {
      this.container.innerHTML = '<div class="text-sm text-base-content/60">Select a log event to view details.</div>';
      return;
    }

    const threads = comments?.buildThreads(event.row_id) || [];
    const colorIndex = bookmarks?.getColor(event.row_id) || 0;
    const isBookmarked = bookmarks?.isBookmarked(event.row_id);
    const rows = event.data ? this.renderRows(event.data) : [];

    this.container.innerHTML = `
      <div class="space-y-3">
        <div class="text-sm font-semibold">${event.name}</div>
        <div class="text-xs text-base-content/60">${event.utctime} • ${event.set_clear}</div>
        <div class="text-xs text-base-content/70">${event.description}</div>
        <div class="text-xs text-base-content/50">${event.system}/${event.subsystem}/${event.unit}/${event.code}</div>
        ${
          rows.length
            ? `<div class="event-data">${rows
                .map(
                  (row) => `
                    <div class="data-row">
                      <div class="data-key">${row.key}</div>
                      <div class="data-value">${row.value}</div>
                    </div>
                  `
                )
                .join("")}</div>`
            : '<div class="text-xs text-base-content/50">No event data available.</div>'
        }
        ${
          isBookmarked
            ? `
          <div class="bookmark-notes">
            <div class="text-xs uppercase tracking-wide text-base-content/60">Bookmark Color</div>
            <div class="bookmark-colors" data-row-id="${event.row_id}">
              ${[1, 2, 3, 4, 5]
                .map(
                  (idx) => `
                    <button class="bookmark-color ${colorIndex === idx ? "is-active" : ""}" data-color="${idx}" title="Color ${idx}"></button>
                  `
                )
                .join("")}
            </div>
          </div>
        `
            : ""
        }
        <div class="comment-section">
          <div class="text-xs uppercase tracking-wide text-base-content/60">Comments</div>
          ${this.activeReply ? `<div class="comment-replying text-xs">Replying to #${this.activeReply} <button class="btn btn-ghost btn-xs" id="cancel-reply">Cancel</button></div>` : ""}
          <div class="comment-list">${this.renderThread(threads)}</div>
          ${
            isLoggedIn
              ? `
            <div class="comment-form">
              <textarea id="comment-body" class="textarea textarea-bordered textarea-sm w-full" rows="3" placeholder="Add a comment..."></textarea>
              <button class="btn btn-primary btn-sm mt-2" id="submit-comment">Post</button>
            </div>
          `
              : '<div class="text-xs text-base-content/50">Log in to comment.</div>'
          }
        </div>
      </div>
    `;

    const colorContainer = this.container.querySelector(".bookmark-colors");
    if (colorContainer) {
      colorContainer.addEventListener("click", (clickEvent) => {
        const button = clickEvent.target.closest(".bookmark-color");
        if (!button) return;
        bookmarks?.setColor(event.row_id, Number(button.dataset.color) || 1);
        if (bus) bus.emit("bookmarks:changed", bookmarks?.getAllWithColors() || {});
        this.renderEvent(event);
      });
    }

    this.container.querySelectorAll(".comment-reply").forEach((button) => {
      button.addEventListener("click", () => {
        this.activeReply = Number(button.dataset.commentId) || null;
        this.renderEvent(event);
      });
    });

    const cancelReplyButton = this.container.querySelector("#cancel-reply");
    if (cancelReplyButton) {
      cancelReplyButton.addEventListener("click", () => {
        this.activeReply = null;
        this.renderEvent(event);
      });
    }

    const submitButton = this.container.querySelector("#submit-comment");
    if (submitButton) {
      submitButton.addEventListener("click", async () => {
        const input = this.container.querySelector("#comment-body");
        const body = input?.value.trim();
        if (!body) return;
        const created = await comments?.addComment(event.row_id, body, this.activeReply);
        if (created && input) {
          input.value = "";
          this.activeReply = null;
          this.renderEvent(event);
        }
      });
    }
  }

  initialize() {
    const { bus } = this.services;
    if (!this.container || !bus) return;
    bus.on("event:selected", (event) => this.renderEvent(event));
    bus.on("comments:changed", () => {
      if (this.currentEvent) this.renderEvent(this.currentEvent);
    });
  }
}

class LogDetailPanelElement extends LogAppComponentElement {
  renderShell() {
    this.innerHTML = `
      <div class="pane-header">
        <span class="icon">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
            <path stroke-linecap="round" stroke-linejoin="round" d="M3 12h6l3-3 6 6" />
          </svg>
        </span>
        Insights
      </div>
      <div class="pane-body" id="event-detail">
        <div class="text-sm text-base-content/60">Select a log event to view details.</div>
      </div>
    `;
  }

  getServices() {
    return {
      bus: this.getBus(),
      bookmarks: this.getBookmarks(),
      comments: this.getComments(),
      isLoggedIn: this.isLoggedIn(),
    };
  }

  connectedCallback() {
    this.connectToApp(() => {
      if (!this.querySelector("#event-detail")) {
        this.renderShell();
      }
      this.controller = this.controller || new DetailPanelController(this, this.getServices());
      this.controller.initialize();
    });
  }
}

if (!customElements.get("log-detail-panel")) {
  customElements.define("log-detail-panel", LogDetailPanelElement);
}
