class DetailPanelController {
  constructor(element, services) {
    this.element = element;
    this.services = services;
    this.container = queryById(element, "event-detail");
    this.currentEvent = null;
    this.activeReply = null;
  }

  buildDataTree(value, key = "root", depth = 0) {
    const isArray = Array.isArray(value);
    const isObject = value && typeof value === "object" && !isArray;
    if (!isArray && !isObject) {
      return {
        key,
        depth,
        kind: "value",
        value: value == null ? "null" : String(value),
      };
    }

    const entries = isArray
      ? value.map((item, index) => [String(index), item])
      : Object.entries(value);
    return {
      key,
      depth,
      kind: isArray ? "array" : "object",
      count: entries.length,
      children: entries.map(([childKey, childValue]) => this.buildDataTree(childValue, childKey, depth + 1)),
    };
  }

  renderDataTree(node) {
    if (!node) return "";
    if (node.kind === "value") {
      return `
        <div class="data-tree-row data-tree-leaf" style="--tree-depth:${node.depth}">
          <div class="data-tree-key">${this.escapeHtml(node.key)}</div>
          <div class="data-tree-value">${this.escapeHtml(node.value)}</div>
        </div>
      `;
    }

    return `
      <div class="data-tree-node" data-tree-kind="${node.kind}">
        <button type="button" class="data-tree-row data-tree-toggle" data-tree-toggle="true" aria-expanded="true" style="--tree-depth:${node.depth}">
          <div class="data-tree-key">
            <span class="data-tree-chevron" aria-hidden="true"></span>
            <span>${this.escapeHtml(node.key)}</span>
          </div>
          <div class="data-tree-value"></div>
        </button>
        <div class="data-tree-children">
          ${node.children.map((child) => this.renderDataTree(child)).join("")}
        </div>
      </div>
    `;
  }

  escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  renderThread(threads, depth = 0) {
    if (!threads.length) return '<div class="support-text">No comments yet.</div>';
    return `
      <div class="comment-thread">
        ${threads
          .map(
            (comment) => `
            <div class="comment-item" data-comment-id="${comment.id}" style="margin-left:${depth * 16}px">
              <div class="comment-meta">
                <span class="comment-time">${this.escapeHtml(comment.created_at)}</span>
                <button class="button button-ghost button-xs comment-reply" data-comment-id="${comment.id}">Reply</button>
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
    const { bus, bookmarks, comments } = this.services;
    const bookmarksEnabled = bookmarks?.enabled !== false;
    const commentsEnabled = comments?.enabled !== false;
    this.currentEvent = event || null;
    if (!this.container) return;
    if (!event) {
      this.container.innerHTML = '<div class="empty-panel-message">Select a log event to view details.</div>';
      return;
    }

    const threads = commentsEnabled ? comments?.buildThreads(event.row_id) || [] : [];
    const colorIndex = bookmarksEnabled ? bookmarks?.getColor(event.row_id) || 0 : 0;
    const isBookmarked = bookmarksEnabled ? bookmarks?.isBookmarked(event.row_id) : false;
    const dataTree = event.data && typeof event.data === "object" ? this.buildDataTree(event.data) : null;

    this.container.innerHTML = `
      <div class="detail-stack">
        <div class="detail-title">${event.name}</div>
        <div class="detail-meta">${event.utctime} • ${event.set_clear}</div>
        <div class="detail-summary">${event.description}</div>
        <div class="detail-path">${event.system}/${event.subsystem}/${event.unit}/${event.code}</div>
        ${
          dataTree
            ? `<div class="event-data">
                <div class="event-data-header">
                  <div class="section-label">Log Data</div>
                  <div class="event-data-actions">
                    <button type="button" class="button button-ghost button-xs" id="collapse-all-data">Collapse All</button>
                    <button type="button" class="button button-ghost button-xs" id="expand-all-data">Expand All</button>
                  </div>
                </div>
                <div class="data-tree">
                  ${this.renderDataTree(dataTree)}
                </div>
              </div>`
            : '<div class="support-text">No event data available.</div>'
        }
        ${
          isBookmarked
            ? `
          <div class="bookmark-notes">
            <div class="section-label">Bookmark Color</div>
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
        ${
          commentsEnabled
            ? `<div class="comment-section">
                <div class="section-label">Comments</div>
                <div class="support-text">Stored only in this browser.</div>
                ${this.activeReply ? `<div class="comment-replying">Replying to #${this.activeReply} <button class="button button-ghost button-xs" id="cancel-reply">Cancel</button></div>` : ""}
                <div class="comment-list">${this.renderThread(threads)}</div>
                <div class="comment-form">
                  <textarea id="comment-body" class="text-area" rows="3" placeholder="Add a comment..."></textarea>
                  <button class="button button-primary button-small comment-submit" id="submit-comment">Post</button>
                </div>
              </div>`
            : ""
        }
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

    this.container.querySelectorAll("[data-tree-toggle]").forEach((button) => {
      button.addEventListener("click", () => {
        const node = button.closest(".data-tree-node");
        if (!node) return;
        const expanded = button.getAttribute("aria-expanded") !== "false";
        button.setAttribute("aria-expanded", String(!expanded));
        node.classList.toggle("is-collapsed", expanded);
      });
    });

    const collapseAllButton = this.container.querySelector("#collapse-all-data");
    if (collapseAllButton) {
      collapseAllButton.addEventListener("click", () => {
        this.container.querySelectorAll(".data-tree-node").forEach((node) => node.classList.add("is-collapsed"));
        this.container
          .querySelectorAll("[data-tree-toggle]")
          .forEach((button) => button.setAttribute("aria-expanded", "false"));
      });
    }

    const expandAllButton = this.container.querySelector("#expand-all-data");
    if (expandAllButton) {
      expandAllButton.addEventListener("click", () => {
        this.container.querySelectorAll(".data-tree-node").forEach((node) => node.classList.remove("is-collapsed"));
        this.container
          .querySelectorAll("[data-tree-toggle]")
          .forEach((button) => button.setAttribute("aria-expanded", "true"));
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
      <div class="pane-header pane-header-spread">
        <div class="pane-header-title">
          <span class="icon">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
              <path stroke-linecap="round" stroke-linejoin="round" d="M3 12h6l3-3 6 6" />
            </svg>
          </span>
          Insights
        </div>
        <button id="toggle-detail" class="button button-ghost button-xs" title="Hide info pane" aria-label="Hide info pane">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" class="tool-icon">
            <path stroke-linecap="round" stroke-linejoin="round" d="M9 6l6 6-6 6" />
          </svg>
        </button>
      </div>
      <div class="pane-body" id="event-detail">
        <div class="empty-panel-message">Select a log event to view details.</div>
      </div>
    `;
  }

  getServices() {
    return {
      bus: this.getBus(),
      bookmarks: this.getBookmarks(),
      comments: this.getComments(),
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
