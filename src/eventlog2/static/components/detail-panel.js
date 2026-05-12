(function () {
  const ui = window.EventLog2UI || {};
  const html = ui.html;
  const Fragment = ui.Fragment;
  const useEffect = ui.hooks?.useEffect || null;
  const useState = ui.hooks?.useState || null;

  ui.components = ui.components || {};

  const buildDataTree = (value, key = "root", depth = 0, path = "root") => {
    const isArray = Array.isArray(value);
    const isObject = value && typeof value === "object" && !isArray;
    if (!isArray && !isObject) {
      return {
        key,
        depth,
        path,
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
      path,
      kind: isArray ? "array" : "object",
      count: entries.length,
      children: entries.map(([childKey, childValue]) =>
        buildDataTree(childValue, childKey, depth + 1, `${path}.${childKey}`)
      ),
    };
  };

  const collectExpandablePaths = (node, paths = []) => {
    if (!node || node.kind === "value") return paths;
    paths.push(node.path);
    (node.children || []).forEach((child) => collectExpandablePaths(child, paths));
    return paths;
  };

  const DataTreeNode = ({ node, collapsedPaths, onToggle }) => {
    if (!node) return null;
    if (node.kind === "value") {
      return html`
        <div class="data-tree-row data-tree-leaf" style=${{ "--tree-depth": node.depth }}>
          <div class="data-tree-key">${node.key}</div>
          <div class="data-tree-value">${node.value}</div>
        </div>
      `;
    }

    const collapsed = collapsedPaths.has(node.path);
    return html`
      <div class=${`data-tree-node${collapsed ? " is-collapsed" : ""}`} data-tree-kind=${node.kind}>
        <button
          type="button"
          class="data-tree-row data-tree-toggle"
          data-tree-toggle="true"
          aria-expanded=${String(!collapsed)}
          style=${{ "--tree-depth": node.depth }}
          onClick=${() => onToggle(node.path)}
        >
          <div class="data-tree-key">
            <span class="data-tree-chevron" aria-hidden="true"></span>
            <span>${node.key}</span>
          </div>
          <div class="data-tree-value"></div>
        </button>
        <div class="data-tree-children">
          ${(node.children || []).map(
            (child) => html`<${DataTreeNode} node=${child} collapsedPaths=${collapsedPaths} onToggle=${onToggle} />`
          )}
        </div>
      </div>
    `;
  };

  const CommentThread = ({ threads, depth = 0, onReply }) => {
    if (!threads.length) return html`<div class="support-text">No comments yet.</div>`;
    return html`
      <div class="comment-thread">
        ${threads.map(
          (comment) => html`
            <div class="comment-item" data-comment-id=${comment.id} style=${{ marginLeft: `${depth * 16}px` }}>
              <div class="comment-meta">
                <span class="comment-time">${comment.created_at}</span>
                <button
                  type="button"
                  class="button button-ghost button-xs comment-reply"
                  data-comment-id=${comment.id}
                  onClick=${() => onReply(String(comment.id))}
                >
                  Reply
                </button>
              </div>
              <div class="comment-body">${comment.body}</div>
              ${comment.replies?.length
                ? html`<${CommentThread} threads=${comment.replies} depth=${depth + 1} onReply=${onReply} />`
                : null}
            </div>
          `
        )}
      </div>
    `;
  };

  const DetailPanelHeader = () => html`
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
  `;

  const EmptyDetailState = () => html`<div class="empty-panel-message">Select a log event to view details.</div>`;

  const EventSummary = ({ event }) => html`
    <${Fragment}>
      <div class="detail-title">${event.name}</div>
      <div class="detail-meta">${event.utctime} • ${event.set_clear}</div>
      <div class="detail-summary">${event.description}</div>
      <div class="detail-path">${event.system}/${event.subsystem}/${event.unit}/${event.code}</div>
    </${Fragment}>
  `;

  const DataTreeSection = ({
    dataTree,
    allExpandablePaths,
    collapsedPaths,
    onCollapseAll,
    onExpandAll,
    onToggle,
  }) => {
    if (!dataTree) return html`<div class="support-text">No event data available.</div>`;
    return html`
      <div class="event-data">
        <div class="event-data-header">
          <div class="section-label">Log Data</div>
          <div class="event-data-actions">
            <button
              type="button"
              class="button button-ghost button-xs"
              id="collapse-all-data"
              onClick=${() => onCollapseAll(new Set(allExpandablePaths))}
            >
              Collapse All
            </button>
            <button
              type="button"
              class="button button-ghost button-xs"
              id="expand-all-data"
              onClick=${onExpandAll}
            >
              Expand All
            </button>
          </div>
        </div>
        <div class="data-tree">
          <${DataTreeNode} node=${dataTree} collapsedPaths=${collapsedPaths} onToggle=${onToggle} />
        </div>
      </div>
    `;
  };

  const BookmarkSection = ({ event, colorIndex, onSetColor }) => html`
    <div class="bookmark-notes">
      <div class="section-label">Bookmark Color</div>
      <div class="bookmark-colors" data-row-id=${event.row_id}>
        ${[1, 2, 3, 4, 5].map(
          (idx) => html`
            <button
              type="button"
              class=${`bookmark-color${colorIndex === idx ? " is-active" : ""}`}
              data-color=${idx}
              title=${`Color ${idx}`}
              onClick=${() => onSetColor(idx)}
            ></button>
          `
        )}
      </div>
    </div>
  `;

  const CommentSection = ({
    threads,
    activeReply,
    commentBody,
    onReply,
    onCancelReply,
    onCommentInput,
    onSubmitComment,
  }) => html`
    <div class="comment-section">
      <div class="section-label">Comments</div>
      <div class="support-text">Stored only in this browser.</div>
      ${activeReply
        ? html`
            <div class="comment-replying">
              ${`Replying to #${activeReply} `}
              <button
                type="button"
                class="button button-ghost button-xs"
                id="cancel-reply"
                onClick=${onCancelReply}
              >
                Cancel
              </button>
            </div>
          `
        : null}
      <div class="comment-list">
        <${CommentThread} threads=${threads} onReply=${onReply} />
      </div>
      <div class="comment-form">
        <textarea
          id="comment-body"
          class="text-area"
          rows="3"
          placeholder="Add a comment..."
          value=${commentBody}
          onInput=${onCommentInput}
        ></textarea>
        <button
          type="button"
          class="button button-primary button-small comment-submit"
          id="submit-comment"
          onClick=${onSubmitComment}
        >
          Post
        </button>
      </div>
    </div>
  `;

  const DetailEventContent = ({
    event,
    dataTree,
    allExpandablePaths,
    collapsedPaths,
    onCollapseAll,
    onExpandAll,
    onToggleCollapsed,
    isBookmarked,
    colorIndex,
    onSetBookmarkColor,
    commentsEnabled,
    threads,
    activeReply,
    commentBody,
    onReply,
    onCancelReply,
    onCommentInput,
    onSubmitComment,
  }) => html`
    <div class="detail-stack">
      <${EventSummary} event=${event} />
      <${DataTreeSection}
        dataTree=${dataTree}
        allExpandablePaths=${allExpandablePaths}
        collapsedPaths=${collapsedPaths}
        onCollapseAll=${onCollapseAll}
        onExpandAll=${onExpandAll}
        onToggle=${onToggleCollapsed}
      />
      ${isBookmarked
        ? html`
            <${BookmarkSection}
              event=${event}
              colorIndex=${colorIndex}
              onSetColor=${onSetBookmarkColor}
            />
          `
        : null}
      ${commentsEnabled
        ? html`
            <${CommentSection}
              threads=${threads}
              activeReply=${activeReply}
              commentBody=${commentBody}
              onReply=${onReply}
              onCancelReply=${onCancelReply}
              onCommentInput=${onCommentInput}
              onSubmitComment=${onSubmitComment}
            />
          `
        : null}
    </div>
  `;

  const DetailPanelApp = () => {
    const services = ui.appHooks.useAppServices();
    const viewerStore = services?.viewerStore || null;
    const selectedEvent = viewerStore?.selectedEvent?.value || null;
    const [activeReply, setActiveReply] = useState ? useState(null) : [null, () => {}];
    const [commentBody, setCommentBody] = useState ? useState("") : ["", () => {}];
    const [collapsedPaths, setCollapsedPaths] = useState ? useState(() => new Set()) : [new Set(), () => {}];

    const bookmarks = viewerStore || null;
    const comments = viewerStore || null;
    const bookmarksEnabled = viewerStore?.bookmarksEnabled !== false;
    const commentsEnabled = viewerStore?.commentsEnabled !== false;

    if (typeof useEffect === "function") {
      useEffect(() => {
        setActiveReply(null);
        setCommentBody("");
        setCollapsedPaths(new Set());
      }, [selectedEvent?.row_id]);
    }

    const event = selectedEvent || null;
    const threads = commentsEnabled && event ? comments?.buildThreads(event.row_id) || [] : [];
    const colorIndex = bookmarksEnabled && event ? bookmarks?.getColor(event.row_id) || 0 : 0;
    const isBookmarked = bookmarksEnabled && event ? bookmarks?.isBookmarked(event.row_id) : false;
    const dataTree = event?.data && typeof event.data === "object" ? buildDataTree(event.data) : null;
    const allExpandablePaths = dataTree ? collectExpandablePaths(dataTree) : [];

    const toggleCollapsed = (path) => {
      setCollapsedPaths((current) => {
        const next = new Set(current);
        if (next.has(path)) next.delete(path);
        else next.add(path);
        return next;
      });
    };

    const setBookmarkColor = (nextColor) => {
      if (!event) return;
      bookmarks?.setColor(event.row_id, Number(nextColor) || 1);
    };

    const submitComment = async () => {
      if (!event) return;
      const body = commentBody.trim();
      if (!body) return;
      const created = await comments?.addComment(event.row_id, body, activeReply);
      if (created) {
        setCommentBody("");
        setActiveReply(null);
      }
    };

    return html`
      <${Fragment}>
        <${DetailPanelHeader} />
        <div class="pane-body" id="event-detail">
          ${!event
            ? html`<${EmptyDetailState} />`
            : html`
                <${DetailEventContent}
                  event=${event}
                  dataTree=${dataTree}
                  allExpandablePaths=${allExpandablePaths}
                  collapsedPaths=${collapsedPaths}
                  onCollapseAll=${setCollapsedPaths}
                  onExpandAll=${() => setCollapsedPaths(new Set())}
                  onToggleCollapsed=${toggleCollapsed}
                  isBookmarked=${isBookmarked}
                  colorIndex=${colorIndex}
                  onSetBookmarkColor=${setBookmarkColor}
                  commentsEnabled=${commentsEnabled}
                  threads=${threads}
                  activeReply=${activeReply}
                  commentBody=${commentBody}
                  onReply=${setActiveReply}
                  onCancelReply=${() => setActiveReply(null)}
                  onCommentInput=${(eventInput) => setCommentBody(eventInput.currentTarget.value)}
                  onSubmitComment=${submitComment}
                />
              `}
        </div>
      </${Fragment}>
    `;
  };

  ui.components.LogDetailPanel = DetailPanelApp;
})();
