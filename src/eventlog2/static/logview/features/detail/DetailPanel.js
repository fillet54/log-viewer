(function () {
  const ui = window.EventLog2UI || {};
  const html = ui.html;
  const Fragment = ui.Fragment;
  const useEffect = ui.hooks?.useEffect || null;
  const useState = ui.hooks?.useState || null;

  ui.components = ui.components || {};

  const DetailPanel = () => {
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
    
    const buildDataTree = ui.utils?.buildDataTree || (() => null);
    const collectExpandablePaths = ui.utils?.collectExpandablePaths || (() => []);
    
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

    const DetailPanelHeader = ui.components.DetailPanelHeader;
    const EmptyDetailState = ui.components.EmptyDetailState;
    const DetailEventContent = ui.components.DetailEventContent;

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

  ui.components.LogDetailPanel = DetailPanel;
})();
