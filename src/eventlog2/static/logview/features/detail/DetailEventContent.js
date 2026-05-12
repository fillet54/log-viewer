(function () {
  const ui = window.EventLog2UI || {};
  const html = ui.html;

  ui.components = ui.components || {};

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
  }) => {
    const EventSummary = ui.components.EventSummary;
    const DataTreeSection = ui.components.DataTreeSection;
    const BookmarkSection = ui.components.BookmarkSection;
    const CommentSection = ui.components.CommentSection;

    return html`
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
  };

  ui.components.DetailEventContent = DetailEventContent;
})();
