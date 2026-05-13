import { html } from "logview/lib";
import { EventSummary } from "./EventSummary.js";
import { DataTreeSection } from "./DataTreeSection.js";
import { BookmarkSection } from "./BookmarkSection.js";
import { CommentSection } from "./CommentSection.js";

export const DetailEventContent = ({
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

window.EventLog2UI = window.EventLog2UI || {};
window.EventLog2UI.components = window.EventLog2UI.components || {};
window.EventLog2UI.components.DetailEventContent = DetailEventContent;
