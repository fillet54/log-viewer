import { html } from "logview/lib";
import { CommentThread } from "./CommentThread.js";

export const CommentSection = ({
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

window.EventLog2UI = window.EventLog2UI || {};
window.EventLog2UI.components = window.EventLog2UI.components || {};
window.EventLog2UI.components.CommentSection = CommentSection;
