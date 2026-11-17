import { html } from "logview/lib";

export const CommentThread = ({ threads, depth = 0, onReply }) => {
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
