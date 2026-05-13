import { html } from "logview/lib";

export const ReadOnlyCommentThread = ({ threads, depth = 0 }) => {
    if (!threads.length) return null;

    return html`
      <div class="comment-thread">
        ${threads.map(
          (comment) => html`
            <div class="comment-item" style=${{ marginLeft: `${depth * 16}px` }}>
              <div class="comment-meta">
                <span class="comment-time">${comment.created_at}</span>
              </div>
              <div class="comment-body">${comment.body}</div>
              ${comment.replies?.length
                ? html`<${ReadOnlyCommentThread} threads=${comment.replies} depth=${depth + 1} />`
                : null}
            </div>
          `
        )}
      </div>
    `;
};
