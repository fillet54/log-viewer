import { html } from "logview/lib";
import { RenderedRow } from "./RenderedRow.js";
import { ReadOnlyCommentThread } from "./ReadOnlyCommentThread.js";

export const ActivityItem = ({
    event,
    services,
    events,
    comments,
    activityEnabled,
    version,
    bookmarkState,
    commentState,
  }) => {
    const threads = comments?.buildThreads(event.row_id) || [];

    return html`
      <div class="activity-item">
        <${RenderedRow}
          event=${event}
          services=${services}
          events=${events}
          activityEnabled=${activityEnabled}
          version=${version}
          bookmarkState=${bookmarkState}
          commentState=${commentState}
        />
        ${threads.length
          ? html`
              <div class="activity-thread">
                <${ReadOnlyCommentThread} threads=${threads} />
              </div>
            `
          : null}
      </div>
    `;
  };
