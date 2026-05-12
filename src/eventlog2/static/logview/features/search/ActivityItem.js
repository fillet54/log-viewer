(function () {
  const ui = window.EventLog2UI || {};
  const html = ui.html;

  ui.components = ui.components || {};

  const ActivityItem = ({
    event,
    services,
    events,
    comments,
    activityEnabled,
    version,
    bookmarkState,
    commentState,
  }) => {
    const RenderedRow = ui.components.RenderedRow;
    const ReadOnlyCommentThread = ui.components.ReadOnlyCommentThread;
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

  ui.components.ActivityItem = ActivityItem;
})();
