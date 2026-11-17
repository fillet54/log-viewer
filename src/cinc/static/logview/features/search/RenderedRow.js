import { html } from "logview/lib";
import { PluginLogRow } from "../rows/PluginLogRow.js";

export const RenderedRow = ({
    event,
    services,
    events,
    activityEnabled,
    version,
    bookmarkState,
    commentState,
    className = "",
  }) => {
    return html`
      <div class=${className}>
        <${PluginLogRow}
          event=${event}
          services=${services}
          events=${events}
          extraClasses=${["search-result-row"]}
          activityEnabled=${activityEnabled}
          jumpOnSelect=${true}
        />
      </div>
    `;
};
