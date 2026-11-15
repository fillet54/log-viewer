import { html } from "logview/lib";
import { EXAMPLES, RULES, SYNTAX } from "../../../services/search-help.js";

// Text with `code` spans.
const withCode = (text) =>
  String(text)
    .split("`")
    .map((part, index) => (index % 2 ? html`<code>${part}</code>` : part));

const Example = ({ query, onSelectExample }) => html`
  <button
    type="button"
    class="search-help-example"
    data-search-example=${query}
    title="Run this search"
    onClick=${() => onSelectExample(query)}
  >
    ${query}
  </button>
`;

// fieldGroups: [{id, name, fields: [{name, nested}]}], one per log type in the open log.
export const SearchHelpDialog = ({ dialogRef, fieldGroups = [], onSelectExample }) => html`
  <dialog ref=${dialogRef} id="search-help-dialog" class="search-help-dialog" aria-labelledby="search-help-title">
    <form method="dialog" class="search-help-card">
      <div class="search-help-header">
        <div>
          <div class="section-label">Search help</div>
          <div class="search-help-title" id="search-help-title">Query syntax</div>
        </div>
        <button class="button button-ghost button-xs" value="close" aria-label="Close search help">Close</button>
      </div>
      <div class="search-help-body">
        <p class="support-text">
          Type a query and press Enter; matching rows are listed below the log. Pin a search to keep it, or
          make it a filter to narrow the log itself. Click any example here to run it.
        </p>

        <section class="search-help-section">
          <h3 class="search-help-section-title">At a glance</h3>
          <table class="search-help-table">
            <tbody>
              ${SYNTAX.map(
                ([syntax, meaning, example]) => html`
                  <tr>
                    <td><code>${syntax}</code></td>
                    <td>${meaning}</td>
                    <td><${Example} query=${example} onSelectExample=${onSelectExample} /></td>
                  </tr>
                `
              )}
            </tbody>
          </table>
        </section>

        <section class="search-help-section">
          <h3 class="search-help-section-title">Examples</h3>
          <div class="search-help-groups">
            ${EXAMPLES.map(
              (group) => html`
                <div class="search-help-group">
                  <h4 class="search-help-group-title">${group.title}</h4>
                  <ul class="search-help-list">
                    ${group.items.map(
                      ([query, note]) => html`
                        <li>
                          <${Example} query=${query} onSelectExample=${onSelectExample} />
                          <span class="search-help-note">${note}</span>
                        </li>
                      `
                    )}
                  </ul>
                </div>
              `
            )}
          </div>
        </section>

        <section class="search-help-section">
          <h3 class="search-help-section-title">How matching works</h3>
          <dl class="search-help-rules">
            ${RULES.map(
              ([title, text]) => html`
                <div class="search-help-rule">
                  <dt>${title}</dt>
                  <dd>${withCode(text)}</dd>
                </div>
              `
            )}
          </dl>
        </section>

        <section class="search-help-section">
          <h3 class="search-help-section-title">Fields in this log</h3>
          <p class="support-text">
            Top-level fields of each row type. Fields marked <code>map</code> hold nested data: search them with a
            dotted path (<code>data.bus.load_pct</code>), <code>~</code>, or <code>$</code>.
          </p>
          ${fieldGroups.map(
            (group) => html`
              <div class="search-help-field-group">
                <h4 class="search-help-group-title">
                  ${group.name} <code class="search-help-type-id">log_type:${group.id}</code>
                </h4>
                <div class="search-help-fields">
                  ${group.fields.map(
                    (field) => html`
                      <span class="search-help-field">
                        <code>${field.name}</code>${field.nested
                          ? html`<span class="search-help-field-badge">map</span>`
                          : null}
                      </span>
                    `
                  )}
                </div>
              </div>
            `
          )}
        </section>
      </div>
    </form>
  </dialog>
`;
