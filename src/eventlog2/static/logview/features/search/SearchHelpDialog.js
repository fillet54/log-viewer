(function () {
  const ui = window.EventLog2UI || {};
  const html = ui.html;

  ui.components = ui.components || {};

  const HELP_EXAMPLES = [
    "name:temp_core",
    "system:Power",
    "color:Red",
    "data.bus.load_pct>=68",
    "description~timeout",
    "system:Power OR system:Thermal",
    "NOT color:Green",
    "$:sensor",
    "$.*:writer",
  ];

  const SearchHelpDialog = ({ dialogRef, fields, onSelectExample }) => html`
    <dialog ref=${dialogRef} id="search-help-dialog" class="search-help-dialog">
      <form method="dialog" class="search-help-card">
        <div class="search-help-header">
          <div>
            <div class="section-label">Search Help</div>
            <div class="search-help-title">Query Syntax</div>
          </div>
          <button class="button button-ghost button-xs" value="close" aria-label="Close search help">Close</button>
        </div>
        <div class="search-help-body">
          <div class="search-help-section">
            <div class="search-help-section-title">Basics</div>
            <div class="support-text">
              Bare terms search across the main event fields and also prefix-match the event <code>name</code>.
              For example, typing <code>temp</code> will match names that start with <code>temp</code> and exact
              matching values in common fields.
            </div>
            <div class="support-text">
              Use <code>field:value</code> for exact field matching and <code>field~text</code> for substring matching.
              Numeric fields support <code>&gt;</code>, <code>&gt;=</code>, <code>&lt;</code>, and <code>&lt;=</code>.
            </div>
            <div class="support-text">
              Boolean logic is supported with <code>AND</code>, <code>OR</code>, and <code>NOT</code>. If you omit an
              operator between filters, it behaves like an <code>AND</code>.
            </div>
            <div class="support-text">
              Use <code>*</code> as a wildcard for text matching. Use <code>$:key</code> to search object key names and
              <code> $.*:value</code> to search deeply across nested object values.
            </div>
          </div>
          <div class="search-help-section">
            <div class="search-help-section-title">Examples</div>
            <div class="search-help-examples">
              ${HELP_EXAMPLES.map(
                (example) => html`
                  <button
                    type="button"
                    class="search-help-example"
                    data-search-example=${example}
                    onClick=${() => onSelectExample(example)}
                  >
                    ${example}
                  </button>
                `
              )}
            </div>
          </div>
          <div class="search-help-section">
            <div class="search-help-section-title">Searchable Columns</div>
            <div class="support-text">
              These are top-level event fields. Fields marked <code>map</code> contain nested object data and can still
              be queried with dotted paths like <code>data.bus.load_pct</code> when needed.
            </div>
            <div class="search-help-fields">
              ${fields.map(
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
        </div>
      </form>
    </dialog>
  `;

  ui.components.SearchHelpDialog = SearchHelpDialog;
})();
