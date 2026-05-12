(function () {
  const ui = window.EventLog2UI || {};
  const html = ui.html;

  ui.components = ui.components || {};

  const DataTreeSection = ({
    dataTree,
    allExpandablePaths,
    collapsedPaths,
    onCollapseAll,
    onExpandAll,
    onToggle,
  }) => {
    if (!dataTree) return html`<div class="support-text">No event data available.</div>`;
    const DataTreeNode = ui.components.DataTreeNode;

    return html`
      <div class="event-data">
        <div class="event-data-header">
          <div class="section-label">Log Data</div>
          <div class="event-data-actions">
            <button
              type="button"
              class="button button-ghost button-xs"
              id="collapse-all-data"
              onClick=${() => onCollapseAll(new Set(allExpandablePaths))}
            >
              Collapse All
            </button>
            <button
              type="button"
              class="button button-ghost button-xs"
              id="expand-all-data"
              onClick=${onExpandAll}
            >
              Expand All
            </button>
          </div>
        </div>
        <div class="data-tree">
          <${DataTreeNode} node=${dataTree} collapsedPaths=${collapsedPaths} onToggle=${onToggle} />
        </div>
      </div>
    `;
  };

  ui.components.DataTreeSection = DataTreeSection;
})();
