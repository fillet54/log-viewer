(function () {
  const ui = window.EventLog2UI || {};
  const html = ui.html;

  ui.components = ui.components || {};

  const ViewerRoot = () => {
    const LayoutShell = ui.components?.LogLayoutShell || null;
    return typeof LayoutShell === "function"
      ? html`<${LayoutShell} />`
      : html`<div class="empty-panel-message">Layout shell component is not registered.</div>`;
  };

  ui.components.LogViewerRoot = ViewerRoot;
})();
