(function () {
  const ui = window.EventLog2UI || {};
  const html = ui.html;

  ui.components = ui.components || {};

  const EmptyDetailState = () => html`<div class="empty-panel-message">Select a log event to view details.</div>`;

  ui.components.EmptyDetailState = EmptyDetailState;
})();
