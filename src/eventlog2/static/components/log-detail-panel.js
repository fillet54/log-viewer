class LogDetailPanelElement extends LogAppComponentElement {
  getServices() {
    return {
      bus: this.getBus(),
      bookmarks: this.getBookmarks(),
      comments: this.getComments(),
    };
  }

  renderMountError(message) {
    this.innerHTML = `
      <div class="pane-header pane-header-spread">
        <div class="pane-header-title">
          <span class="icon">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
              <path stroke-linecap="round" stroke-linejoin="round" d="M3 12h6l3-3 6 6" />
            </svg>
          </span>
          Insights
        </div>
        <button id="toggle-detail" class="button button-ghost button-xs" title="Hide info pane" aria-label="Hide info pane">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" class="tool-icon">
            <path stroke-linecap="round" stroke-linejoin="round" d="M9 6l6 6-6 6" />
          </svg>
        </button>
      </div>
      <div class="pane-body" id="event-detail">
        <div class="empty-panel-message">${String(message || "Unable to load detail panel.")}</div>
      </div>
    `;
  }

  mountWithPreact() {
    const ui = window.EventLog2UI || {};
    const Component = ui.components?.LogDetailPanel || null;

    if (!ui.available) {
      this.renderMountError("Local Preact runtime is required for the detail panel.");
      return false;
    }

    if (typeof ui.createMountController !== "function" || typeof Component !== "function") {
      this.renderMountError("Detail panel component is not registered.");
      return false;
    }

    if (!this._mountController) {
      this._mountController = ui.createMountController({
        host: this,
        Component,
        getServices: () => this.getServices(),
        onError: (error) => {
          console.error(error);
          this.renderMountError(error?.message || "Unable to load detail panel.");
        },
      });
    }

    return this._mountController.render();
  }

  connectedCallback() {
    this.connectToApp(() => {
      this.mountWithPreact();
    });
  }

  disconnectedCallback() {
    this._mountController?.destroy?.();
  }
}

if (!customElements.get("log-detail-panel")) {
  customElements.define("log-detail-panel", LogDetailPanelElement);
}
