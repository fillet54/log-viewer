class LogSearchPanelElement extends LogAppComponentElement {
  captureRowTemplate() {
    if (this._rowTemplate) return this._rowTemplate;
    const template = this.querySelector('template[data-role="row-template"]');
    this._rowTemplate = template ? template.cloneNode(true) : null;
    return this._rowTemplate;
  }

  getServices() {
    return {
      bus: this.getBus(),
      logData: this.getLogData(),
      plugin: this.getPlugin(),
      view: this.getView(),
      rowTemplate: this.captureRowTemplate(),
      searchWorker: this.getSearchWorker(),
      bookmarks: this.getBookmarks(),
      comments: this.getComments(),
    };
  }

  renderMountError(message) {
    this.innerHTML = `
      <div class="pane-header compact-header pane-header-spread">
        <span id="search-header-text" class="section-label">Search</span>
        <div id="search-header-tabs" class="search-tabs hidden"></div>
        <button id="toggle-bottom" class="button button-ghost button-xs" title="Toggle search pane">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="tool-icon">
            <path stroke-linecap="round" stroke-linejoin="round" d="M6 10l6 6 6-6" />
          </svg>
        </button>
      </div>
      <div class="pane-body search-pane">
        <div id="search-results" class="search-list search-results-list">
          <div class="empty-panel-message">${String(message || "Unable to load search panel.")}</div>
        </div>
      </div>
    `;
  }

  mountWithPreact() {
    const ui = window.EventLog2UI || {};
    const Component = ui.components?.LogSearchPanel || null;

    if (!ui.available) {
      this.renderMountError("Local Preact runtime is required for the search panel.");
      return false;
    }

    if (typeof ui.createMountController !== "function" || typeof Component !== "function") {
      this.renderMountError("Search panel component is not registered.");
      return false;
    }

    if (!this._mountController) {
      this._mountController = ui.createMountController({
        host: this,
        Component,
        getServices: () => this.getServices(),
        onError: (error) => {
          console.error(error);
          this.renderMountError(error?.message || "Unable to load search panel.");
        },
      });
    }

    return this._mountController.render();
  }

  connectedCallback() {
    this.connectToApp(() => {
      this.captureRowTemplate();
      this.mountWithPreact();
      this.dispatchEvent(new CustomEvent("searchpanel:ready", { bubbles: true, composed: true }));
    });
  }

  disconnectedCallback() {
    this._mountController?.destroy?.();
  }
}

if (!customElements.get("log-search-panel")) {
  customElements.define("log-search-panel", LogSearchPanelElement);
}
