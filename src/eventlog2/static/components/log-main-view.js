class LogMainViewElement extends LogAppComponentElement {
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
      <div class="main-view-shell">
        <div class="main-view-toolbar">
          <div class="chart-type-picker">
            <select class="text-input text-input-small chart-type-select" disabled aria-label="Chart type">
              <option>Unavailable</option>
            </select>
          </div>
          <div class="chart-command-bar"></div>
          <div class="view-mode-toggle" role="tablist" aria-label="Main view layout"></div>
        </div>
        <div class="main-view-stack" id="main-view-stack" data-mode="list">
          <section class="main-view-region main-view-log-region" id="log-region" style="flex: 1 1 auto;">
            <div class="pane-body log-body" id="log-body">
              <div class="empty-panel-message">${String(message || "Unable to load main view.")}</div>
            </div>
          </section>
        </div>
      </div>
    `;
  }

  mountWithPreact() {
    return this.mountPreactComponent({
      componentName: "LogMainViewShell",
      unavailableMessage: "Local Preact runtime is required for the main view.",
      missingComponentMessage: "Main view component is not registered.",
      mountErrorMessage: "Unable to load main view.",
      getServices: () => this.getServices(),
    });
  }

  connectedCallback() {
    this.connectToApp(() => {
      this.captureRowTemplate();
      this.mountWithPreact();
    });
  }

  disconnectedCallback() {
    this.destroyMountedComponent();
  }
}

if (!customElements.get("log-main-view")) {
  customElements.define("log-main-view", LogMainViewElement);
}
