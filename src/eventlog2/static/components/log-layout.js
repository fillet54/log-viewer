class LogLayoutElement extends LogAppComponentElement {
  getDirectChild(tagName) {
    const target = String(tagName || "").toUpperCase();
    return Array.from(this.children).find((child) => child.tagName === target) || null;
  }

  captureChildNodes() {
    if (this._capturedChildren) return this._capturedChildren;
    this._capturedChildren = {
      mainViewNode: this.getDirectChild("log-main-view"),
      detailPanelNode: this.getDirectChild("log-detail-panel"),
      searchPanelNode: this.getDirectChild("log-search-panel"),
    };
    return this._capturedChildren;
  }

  renderMountError(message) {
    this.innerHTML = `<div class="empty-panel-message">${String(message || "Unable to load layout shell.")}</div>`;
  }

  mountWithPreact() {
    return this.mountPreactComponent({
      componentName: "LogLayoutShell",
      unavailableMessage: "Local Preact runtime is required for the layout shell.",
      missingComponentMessage: "Layout shell component is not registered.",
      mountErrorMessage: "Unable to load layout shell.",
      getProps: () => ({
        ...this.captureChildNodes(),
        searchReadyVersion: this._searchReadyVersion || 0,
      }),
    });
  }

  connectedCallback() {
    if (!this._onSearchPanelReady) {
      this._onSearchPanelReady = () => {
        this._searchReadyVersion = (this._searchReadyVersion || 0) + 1;
        this._mountController?.render?.();
      };
      this.addEventListener("searchpanel:ready", this._onSearchPanelReady);
    }

    this.connectToApp(() => {
      this.captureChildNodes();
      this.mountWithPreact();
    });
  }

  disconnectedCallback() {
    this.destroyMountedComponent();
  }
}

if (!customElements.get("log-layout")) {
  customElements.define("log-layout", LogLayoutElement);
}
