// app.js - Main entry point
import "./state/viewer-store-shared.js";
import "./state/viewer-store-navigation.js";
import "./state/viewer-store-layout.js";
import "./state/viewer-store-search.js";
import "./state/viewer-store-chart.js";
import "./state/viewer-store-activity.js";
import "./state/viewer-store.js";

import { createMountController } from './mount.js';
import { STORAGE_KEYS, queryById, smoothScrollTo } from './shared.js';
import { ViewerRoot } from './logview/ViewerRoot.js';

const loadPageData = () => {
  return window.EVENTLOG2_PAGE_DATA || null;
};

const cloneTemplateNode = (html) => {
  if (typeof html !== "string" || !html.trim()) return null;
  const wrapper = document.createElement("div");
  wrapper.innerHTML = html.trim();
  const template = wrapper.querySelector('template[data-role="row-template"]');
  return template ? template.cloneNode(true) : null;
};

const applyPageView = (root, pageData) => {
  const rowTemplateHtml = pageData?.view?.rowTemplate;
  const template = cloneTemplateNode(rowTemplateHtml);
  if (!template || !root) return;
  root.querySelectorAll('template[data-role="row-template"]').forEach((node) => {
    node.replaceWith(template.cloneNode(true));
  });
};

const loadPageScripts = (root, pageData) => {
  if (!root) return;
  root.querySelectorAll('script[data-role="plugin-view-script"]').forEach((node) => node.remove());
  const scripts = Array.isArray(pageData?.view?.scripts) ? pageData.view.scripts : [];
  scripts.forEach((source) => {
    if (typeof source !== "string" || !source.trim()) return;
    const script = document.createElement("script");
    script.dataset.role = "plugin-view-script";
    script.textContent = source;
    root.appendChild(script);
  });
};

const attachServices = (root, services) => {
  root._services = services;
  return services;
};

class LogViewerAppElement extends HTMLElement {
  isReady() {
    return Boolean(this._services);
  }

  set data(value) {
    if (this._initialized) return;
    this._data = value;
    this.initializeIfReady();
  }

  get data() {
    return this._services?.logData || this._data || null;
  }

  captureRowTemplate(pageData) {
    const fromDom = this.querySelector('template[data-role="row-template"]');
    if (fromDom) return fromDom.cloneNode(true);
    return cloneTemplateNode(pageData?.view?.rowTemplate) || null;
  }

  renderStartupError(message) {
    this.innerHTML = `<div class="empty-panel-message">${String(message || "Unable to load event log viewer.")}</div>`;
  }

  mountWithPreact() {
    const Component = ViewerRoot;

    if (!Component) {
      this.renderStartupError("Log viewer root component is not registered.");
      return false;
    }

    if (!this._mountController) {
      this._mountController = createMountController({
        host: this,
        Component,
        getServices: () => this._services,
        onError: (error) => {
          console.error(error);
          this.renderStartupError(error?.message || "Unable to load event log viewer.");
        },
      });
    }

    return this._mountController.render();
  }

  initializeIfReady() {
    if (this._initialized) return;
    if (!this.isConnected) return;

    const pageData = this._data ?? loadPageData();
    if (!pageData) return;

    try {
      applyPageView(this, pageData);
      loadPageScripts(this, pageData);
      this._data = pageData;
      
      const services = window.LogServices.createRootServices({ pageData });
      services.rowTemplate = this.captureRowTemplate(pageData);
      attachServices(this, services);
      this.mountWithPreact();
      this._initialized = true;
      this.dispatchEvent(new CustomEvent("logapp:ready", { bubbles: true, composed: true }));
    } catch (error) {
      console.error(error);
      this.renderStartupError(error?.message || "Unable to load event log viewer.");
    }
  }

  connectedCallback() {
    if (this._connected) return;
    this._connected = true;

    if (this._data == null) {
      this._data = loadPageData();
    }
    this.initializeIfReady();
  }

  disconnectedCallback() {
    this._mountController?.destroy?.();
  }
}

if (!customElements.get("log-viewer-app")) {
  customElements.define("log-viewer-app", LogViewerAppElement);
}

export { STORAGE_KEYS, queryById, smoothScrollTo };
