const STORAGE_KEYS = {
  root: "loglayout.split.root",
  top: "loglayout.split.top",
  detailCollapsed: "loglayout.split.detail.collapsed",
  bottomCollapsed: "loglayout.split.bottom.collapsed",
  search: "loglayout.split.search",
  rootExpanded: "loglayout.split.root.expanded",
  searchHistory: "loglayout.search.history",
  searchPinned: "loglayout.search.pinned",
  searchFilters: "loglayout.search.filters",
  chartTooltips: "loglayout.chart.tooltips",
  chartType: "loglayout.chart.type",
  mainViewMode: "loglayout.mainview.mode",
  mainViewSplit: "loglayout.mainview.split",
  bookmarks: "loglayout.bookmarks",
  comments: "loglayout.comments",
};

const loadSizes = (key, fallback) => {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return fallback;
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed) || parsed.length !== fallback.length) return fallback;
    if (parsed.some((value) => typeof value !== "number")) return fallback;
    return parsed;
  } catch (err) {
    return fallback;
  }
};

const saveSizes = (key, sizes) => {
  localStorage.setItem(key, JSON.stringify(sizes));
};

const queryById = (root, id) => {
  if (!root || !id) return null;
  if (typeof root.getElementById === "function") return root.getElementById(id);
  if (typeof root.querySelector === "function") return root.querySelector(`#${id}`);
  return null;
};

window.EventLog2 = window.EventLog2 || {};
window.EventLog2._pendingViewRegistrations = window.EventLog2._pendingViewRegistrations || [];
window.EventLog2._rowRenderers = window.EventLog2._rowRenderers || new Map();
window.EventLog2.registerRowRenderer = (renderer) => {
  if (typeof renderer !== "function") throw new Error("Row renderer must be a function.");
  window.EventLog2._rowRenderers.set("global", renderer);
  return renderer;
};
window.EventLog2.registerPluginRowRenderer = (pluginId, renderer) => {
  const normalizedPluginId = String(pluginId || "").trim();
  if (!normalizedPluginId) throw new Error("Plugin row renderers must define a plugin id.");
  if (typeof renderer !== "function") throw new Error("Row renderer must be a function.");
  window.EventLog2._rowRenderers.set(normalizedPluginId, renderer);
  return renderer;
};
window.EventLog2.resolveRowRenderer = (plugin) => {
  const pluginId =
    typeof plugin === "string"
      ? String(plugin).trim()
      : String(plugin?.id || plugin?.pluginId || "").trim();
  return window.EventLog2._rowRenderers.get(pluginId) || window.EventLog2._rowRenderers.get("global") || null;
};
window.EventLog2.registerChartType = (definition) => {
  if (!window.LogMainViewChart?.registerType) {
    window.EventLog2._pendingViewRegistrations.push({ kind: "chart", pluginId: null, definition });
    return;
  }
  return window.LogMainViewChart.registerType(definition);
};
window.EventLog2.registerPluginChartType = (pluginId, definition) => {
  if (!window.LogMainViewChart?.registerPluginType) {
    window.EventLog2._pendingViewRegistrations.push({ kind: "chart", pluginId, definition });
    return;
  }
  return window.LogMainViewChart.registerPluginType(pluginId, definition);
};
window.EventLog2.registerTimelineView = (definition) => {
  if (!window.LogMainViewTimeline?.registerView) {
    window.EventLog2._pendingViewRegistrations.push({ kind: "timeline", pluginId: null, definition });
    return;
  }
  return window.LogMainViewTimeline.registerView(definition);
};
window.EventLog2.registerPluginTimelineView = (pluginId, definition) => {
  if (!window.LogMainViewTimeline?.registerPluginView) {
    window.EventLog2._pendingViewRegistrations.push({ kind: "timeline", pluginId, definition });
    return;
  }
  return window.LogMainViewTimeline.registerPluginView(pluginId, definition);
};

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

const smoothScrollTo = (container, targetTop, durationMs = 200, onComplete = null) => {
  const startTop = container.scrollTop;
  const delta = targetTop - startTop;
  if (Math.abs(delta) < 2) {
    container.scrollTop = targetTop;
    if (onComplete) onComplete();
    return;
  }
  const startTime = performance.now();
  const easeOut = (t) => 1 - Math.pow(1 - t, 3);
  const step = (now) => {
    const elapsed = now - startTime;
    const progress = Math.min(1, elapsed / durationMs);
    container.scrollTop = startTop + delta * easeOut(progress);
    if (progress < 1) {
      requestAnimationFrame(step);
    } else if (onComplete) {
      onComplete();
    }
  };
  requestAnimationFrame(step);
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

  getBus() {
    return this._services?.bus || null;
  }

  getLogData() {
    return this._services?.logData || null;
  }

  getSearchWorker() {
    return this._services?.searchWorker || null;
  }

  getBookmarks() {
    return this._services?.bookmarks || null;
  }

  getComments() {
    return this._services?.comments || null;
  }

  getPlugin() {
    return this._services?.plugin || null;
  }

  getView() {
    return this._services?.view || null;
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
    const ui = window.EventLog2UI || {};
    const Component = ui.components?.LogViewerRoot || null;

    if (!ui.available) {
      this.renderStartupError("Local Preact runtime is required for the log viewer.");
      return false;
    }

    if (typeof ui.createMountController !== "function" || typeof Component !== "function") {
      this.renderStartupError("Log viewer root component is not registered.");
      return false;
    }

    if (!this._mountController) {
      this._mountController = ui.createMountController({
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
      const services = LogServices.createRootServices({ pageData });
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
