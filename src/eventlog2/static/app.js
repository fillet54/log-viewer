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

const getAppRoot = (node) => {
  if (!node || typeof node.closest !== "function") return null;
  return node.closest("log-viewer-app");
};

const connectComponent = (element, setup) => {
  if (!element || typeof setup !== "function") return;
  const root = getAppRoot(element);
  if (!root) return;

  const initialize = () => {
    if (element._initialized) return true;
    if (typeof root.isReady !== "function" || !root.isReady()) return false;
    setup();
    element._initialized = true;
    return true;
  };

  if (initialize()) return;

  const onReady = () => {
    if (!initialize()) return;
    root.removeEventListener("logapp:ready", onReady);
  };
  root.addEventListener("logapp:ready", onReady);
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

  renderStartupError(message) {
    this.innerHTML = `<div class="empty-panel-message">${String(message || "Unable to load event log viewer.")}</div>`;
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
      this._initialized = true;
      attachServices(this, LogServices.createRootServices({ pageData }));
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
}

if (!customElements.get("log-viewer-app")) {
  customElements.define("log-viewer-app", LogViewerAppElement);
}

class LogAppComponentElement extends HTMLElement {
  queryById(id) {
    return queryById(this, id);
  }

  connectToApp(setup) {
    connectComponent(this, setup);
  }

  getAppRoot() {
    return getAppRoot(this);
  }

  getBus() {
    return this.getAppRoot()?.getBus() || null;
  }

  getLogData() {
    return this.getAppRoot()?.getLogData() || null;
  }

  getSearchWorker() {
    return this.getAppRoot()?.getSearchWorker() || null;
  }

  getBookmarks() {
    return this.getAppRoot()?.getBookmarks() || null;
  }

  getComments() {
    return this.getAppRoot()?.getComments() || null;
  }

  getPlugin() {
    return this.getAppRoot()?.getPlugin() || null;
  }

  getView() {
    return this.getAppRoot()?.getView() || null;
  }
}
