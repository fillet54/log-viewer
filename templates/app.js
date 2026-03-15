const IS_LOGGED_IN = {{ "true" if current_user else "false" }};

const STORAGE_KEYS = {
  root: "loglayout.split.root",
  top: "loglayout.split.top",
  bottomCollapsed: "loglayout.split.bottom.collapsed",
  search: "loglayout.split.search",
  rootExpanded: "loglayout.split.root.expanded",
  searchHistory: "loglayout.search.history",
  searchPinned: "loglayout.search.pinned",
  searchFilters: "loglayout.search.filters",
  chartTooltips: "loglayout.chart.tooltips",
  bookmarks: "loglayout.bookmarks",
  bookmarkNotes: "loglayout.bookmark.notes",
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

const parseJsonScript = (scriptElement) => {
  if (!scriptElement) return null;
  try {
    return JSON.parse(scriptElement.textContent || "{}");
  } catch (err) {
    return null;
  }
};

const loadInitialData = (root) => {
  if (!root || typeof root.querySelector !== "function") return null;
  const scopedPayload = root.querySelector('script[data-role="initial-data"]');
  return parseJsonScript(scopedPayload);
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

{% include 'components/services/search.js' %}

{% include 'components/shared/log_row_helper.js' %}

{% include 'components/services/app_services.js' %}

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

  isLoggedIn() {
    return IS_LOGGED_IN;
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

  initializeIfReady() {
    if (this._initialized) return;
    if (!this.isConnected) return;

    const logData = this._data ?? loadInitialData(this);
    if (!logData) return;

    this._data = logData;
    this._initialized = true;
    attachServices(
      this,
      LogServices.createRootServices({
        logData,
        isLoggedIn: this.isLoggedIn(),
      })
    );
    this.dispatchEvent(new CustomEvent("logapp:ready", { bubbles: true, composed: true }));
  }

  connectedCallback() {
    if (this._connected) return;
    this._connected = true;

    if (this._data == null) {
      this._data = loadInitialData(this);
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

  isLoggedIn() {
    return this.getAppRoot()?.isLoggedIn() || false;
  }
}

{% include 'components/layout/element.js' %}

{% include 'components/main_view/chart.js' %}

{% include 'components/main_view/element.js' %}

{% include 'components/detail_panel/element.js' %}

{% include 'components/search_panel/element.js' %}
