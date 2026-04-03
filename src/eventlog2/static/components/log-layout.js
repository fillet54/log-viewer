class LayoutController {
  constructor(element) {
    this.element = element;
    this.storageKeys = STORAGE_KEYS;
    this.rootLayoutInitialized = false;
    this.topLayoutInitialized = false;
    this.searchLayoutInitialized = false;
    this.bottomToggleInitialized = false;
  }

  getById(id) {
    return queryById(this.element, id);
  }

  observeContainerSize(container, callback) {
    if (!container || typeof callback !== "function") return;
    if (!container._resizeObservers) container._resizeObservers = [];

    let rafId = 0;
    const schedule = () => {
      if (rafId) cancelAnimationFrame(rafId);
      rafId = requestAnimationFrame(() => {
        rafId = 0;
        callback();
      });
    };

    schedule();
    requestAnimationFrame(schedule);

    if (typeof ResizeObserver === "function") {
      const observer = new ResizeObserver(() => schedule());
      observer.observe(container);
      container._resizeObservers.push(observer);
      return;
    }

    window.addEventListener("resize", schedule);
  }

  resolveMinHeight(element, fallback) {
    if (!element) return fallback;
    const computed = Number.parseFloat(getComputedStyle(element).minHeight || "");
    return Number.isFinite(computed) && computed > 0 ? computed : fallback;
  }

  resolveMinWidth(element, fallback) {
    if (!element) return fallback;
    const computed = Number.parseFloat(getComputedStyle(element).minWidth || "");
    return Number.isFinite(computed) && computed > 0 ? computed : fallback;
  }

  applyRootSplitFromStorage() {
    const splitRoot = this.getById("split-root");
    const topPane = this.getById("pane-top");
    const bottomPane = this.getById("pane-bottom");
    if (!splitRoot || !topPane || !bottomPane) return;

    const saved = loadSizes(this.storageKeys.root, [70, 30]);
    const height = splitRoot.clientHeight;
    const gutterSize = 10;
    if (height <= 0) return;

    const minTop = this.resolveMinHeight(topPane, 240);
    const minBottom = this.resolveMinHeight(bottomPane, 120);
    const available = Math.max(1, height - gutterSize);
    const maxTop = Math.max(minTop, available - minBottom);
    const preferredTop = Math.max(minTop, (saved[0] / 100) * available);
    const topPx = Math.min(maxTop, preferredTop);
    const bottomPx = Math.max(minBottom, height - topPx - gutterSize);

    topPane.style.flex = `0 0 ${topPx}px`;
    bottomPane.style.flex = `1 1 ${bottomPx}px`;
  }

  initialize() {
    this.initializePrimaryLayout();
    this.initializeSearchLayout();
  }

  initializePrimaryLayout() {
    if (!this.rootLayoutInitialized) {
      this.initGhostVertical({
        container: this.getById("split-root"),
        top: this.getById("pane-top"),
        bottom: this.getById("pane-bottom"),
        storageKey: this.storageKeys.root,
        minTop: 240,
        minBottom: 120,
        gutterSize: 10,
      });
      this.rootLayoutInitialized = true;
    }

    if (!this.topLayoutInitialized) {
      this.initGhostSplit({
        container: this.getById("split-top"),
        left: this.getById("pane-center"),
        right: this.getById("pane-right"),
        storageKey: this.storageKeys.top,
        minLeft: 420,
        minRight: 240,
        gutterSize: 10,
      });
      this.topLayoutInitialized = true;
    }
  }

  initializeSearchLayout() {
    if (!this.searchLayoutInitialized) {
      const container = this.getById("search-split");
      const left = this.getById("search-history-pane");
      const right = this.getById("search-results-pane");
      if (container && left && right) {
        this.initGhostSplit({
          container,
          left,
          right,
          storageKey: this.storageKeys.search,
          minLeft: 160,
          minRight: 320,
          gutterSize: 8,
        });
        this.searchLayoutInitialized = true;
      }
    }

    if (!this.bottomToggleInitialized) {
      const toggleButton = this.getById("toggle-bottom");
      if (toggleButton) {
        toggleButton.addEventListener("click", () => {
          const next = !(localStorage.getItem(this.storageKeys.bottomCollapsed) === "true");
          localStorage.setItem(this.storageKeys.bottomCollapsed, String(next));
          this.setBottomCollapsed(next);
        });
        this.bottomToggleInitialized = true;
      }
    }

    this.setBottomCollapsed(localStorage.getItem(this.storageKeys.bottomCollapsed) === "true");
  }

  setBottomCollapsed(collapsed) {
    const splitRoot = this.getById("split-root");
    const button = this.getById("toggle-bottom");
    const headerText = this.getById("search-header-text");
    const headerTabs = this.getById("search-header-tabs");
    const topPane = this.getById("pane-top");
    const bottomPane = this.getById("pane-bottom");
    if (!splitRoot || !button || !topPane || !bottomPane) return;

    if (collapsed) {
      splitRoot.classList.add("bottom-collapsed");
      const header = bottomPane.querySelector(".pane-header");
      const headerHeight = header ? header.offsetHeight : 24;
      topPane.style.flex = "1 1 auto";
      bottomPane.style.flex = `0 0 ${headerHeight}px`;
      button.setAttribute("aria-label", "Expand search pane");
      button.classList.add("is-collapsed");
      if (headerText) {
        headerText.textContent = "Search";
        headerText.classList.remove("hidden");
      }
      if (headerTabs) headerTabs.classList.add("hidden");
      return;
    }

    splitRoot.classList.remove("bottom-collapsed");
    const rootSizes = loadSizes(this.storageKeys.root, [70, 30]);
    const savedExpanded = loadSizes(this.storageKeys.rootExpanded, [70, 30]);
    const safeSizes =
      rootSizes[1] > 6 ? rootSizes : savedExpanded[1] > 6 ? savedExpanded : [70, 30];
    saveSizes(this.storageKeys.root, safeSizes);
    this.applyRootSplitFromStorage();
    button.setAttribute("aria-label", "Collapse search pane");
    button.classList.remove("is-collapsed");
    if (headerText) headerText.classList.add("hidden");
    if (headerTabs) headerTabs.classList.remove("hidden");
  }

  initGhostSplit(options) {
    const {
      container,
      left,
      right,
      storageKey,
      minLeft = 200,
      minRight = 200,
      gutterSize = 8,
    } = options;

    if (!container || !left || !right) return;

    const applySizes = (leftPx, width) => {
      const resolvedMinRight = this.resolveMinWidth(right, minRight);
      left.style.flex = `0 0 ${leftPx}px`;
      right.style.flex = `1 1 ${Math.max(resolvedMinRight, width - leftPx - gutterSize)}px`;
    };

    const setFromPercent = () => {
      const saved = loadSizes(storageKey, [70, 30]);
      const width = container.clientWidth;
      if (width <= 0) return;
      const resolvedMinLeft = this.resolveMinWidth(left, minLeft);
      const resolvedMinRight = this.resolveMinWidth(right, minRight);
      const available = Math.max(1, width - gutterSize);
      const maxLeft = Math.max(resolvedMinLeft, available - resolvedMinRight);
      const preferredLeft = Math.max(resolvedMinLeft, (saved[0] / 100) * available);
      const leftPx = Math.min(maxLeft, preferredLeft);
      applySizes(leftPx, width);
    };

    const gutter = document.createElement("div");
    gutter.className = "ghost-gutter";
    gutter.style.width = `${gutterSize}px`;
    container.insertBefore(gutter, right);

    const onMouseDown = (event) => {
      event.preventDefault();
      const rect = container.getBoundingClientRect();
      const width = rect.width;
      const minX = rect.left + minLeft;
      const maxX = rect.right - minRight - gutterSize;
      let currentX = Math.min(maxX, Math.max(minX, event.clientX));

      const ghost = document.createElement("div");
      ghost.className = "ghost-line";
      ghost.style.left = `${currentX - rect.left}px`;
      container.appendChild(ghost);
      document.body.style.cursor = "col-resize";

      const onMove = (moveEvent) => {
        currentX = Math.min(maxX, Math.max(minX, moveEvent.clientX));
        ghost.style.left = `${currentX - rect.left}px`;
      };

      const onUp = () => {
        document.removeEventListener("mousemove", onMove);
        document.removeEventListener("mouseup", onUp);
        document.body.style.cursor = "";
        container.removeChild(ghost);

        const leftPx = currentX - rect.left;
        const available = Math.max(1, width - gutterSize);
        const leftPercent = Math.max(0, Math.min(100, (leftPx / available) * 100));
        saveSizes(storageKey, [leftPercent, 100 - leftPercent]);
        applySizes(leftPx, width);
      };

      document.addEventListener("mousemove", onMove);
      document.addEventListener("mouseup", onUp);
    };

    gutter.addEventListener("mousedown", onMouseDown);
    this.observeContainerSize(container, setFromPercent);
  }

  initGhostVertical(options) {
    const {
      container,
      top,
      bottom,
      storageKey,
      minTop = 200,
      minBottom = 80,
      gutterSize = 10,
    } = options;

    if (!container || !top || !bottom) return;

    const applySizes = (topPx, height) => {
      const resolvedMinBottom = this.resolveMinHeight(bottom, minBottom);
      top.style.flex = `0 0 ${topPx}px`;
      bottom.style.flex = `1 1 ${Math.max(resolvedMinBottom, height - topPx - gutterSize)}px`;
    };

    const setFromPercent = () => {
      const saved = loadSizes(storageKey, [70, 30]);
      const height = container.clientHeight;
      if (height <= 0) return;
      const resolvedMinTop = this.resolveMinHeight(top, minTop);
      const resolvedMinBottom = this.resolveMinHeight(bottom, minBottom);
      const available = Math.max(1, height - gutterSize);
      const maxTop = Math.max(resolvedMinTop, available - resolvedMinBottom);
      const preferredTop = Math.max(resolvedMinTop, (saved[0] / 100) * available);
      const topPx = Math.min(maxTop, preferredTop);
      applySizes(topPx, height);
    };

    const gutter = document.createElement("div");
    gutter.className = "ghost-gutter ghost-gutter-vertical";
    gutter.style.height = `${gutterSize}px`;
    container.insertBefore(gutter, bottom);

    const onMouseDown = (event) => {
      event.preventDefault();
      const rect = container.getBoundingClientRect();
      const height = rect.height;
      const minY = rect.top + minTop;
      const maxY = rect.bottom - minBottom - gutterSize;
      let currentY = Math.min(maxY, Math.max(minY, event.clientY));

      const ghost = document.createElement("div");
      ghost.className = "ghost-line ghost-line-horizontal";
      ghost.style.top = `${currentY - rect.top}px`;
      container.appendChild(ghost);
      document.body.style.cursor = "row-resize";

      const onMove = (moveEvent) => {
        currentY = Math.min(maxY, Math.max(minY, moveEvent.clientY));
        ghost.style.top = `${currentY - rect.top}px`;
      };

      const onUp = () => {
        document.removeEventListener("mousemove", onMove);
        document.removeEventListener("mouseup", onUp);
        document.body.style.cursor = "";
        container.removeChild(ghost);

        const topPx = currentY - rect.top;
        const available = Math.max(1, height - gutterSize);
        const topPercent = Math.max(0, Math.min(100, (topPx / available) * 100));
        const bottomPercent = 100 - topPercent;
        saveSizes(storageKey, [topPercent, bottomPercent]);
        applySizes(topPx, height);
        localStorage.setItem(this.storageKeys.bottomCollapsed, String(bottomPercent <= 6));
      };

      document.addEventListener("mousemove", onMove);
      document.addEventListener("mouseup", onUp);
    };

    gutter.addEventListener("mousedown", onMouseDown);
    this.observeContainerSize(container, setFromPercent);
  }
}

class LogLayoutElement extends LogAppComponentElement {
  getDirectChild(tagName) {
    const target = String(tagName || "").toUpperCase();
    return Array.from(this.children).find((child) => child.tagName === target) || null;
  }

  renderShell() {
    const mainView = this.getDirectChild("log-main-view");
    const detailPanel = this.getDirectChild("log-detail-panel");
    const searchPanel = this.getDirectChild("log-search-panel");

    this.innerHTML = `
      <div id="split-root" class="layout-shell ghost-split">
        <section id="pane-top" class="layout-top">
          <div id="split-top" class="layout-top-inner ghost-split"></div>
        </section>
      </div>
    `;
    const splitTop = queryById(this, "split-top");
    const splitRoot = queryById(this, "split-root");

    if (mainView) {
      mainView.id = "pane-center";
      mainView.classList.add("pane");
      splitTop?.appendChild(mainView);
    }

    if (detailPanel) {
      detailPanel.id = "pane-right";
      detailPanel.classList.add("pane");
      splitTop?.appendChild(detailPanel);
    }

    if (searchPanel) {
      searchPanel.id = "pane-bottom";
      searchPanel.classList.add("pane");
      splitRoot?.appendChild(searchPanel);
    }
  }

  connectedCallback() {
    if (!this._onSearchPanelReady) {
      this._onSearchPanelReady = () => {
        this.controller?.initializeSearchLayout();
      };
      this.addEventListener("searchpanel:ready", this._onSearchPanelReady);
    }

    this.connectToApp(() => {
      if (!this.querySelector("#split-root")) {
        this.renderShell();
      }
      this.controller = this.controller || new LayoutController(this);
      this.controller.initialize();
    });
  }
}

if (!customElements.get("log-layout")) {
  customElements.define("log-layout", LogLayoutElement);
}
