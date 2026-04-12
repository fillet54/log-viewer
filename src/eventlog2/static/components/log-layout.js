class LayoutController {
  constructor(element) {
    this.element = element;
    this.storageKeys = STORAGE_KEYS;
    this.rootSplit = null;
    this.topSplit = null;
    this.bound = false;
  }

  getById(id) {
    return queryById(this.element, id);
  }

  destroySplit(name) {
    const instance = this[name];
    if (!instance || typeof instance.destroy !== "function") return;
    instance.destroy();
    this[name] = null;
  }

  buildSplitOptions({ direction = "horizontal", gutterSize = 10, onDragEnd = null, minSize = undefined, sizes = undefined }) {
    return {
      direction,
      gutterSize,
      sizes,
      minSize,
      elementStyle: (dimension, size, gutterSizeValue) => ({
        "flex-basis": `calc(${size}% - ${gutterSizeValue}px)`,
      }),
      gutterStyle: (dimension, gutterSizeValue) => ({
        "flex-basis": `${gutterSizeValue}px`,
      }),
      onDragEnd,
    };
  }

  bindEvents() {
    if (this.bound) return;
    this.bound = true;

    this.element.addEventListener("click", (event) => {
      const toggleBottom = event.target.closest("#toggle-bottom");
      const collapsedSearchHeader =
        event.target.closest("#pane-bottom .pane-header") &&
        this.getById("split-root")?.classList.contains("bottom-collapsed");
      if (toggleBottom || collapsedSearchHeader) {
        const next = !(localStorage.getItem(this.storageKeys.bottomCollapsed) === "true");
        localStorage.setItem(this.storageKeys.bottomCollapsed, String(next));
        this.setBottomCollapsed(next);
        return;
      }

      const toggleDetail = event.target.closest("#toggle-detail, #toggle-detail-restore");
      if (toggleDetail) {
        const next = !(localStorage.getItem(this.storageKeys.detailCollapsed) === "true");
        localStorage.setItem(this.storageKeys.detailCollapsed, String(next));
        this.setDetailCollapsed(next);
      }
    });
  }

  initialize() {
    this.bindEvents();
    this.setDetailCollapsed(localStorage.getItem(this.storageKeys.detailCollapsed) === "true");
    this.setBottomCollapsed(localStorage.getItem(this.storageKeys.bottomCollapsed) === "true");
  }

  createTopSplit() {
    const centerPane = this.getById("pane-center");
    const rightPane = this.getById("pane-right");
    if (!centerPane || !rightPane || typeof Split !== "function") return;

    this.destroySplit("topSplit");
    rightPane.style.display = "flex";
    centerPane.style.flex = "";
    rightPane.style.flex = "";

    this.topSplit = Split([centerPane, rightPane], {
      ...this.buildSplitOptions({
        direction: "horizontal",
        sizes: loadSizes(this.storageKeys.top, [72, 28]),
        minSize: [420, 240],
        gutterSize: 10,
        onDragEnd: (sizes) => {
          saveSizes(this.storageKeys.top, sizes);
        },
      }),
    });
  }

  createRootSplit() {
    const topPane = this.getById("pane-top");
    const bottomPane = this.getById("pane-bottom");
    if (!topPane || !bottomPane || typeof Split !== "function") return;

    this.destroySplit("rootSplit");
    topPane.style.flex = "";
    bottomPane.style.flex = "";

    const saved = loadSizes(this.storageKeys.root, [70, 30]);
    this.rootSplit = Split([topPane, bottomPane], {
      ...this.buildSplitOptions({
        direction: "vertical",
        sizes: saved,
        minSize: [240, 120],
        gutterSize: 10,
        onDragEnd: (sizes) => {
          saveSizes(this.storageKeys.root, sizes);
          if (sizes[1] > 6) saveSizes(this.storageKeys.rootExpanded, sizes);
        },
      }),
    });
  }

  setDetailCollapsed(collapsed) {
    const splitTop = this.getById("split-top");
    const centerPane = this.getById("pane-center");
    const rightPane = this.getById("pane-right");
    const headerButton = this.getById("toggle-detail");
    const restoreButton = this.getById("toggle-detail-restore");
    if (!splitTop || !centerPane || !rightPane) return;

    if (collapsed) {
      this.destroySplit("topSplit");
      splitTop.classList.add("detail-collapsed");
      centerPane.style.flex = "1 1 auto";
      rightPane.style.display = "none";
      rightPane.style.flex = "0 0 0";
      if (headerButton) {
        headerButton.setAttribute("aria-label", "Show info pane");
        headerButton.setAttribute("title", "Show info pane");
      }
      if (restoreButton) {
        restoreButton.classList.add("is-collapsed");
        restoreButton.setAttribute("aria-label", "Show info pane");
        restoreButton.setAttribute("title", "Show info pane");
      }
      requestAnimationFrame(() => window.dispatchEvent(new Event("resize")));
      return;
    }

    splitTop.classList.remove("detail-collapsed");
    if (restoreButton) restoreButton.classList.remove("is-collapsed");
    if (headerButton) {
      headerButton.setAttribute("aria-label", "Hide info pane");
      headerButton.setAttribute("title", "Hide info pane");
    }
    if (restoreButton) {
      restoreButton.setAttribute("aria-label", "Show info pane");
      restoreButton.setAttribute("title", "Show info pane");
    }
    this.createTopSplit();
    requestAnimationFrame(() => window.dispatchEvent(new Event("resize")));
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
      this.destroySplit("rootSplit");
      splitRoot.classList.add("bottom-collapsed");
      const header = bottomPane.querySelector(".pane-header");
      const headerHeight = header ? header.offsetHeight : 34;
      topPane.style.flex = "1 1 auto";
      bottomPane.style.flex = `0 0 ${headerHeight}px`;
      button.setAttribute("aria-label", "Expand search pane");
      button.classList.add("is-collapsed");
      if (headerText) {
        headerText.textContent = "Search";
        headerText.classList.remove("hidden");
      }
      if (headerTabs) headerTabs.classList.add("hidden");
      requestAnimationFrame(() => window.dispatchEvent(new Event("resize")));
      return;
    }

    splitRoot.classList.remove("bottom-collapsed");
    button.setAttribute("aria-label", "Collapse search pane");
    button.classList.remove("is-collapsed");
    if (headerText) headerText.classList.add("hidden");
    if (headerTabs) headerTabs.classList.remove("hidden");

    const rootSizes = loadSizes(this.storageKeys.root, [70, 30]);
    const savedExpanded = loadSizes(this.storageKeys.rootExpanded, [70, 30]);
    const safeSizes = rootSizes[1] > 6 ? rootSizes : savedExpanded[1] > 6 ? savedExpanded : [70, 30];
    saveSizes(this.storageKeys.root, safeSizes);
    this.createRootSplit();
    requestAnimationFrame(() => window.dispatchEvent(new Event("resize")));
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
      <div id="split-root" class="layout-shell">
        <section id="pane-top" class="layout-top">
          <div id="split-top" class="layout-top-inner"></div>
          <button id="toggle-detail-restore" class="button button-ghost button-xs top-pane-toggle" title="Show info pane" aria-label="Show info pane">
            <span class="top-pane-toggle-rail" aria-hidden="true">
              <span class="top-pane-toggle-rail-chevron">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8">
                  <path stroke-linecap="round" stroke-linejoin="round" d="M15 6l-6 6 6 6" />
                </svg>
              </span>
              <span class="top-pane-toggle-label">INFO</span>
            </span>
          </button>
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
        this.controller?.setBottomCollapsed(localStorage.getItem(STORAGE_KEYS.bottomCollapsed) === "true");
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
