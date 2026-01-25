window.LogApp = window.LogApp || {};

LogApp.initSplits = () => {
  const { STORAGE_KEYS, loadSizes, saveSizes } = LogApp;

  const setBottomCollapsed = (collapsed) => {
    const root = document.getElementById("split-root");
    const button = document.getElementById("toggle-bottom");
    const headerText = document.getElementById("search-header-text");
    const headerTabs = document.getElementById("search-header-tabs");
    const topPane = document.getElementById("pane-top");
    const bottomPane = document.getElementById("pane-bottom");
    if (!root || !button || !topPane || !bottomPane) return;

    if (collapsed) {
      root.classList.add("bottom-collapsed");
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
    } else {
      root.classList.remove("bottom-collapsed");
      const rootSizes = loadSizes(STORAGE_KEYS.root, [70, 30]);
      const savedExpanded = loadSizes(STORAGE_KEYS.rootExpanded, [70, 30]);
      const safeSizes =
        rootSizes[1] > 6 ? rootSizes : savedExpanded[1] > 6 ? savedExpanded : [70, 30];
      topPane.style.flex = `0 0 ${safeSizes[0]}%`;
      bottomPane.style.flex = `0 0 ${safeSizes[1]}%`;
      button.setAttribute("aria-label", "Collapse search pane");
      button.classList.remove("is-collapsed");
      if (headerText) headerText.classList.add("hidden");
      if (headerTabs) headerTabs.classList.remove("hidden");
    }
  };

  const initGhostSplit = (options) => {
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
      left.style.flex = `0 0 ${leftPx}px`;
      right.style.flex = `1 1 ${Math.max(minRight, width - leftPx - gutterSize)}px`;
    };

    const setFromPercent = () => {
      const saved = loadSizes(storageKey, [70, 30]);
      const width = container.clientWidth;
      const available = Math.max(1, width - gutterSize);
      const leftPx = Math.min(
        available - minRight,
        Math.max(minLeft, (saved[0] / 100) * available)
      );
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
        const rightPercent = 100 - leftPercent;
        saveSizes(storageKey, [leftPercent, rightPercent]);
        applySizes(leftPx, width);
      };

      document.addEventListener("mousemove", onMove);
      document.addEventListener("mouseup", onUp);
    };

    gutter.addEventListener("mousedown", onMouseDown);
    setFromPercent();
    window.addEventListener("resize", setFromPercent);
  };

  const initGhostVertical = (options) => {
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
      top.style.flex = `0 0 ${topPx}px`;
      bottom.style.flex = `1 1 ${Math.max(minBottom, height - topPx - gutterSize)}px`;
    };

    const setFromPercent = () => {
      const saved = loadSizes(storageKey, [70, 30]);
      const height = container.clientHeight;
      const available = Math.max(1, height - gutterSize);
      const topPx = Math.min(
        available - minBottom,
        Math.max(minTop, (saved[0] / 100) * available)
      );
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

        if (bottomPercent <= 6) {
          localStorage.setItem(STORAGE_KEYS.bottomCollapsed, "true");
        } else {
          localStorage.setItem(STORAGE_KEYS.bottomCollapsed, "false");
        }
      };

      document.addEventListener("mousemove", onMove);
      document.addEventListener("mouseup", onUp);
    };

    gutter.addEventListener("mousedown", onMouseDown);
    setFromPercent();
    window.addEventListener("resize", setFromPercent);
  };

  initGhostVertical({
    container: document.getElementById("split-root"),
    top: document.getElementById("pane-top"),
    bottom: document.getElementById("pane-bottom"),
    storageKey: STORAGE_KEYS.root,
    minTop: 240,
    minBottom: 120,
    gutterSize: 10,
  });

  initGhostSplit({
    container: document.getElementById("split-top"),
    left: document.getElementById("pane-center"),
    right: document.getElementById("pane-right"),
    storageKey: STORAGE_KEYS.top,
    minLeft: 420,
    minRight: 240,
    gutterSize: 10,
  });

  initGhostSplit({
    container: document.getElementById("search-split"),
    left: document.getElementById("search-history-pane"),
    right: document.getElementById("search-results-pane"),
    storageKey: STORAGE_KEYS.search,
    minLeft: 160,
    minRight: 320,
    gutterSize: 8,
  });

  const toggleButton = document.getElementById("toggle-bottom");
  if (toggleButton) {
    toggleButton.addEventListener("click", () => {
      const next = !(localStorage.getItem(STORAGE_KEYS.bottomCollapsed) === "true");
      localStorage.setItem(STORAGE_KEYS.bottomCollapsed, String(next));
      setBottomCollapsed(next);
    });
  }

  setBottomCollapsed(localStorage.getItem(STORAGE_KEYS.bottomCollapsed) === "true");
};
