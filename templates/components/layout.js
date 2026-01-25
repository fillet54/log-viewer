window.LogApp = window.LogApp || {};

LogApp.initSplits = () => {
  const { STORAGE_KEYS, loadSizes, saveSizes } = LogApp;

  let rootSplit = null;

  const setBottomCollapsed = (collapsed) => {
    const root = document.getElementById("split-root");
    const button = document.getElementById("toggle-bottom");
    const headerText = document.getElementById("search-header-text");
    const headerTabs = document.getElementById("search-header-tabs");
    if (!root || !button || !rootSplit) return;

    if (collapsed) {
      root.classList.add("bottom-collapsed");
      rootSplit.setSizes([96, 4]);
      button.setAttribute("aria-label", "Expand search pane");
      button.classList.add("is-collapsed");
      if (headerText) {
        headerText.textContent = "Search";
        headerText.classList.remove("hidden");
      }
      if (headerTabs) headerTabs.classList.add("hidden");
    } else {
      root.classList.remove("bottom-collapsed");
      const savedExpanded = loadSizes(STORAGE_KEYS.rootExpanded, [70, 30]);
      const safeSizes = savedExpanded[1] > 0 ? savedExpanded : [70, 30];
      rootSplit.setSizes(safeSizes);
      button.setAttribute("aria-label", "Collapse search pane");
      button.classList.remove("is-collapsed");
      if (headerText) headerText.classList.add("hidden");
      if (headerTabs) headerTabs.classList.remove("hidden");
    }
  };

  const rootSizes = loadSizes(STORAGE_KEYS.root, [70, 30]);
  const topSizes = loadSizes(STORAGE_KEYS.top, [70, 30]);
  const isCollapsed = localStorage.getItem(STORAGE_KEYS.bottomCollapsed) === "true";

  rootSplit = Split(["#pane-top", "#pane-bottom"], {
    sizes: isCollapsed ? [100, 0] : rootSizes,
    direction: "vertical",
    gutterSize: 10,
    minSize: [240, 0],
    onDragEnd: (sizes) => {
      saveSizes(STORAGE_KEYS.root, sizes);
      if (sizes[1] > 6) {
        saveSizes(STORAGE_KEYS.rootExpanded, sizes);
      }
      if (sizes[1] <= 6) {
        localStorage.setItem(STORAGE_KEYS.bottomCollapsed, "true");
      } else {
        localStorage.setItem(STORAGE_KEYS.bottomCollapsed, "false");
      }
    },
  });

  Split(["#pane-center", "#pane-right"], {
    sizes: topSizes,
    direction: "horizontal",
    gutterSize: 10,
    minSize: [420, 240],
    onDragEnd: (sizes) => saveSizes(STORAGE_KEYS.top, sizes),
  });

  const toggleButton = document.getElementById("toggle-bottom");
  if (toggleButton) {
    toggleButton.addEventListener("click", () => {
      const next = !(localStorage.getItem(STORAGE_KEYS.bottomCollapsed) === "true");
      localStorage.setItem(STORAGE_KEYS.bottomCollapsed, String(next));
      setBottomCollapsed(next);
    });
  }

  setBottomCollapsed(isCollapsed);
};
