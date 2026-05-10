(function () {
  const ui = window.EventLog2UI || {};
  const html = ui.html;
  const useEffect = ui.hooks?.useEffect || null;
  const useLayoutEffect = ui.hooks?.useLayoutEffect || null;
  const useRef = ui.hooks?.useRef || null;
  const useState = ui.hooks?.useState || null;

  ui.components = ui.components || {};

  const TOP_SPLIT_DEFAULT = [72, 28];
  const ROOT_SPLIT_DEFAULT = [70, 30];

  const MountedPaneChild = ({
    node,
    hostId,
    hostRef = null,
    hostClassName = "",
    hostStyle = {},
    childClassName = "",
  }) => {
    const localHostRef = useRef ? useRef(null) : { current: null };
    const targetHostRef = hostRef || localHostRef;

    useLayoutEffect(() => {
      const host = targetHostRef.current;
      if (!host || !node) return undefined;

      node.classList.add("layout-mounted-child");
      if (childClassName) node.classList.add(...childClassName.split(/\s+/).filter(Boolean));
      node.style.display = "flex";
      node.style.flexDirection = "column";
      node.style.flex = "1 1 auto";
      node.style.width = "100%";
      node.style.height = "100%";
      node.style.minWidth = "0";
      node.style.minHeight = "0";
      node.style.overflow = "hidden";

      if (node.parentNode !== host) {
        host.appendChild(node);
      }

      return undefined;
    }, [node, childClassName, targetHostRef]);

    return html`<div id=${hostId} ref=${targetHostRef} class=${hostClassName} style=${hostStyle}></div>`;
  };

  const LayoutShell = ({
    mainViewNode,
    detailPanelNode,
    searchPanelNode,
    searchReadyVersion = 0,
  }) => {
    const rootRef = useRef ? useRef(null) : { current: null };
    const topPaneRef = useRef ? useRef(null) : { current: null };
    const bottomPaneRef = useRef ? useRef(null) : { current: null };
    const centerPaneRef = useRef ? useRef(null) : { current: null };
    const rightPaneRef = useRef ? useRef(null) : { current: null };
    const [detailCollapsed, setDetailCollapsed] = ui.appHooks.useLocalStorageState(
      STORAGE_KEYS.detailCollapsed,
      false
    );
    const [bottomCollapsed, setBottomCollapsed] = ui.appHooks.useLocalStorageState(
      STORAGE_KEYS.bottomCollapsed,
      false
    );
    const [bottomHeaderHeight, setBottomHeaderHeight] = useState ? useState(34) : [34, () => {}];

    const loadRootSizes = () => {
      const rootSizes = loadSizes(STORAGE_KEYS.root, ROOT_SPLIT_DEFAULT);
      const savedExpanded = loadSizes(STORAGE_KEYS.rootExpanded, ROOT_SPLIT_DEFAULT);
      const safeSizes =
        rootSizes[1] > 6 ? rootSizes : savedExpanded[1] > 6 ? savedExpanded : ROOT_SPLIT_DEFAULT;
      saveSizes(STORAGE_KEYS.root, safeSizes);
      return safeSizes;
    };

    ui.appHooks.useSplit({
      refs: [centerPaneRef, rightPaneRef],
      enabled: !detailCollapsed,
      options: {
        direction: "horizontal",
        sizes: loadSizes(STORAGE_KEYS.top, TOP_SPLIT_DEFAULT),
        minSize: [420, 240],
        gutterSize: 10,
        elementStyle: (dimension, size, gutterSizeValue) => ({
          "flex-basis": `calc(${size}% - ${gutterSizeValue}px)`,
        }),
        gutterStyle: (dimension, gutterSizeValue) => ({
          "flex-basis": `${gutterSizeValue}px`,
        }),
        onDragEnd: (sizes) => {
          saveSizes(STORAGE_KEYS.top, sizes);
        },
      },
      dependencies: [detailCollapsed],
    });

    ui.appHooks.useSplit({
      refs: [topPaneRef, bottomPaneRef],
      enabled: !bottomCollapsed,
      options: {
        direction: "vertical",
        sizes: loadRootSizes(),
        minSize: [240, 120],
        gutterSize: 10,
        elementStyle: (dimension, size, gutterSizeValue) => ({
          "flex-basis": `calc(${size}% - ${gutterSizeValue}px)`,
        }),
        gutterStyle: (dimension, gutterSizeValue) => ({
          "flex-basis": `${gutterSizeValue}px`,
        }),
        onDragEnd: (sizes) => {
          saveSizes(STORAGE_KEYS.root, sizes);
          if (sizes[1] > 6) saveSizes(STORAGE_KEYS.rootExpanded, sizes);
        },
      },
      dependencies: [bottomCollapsed],
    });

    if (typeof useLayoutEffect === "function") {
      useLayoutEffect(() => {
        if (!bottomCollapsed) return;
        const bottomPane = bottomPaneRef.current;
        const header = bottomPane?.querySelector(".pane-header");
        const nextHeight = header ? header.offsetHeight : 34;
        if (nextHeight > 0) setBottomHeaderHeight(nextHeight);
      }, [bottomCollapsed, searchReadyVersion]);
    }

    if (typeof useEffect === "function") {
      useEffect(() => {
        const root = rootRef.current;
        if (!root) return;

        const headerButton = root.querySelector("#toggle-detail");
        const restoreButton = root.querySelector("#toggle-detail-restore");
        const bottomButton = root.querySelector("#toggle-bottom");
        const headerText = root.querySelector("#search-header-text");
        const headerTabs = root.querySelector("#search-header-tabs");

        if (headerButton) {
          headerButton.setAttribute(
            "aria-label",
            detailCollapsed ? "Show info pane" : "Hide info pane"
          );
          headerButton.setAttribute(
            "title",
            detailCollapsed ? "Show info pane" : "Hide info pane"
          );
        }

        if (restoreButton) {
          restoreButton.classList.toggle("is-collapsed", detailCollapsed);
          restoreButton.setAttribute("aria-label", "Show info pane");
          restoreButton.setAttribute("title", "Show info pane");
        }

        if (bottomButton) {
          bottomButton.setAttribute(
            "aria-label",
            bottomCollapsed ? "Expand search pane" : "Collapse search pane"
          );
          bottomButton.classList.toggle("is-collapsed", bottomCollapsed);
        }

        if (headerText) {
          headerText.textContent = "Search";
          headerText.classList.toggle("hidden", !bottomCollapsed);
        }

        if (headerTabs) {
          headerTabs.classList.toggle("hidden", bottomCollapsed);
        }

        requestAnimationFrame(() => window.dispatchEvent(new Event("resize")));
      }, [detailCollapsed, bottomCollapsed, searchReadyVersion]);
    }

    const handleShellClick = (event) => {
      const target = event.target;
      if (!target || typeof target.closest !== "function") return;
      const toggleBottom = target.closest("#toggle-bottom");
      const collapsedSearchHeader =
        target.closest("#pane-bottom .pane-header") && bottomCollapsed;

      if (toggleBottom || collapsedSearchHeader) {
        setBottomCollapsed((current) => !current);
        return;
      }

      const toggleDetail = target.closest("#toggle-detail, #toggle-detail-restore");
      if (toggleDetail) {
        setDetailCollapsed((current) => !current);
      }
    };

    return html`
      <div
        id="split-root"
        ref=${rootRef}
        class=${`layout-shell${bottomCollapsed ? " bottom-collapsed" : ""}`}
        onClick=${handleShellClick}
      >
        <section
          id="pane-top"
          ref=${topPaneRef}
          class="layout-top"
          style=${bottomCollapsed ? { flex: "1 1 auto" } : {}}
        >
          <div
            id="split-top"
            class=${`layout-top-inner${detailCollapsed ? " detail-collapsed" : ""}`}
          >
            <${MountedPaneChild}
              node=${mainViewNode}
              hostId="pane-center"
              hostRef=${centerPaneRef}
              hostClassName="pane"
              childClassName="layout-pane-child"
            />
            <${MountedPaneChild}
              node=${detailPanelNode}
              hostId="pane-right"
              hostRef=${rightPaneRef}
              hostClassName="pane"
              hostStyle=${detailCollapsed ? { display: "none", flex: "0 0 0" } : {}}
              childClassName="layout-pane-child"
            />
          </div>
          <button
            id="toggle-detail-restore"
            class=${`button button-ghost button-xs top-pane-toggle${detailCollapsed ? " is-collapsed" : ""}`}
            title="Show info pane"
            aria-label="Show info pane"
          >
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
        <${MountedPaneChild}
          node=${searchPanelNode}
          hostId="pane-bottom"
          hostRef=${bottomPaneRef}
          hostClassName="pane"
          hostStyle=${bottomCollapsed ? { flex: `0 0 ${bottomHeaderHeight}px` } : {}}
          childClassName="layout-pane-child"
        />
      </div>
    `;
  };

  ui.components.LogLayoutShell = LayoutShell;
})();
