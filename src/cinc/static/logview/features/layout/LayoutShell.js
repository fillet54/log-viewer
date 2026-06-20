import { html, hooks, appHooks } from "logview/lib";
import { MainViewShell } from "../main/MainViewShell.js";
import { DetailPanel } from "../detail/DetailPanel.js";
import { SearchPanel } from "../search/SearchPanel.js";

const useLayoutEffect = hooks.useLayoutEffect || null;
const useRef = hooks.useRef || null;
const useState = hooks.useState || null;

const TOP_SPLIT_DEFAULT = [72, 28];
const ROOT_SPLIT_DEFAULT = [70, 30];

const LayoutPane = ({
  hostId,
  hostRef = null,
  hostClassName = "pane",
  hostStyle = {},
  children = null,
}) => html`
  <div id=${hostId} ref=${hostRef} class=${hostClassName} style=${hostStyle}>
    <div class="layout-pane-child">${children}</div>
  </div>
`;

export const LayoutShell = () => {
  const services = appHooks.useAppServices();
  const viewerStore = services?.viewerStore || null;
  const rootRef = useRef ? useRef(null) : { current: null };
  const topPaneRef = useRef ? useRef(null) : { current: null };
  const bottomPaneRef = useRef ? useRef(null) : { current: null };
  const centerPaneRef = useRef ? useRef(null) : { current: null };
  const rightPaneRef = useRef ? useRef(null) : { current: null };
  const detailCollapsed = Boolean(viewerStore?.detailCollapsed?.value);
  const bottomCollapsed = Boolean(viewerStore?.bottomCollapsed?.value);
  const topSplitSizes = viewerStore?.topSplitSizes?.value || TOP_SPLIT_DEFAULT;
  const rootSplitSizes = viewerStore?.rootSplitSizes?.value || ROOT_SPLIT_DEFAULT;
  const rootExpandedSizes = viewerStore?.rootExpandedSizes?.value || ROOT_SPLIT_DEFAULT;
  const [bottomHeaderHeight, setBottomHeaderHeight] = useState ? useState(34) : [34, () => {}];

  const loadRootSizes = () => {
    const rootSizes = rootSplitSizes;
    const savedExpanded = rootExpandedSizes;
    return rootSizes[1] > 6 ? rootSizes : savedExpanded[1] > 6 ? savedExpanded : ROOT_SPLIT_DEFAULT;
  };

  appHooks.useSplit({
    refs: [centerPaneRef, rightPaneRef],
    enabled: !detailCollapsed,
    options: {
      direction: "horizontal",
      sizes: topSplitSizes,
      minSize: [420, 240],
      gutterSize: 10,
      elementStyle: (dimension, size, gutterSizeValue) => ({
        "flex-basis": `calc(${size}% - ${gutterSizeValue}px)`,
      }),
      gutterStyle: (dimension, gutterSizeValue) => ({
        "flex-basis": `${gutterSizeValue}px`,
      }),
      onDragEnd: (sizes) => {
        viewerStore?.setTopSplitSizes?.(sizes);
      },
    },
    dependencies: [detailCollapsed, topSplitSizes[0], topSplitSizes[1]],
  });

  appHooks.useSplit({
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
        viewerStore?.setRootSplitSizes?.(sizes);
        if (sizes[1] > 6) viewerStore?.setRootExpandedSizes?.(sizes);
      },
    },
    dependencies: [bottomCollapsed, rootSplitSizes[0], rootSplitSizes[1], rootExpandedSizes[0], rootExpandedSizes[1]],
  });

  if (typeof useLayoutEffect === "function") {
    useLayoutEffect(() => {
      if (!bottomCollapsed) return;
      const bottomPane = bottomPaneRef.current;
      const header = bottomPane?.querySelector(".pane-header");
      const nextHeight = header ? header.offsetHeight : 34;
      if (nextHeight > 0) setBottomHeaderHeight(nextHeight);
    }, [bottomCollapsed]);

    useLayoutEffect(() => {
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
    }, [detailCollapsed, bottomCollapsed]);
  }

  const handleShellClick = (event) => {
    const target = event.target;
    if (!target || typeof target.closest !== "function") return;
    const toggleBottom = target.closest("#toggle-bottom");
    const collapsedSearchHeader =
      target.closest("#pane-bottom .pane-header") && bottomCollapsed;

    if (toggleBottom || collapsedSearchHeader) {
      viewerStore?.setBottomCollapsed((current) => !current);
      return;
    }

    const toggleDetail = target.closest("#toggle-detail, #toggle-detail-restore");
    if (toggleDetail) {
      viewerStore?.setDetailCollapsed((current) => !current);
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
          <${LayoutPane}
            hostId="pane-center"
            hostRef=${centerPaneRef}
            hostClassName="pane"
          >
            <${MainViewShell} />
          </${LayoutPane}>
          <${LayoutPane}
            hostId="pane-right"
            hostRef=${rightPaneRef}
            hostClassName="pane"
            hostStyle=${detailCollapsed ? { display: "none", flex: "0 0 0" } : {}}
          >
            <${DetailPanel} />
          </${LayoutPane}>
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
      <${LayoutPane}
        hostId="pane-bottom"
        hostRef=${bottomPaneRef}
        hostClassName="pane"
        hostStyle=${bottomCollapsed ? { flex: `0 0 ${bottomHeaderHeight}px` } : {}}
      >
        <${SearchPanel} />
      </${LayoutPane}>
    </div>
  `;
};
