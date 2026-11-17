import { html, hooks, appHooks } from "logview/lib";

const { useLayoutEffect, useRef } = hooks;
const LogMainViewChart = window.LogMainViewChart || null;

// Charts placed in the "side" slot, as collapsible sections under the row details (LogMainChart.mountSide).
export const SideDock = ({ showHeaderToggle = false }) => {
  const services = appHooks.useAppServices();
  const viewerStore = services?.viewerStore || null;
  const hostRef = useRef(null);
  const controllerRef = useRef(null);
  // Read here so the dock re-syncs when charts move or sections collapse.
  const placementKey = JSON.stringify(viewerStore?.chartPlacement?.value || {});
  const collapsedKey = (viewerStore?.sideCollapsed?.value || []).join("|");

  useLayoutEffect(() => {
    if (!hostRef.current || !LogMainViewChart?.mountSide) return undefined;
    const controller = LogMainViewChart.mountSide(hostRef.current, services);
    controllerRef.current = controller;
    controller.sync();
    const onResize = () => controller.resize();
    window.addEventListener("resize", onResize);
    return () => {
      window.removeEventListener("resize", onResize);
      controller.destroy();
      controllerRef.current = null;
    };
  }, [services]);

  useLayoutEffect(() => {
    controllerRef.current?.sync();
  }, [placementKey, collapsedKey]);

  return html`
    <div class="side-dock-shell">
      ${showHeaderToggle
        ? html`<div class="pane-header pane-header-spread side-dock-header">
            <div class="pane-header-title">Charts</div>
            <button
              type="button"
              class="button button-ghost button-xs"
              data-toggle-detail
              title="Hide this pane"
              aria-label="Hide this pane"
            >
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" class="tool-icon">
                <path stroke-linecap="round" stroke-linejoin="round" d="M9 6l6 6-6 6" />
              </svg>
            </button>
          </div>`
        : null}
      <div class="side-dock" ref=${hostRef}></div>
    </div>
  `;
};
