(function () {
  const ui = window.EventLog2UI || {};
  const html = ui.html;
  const Fragment = ui.Fragment;
  const useEffect = ui.hooks?.useEffect || null;
  const useLayoutEffect = ui.hooks?.useLayoutEffect || null;
  const useRef = ui.hooks?.useRef || null;
  const useState = ui.hooks?.useState || null;

  ui.components = ui.components || {};

  const VIEW_MODE_SPLIT = "split";
  const VIEW_MODE_CHART = "chart";
  const VIEW_MODE_LIST = "list";

  const ViewModeButton = ({ id, active, title, label, icon, onClick }) => html`
    <button
      id=${id}
      class=${`button button-ghost button-xs view-mode-button${active ? " is-active" : ""}`}
      role="tab"
      aria-selected=${String(active)}
      title=${title}
      onClick=${onClick}
    >
      ${icon}
      <span class="sr-only">${label}</span>
    </button>
  `;

  const MainViewToolbar = ({
    chartTypes,
    selectedChartType,
    commandBarRef,
    viewMode,
    onChartTypeChange,
    onViewModeChange,
  }) => html`
    <div class="main-view-toolbar">
      <div class="chart-type-picker">
        <select
          id="chart-type-select"
          class="text-input text-input-small chart-type-select"
          aria-label="Chart type"
          value=${selectedChartType}
          disabled=${chartTypes.length === 0}
          onChange=${(event) => onChartTypeChange(event.currentTarget.value)}
        >
          ${chartTypes.map(
            (type) => html`<option value=${type.id}>${type.label}</option>`
          )}
        </select>
      </div>
      <div id="chart-command-bar" ref=${commandBarRef} class="chart-command-bar"></div>
      <div class="view-mode-toggle" role="tablist" aria-label="Main view layout">
        <${ViewModeButton}
          id="view-mode-split"
          active=${viewMode === VIEW_MODE_SPLIT}
          title="Show chart and log"
          label="Chart and log"
          onClick=${() => onViewModeChange(VIEW_MODE_SPLIT)}
          icon=${html`
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="tool-icon" aria-hidden="true">
              <rect x="4" y="5" width="16" height="6" rx="1.5" />
              <rect x="4" y="13" width="16" height="6" rx="1.5" />
            </svg>
          `}
        />
        <${ViewModeButton}
          id="view-mode-chart"
          active=${viewMode === VIEW_MODE_CHART}
          title="Show chart only"
          label="Chart only"
          onClick=${() => onViewModeChange(VIEW_MODE_CHART)}
          icon=${html`
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="tool-icon" aria-hidden="true">
              <path stroke-linecap="round" stroke-linejoin="round" d="M4 19h16" />
              <path stroke-linecap="round" stroke-linejoin="round" d="M7 16V9m5 7V5m5 11v-4" />
            </svg>
          `}
        />
        <${ViewModeButton}
          id="view-mode-list"
          active=${viewMode === VIEW_MODE_LIST}
          title="Show log only"
          label="Log only"
          onClick=${() => onViewModeChange(VIEW_MODE_LIST)}
          icon=${html`
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="tool-icon" aria-hidden="true">
              <path stroke-linecap="round" stroke-linejoin="round" d="M7 7h13M7 12h13M7 17h13" />
              <circle cx="4" cy="7" r="1" fill="currentColor" stroke="none" />
              <circle cx="4" cy="12" r="1" fill="currentColor" stroke="none" />
              <circle cx="4" cy="17" r="1" fill="currentColor" stroke="none" />
            </svg>
          `}
        />
      </div>
    </div>
  `;

  const MainViewShell = ({ shellVersion = 0 }) => {
    const services = ui.appHooks.useAppServices();
    const rootRef = useRef ? useRef(null) : { current: null };
    const chartRegionRef = useRef ? useRef(null) : { current: null };
    const logRegionRef = useRef ? useRef(null) : { current: null };
    const commandBarRef = useRef ? useRef(null) : { current: null };
    const chartControllerRef = useRef ? useRef(null) : { current: null };
    const [viewMode, setViewMode] = ui.appHooks.useLocalStorageState(STORAGE_KEYS.mainViewMode, VIEW_MODE_SPLIT, {
      parse: (raw) => String(raw || VIEW_MODE_SPLIT),
      serialize: (value) => String(value || VIEW_MODE_SPLIT),
    });
    const [chartSplit, setChartSplit] = useState ? useState(() => loadSizes(STORAGE_KEYS.mainViewSplit, [36, 64])) : [[36, 64], () => {}];
    const [chartTypes, setChartTypes] = useState ? useState([]) : [[], () => {}];
    const [selectedChartType, setSelectedChartType] = useState ? useState("") : ["", () => {}];

    ui.appHooks.useSplit({
      refs: [chartRegionRef, logRegionRef],
      enabled: viewMode === VIEW_MODE_SPLIT,
      options: {
        direction: "vertical",
        sizes: chartSplit,
        minSize: [170, 220],
        gutterSize: 10,
        elementStyle: (dimension, size, gutterSizeValue) => ({
          "flex-basis": `calc(${size}% - ${gutterSizeValue}px)`,
        }),
        gutterStyle: (dimension, gutterSizeValue) => ({
          "flex-basis": `${gutterSizeValue}px`,
        }),
        onDragEnd: (sizes) => {
          setChartSplit(sizes);
          saveSizes(STORAGE_KEYS.mainViewSplit, sizes);
          chartControllerRef.current?.resize?.();
          requestAnimationFrame(() => window.dispatchEvent(new Event("resize")));
        },
      },
      dependencies: [viewMode, chartSplit[0], chartSplit[1]],
    });

    if (typeof useLayoutEffect === "function") {
      useLayoutEffect(() => {
        const root = rootRef.current;
        if (!root || !services?.logData) return undefined;

        const controller = LogMainViewChart.mount(root, services);
        if (!controller) return undefined;

        chartControllerRef.current = controller;
        controller.attachToolbar({ commandBarEl: commandBarRef.current });

        const types = controller.listTypes();
        setChartTypes(types);

        if (!types.length) {
          setSelectedChartType("");
          return () => {
            controller.destroy?.();
            chartControllerRef.current = null;
          };
        }

        const fallbackType = types.some((type) => type.id === controller.getCurrentType())
          ? controller.getCurrentType()
          : types[0].id;
        setSelectedChartType(fallbackType);
        controller.setType(fallbackType);

        return () => {
          controller.destroy?.();
          chartControllerRef.current = null;
        };
      }, [services, shellVersion]);
    }

    if (typeof useEffect === "function") {
      useEffect(() => {
        const controller = chartControllerRef.current;
        if (!controller) return;
        controller.attachToolbar({ commandBarEl: commandBarRef.current });
        if (selectedChartType) {
          controller.setType(selectedChartType);
        }
      }, [selectedChartType, chartTypes.length]);

      useEffect(() => {
        chartControllerRef.current?.resize?.();
        requestAnimationFrame(() => window.dispatchEvent(new Event("resize")));
      }, [viewMode, chartSplit[0], chartSplit[1]]);
    }

    const safeViewMode = new Set([VIEW_MODE_SPLIT, VIEW_MODE_CHART, VIEW_MODE_LIST]).has(viewMode)
      ? viewMode
      : VIEW_MODE_SPLIT;

    return html`
      <div ref=${rootRef} class="main-view-shell">
        <${MainViewToolbar}
          chartTypes=${chartTypes}
          selectedChartType=${selectedChartType}
          commandBarRef=${commandBarRef}
          viewMode=${safeViewMode}
          onChartTypeChange=${(nextType) => setSelectedChartType(nextType)}
          onViewModeChange=${(nextMode) => setViewMode(nextMode)}
        />
        <div class="main-view-stack" id="main-view-stack" data-mode=${safeViewMode}>
          <section
            class="main-view-region main-view-chart-region"
            id="chart-region"
            ref=${chartRegionRef}
            style=${safeViewMode === VIEW_MODE_CHART
              ? { flex: "1 1 auto" }
              : safeViewMode === VIEW_MODE_LIST
                ? { flex: "0 0 0" }
                : {}}
          >
            <div class="chart-band">
              <div id="chart-panel-host" class="chart-panel-host"></div>
            </div>
          </section>
          <section
            class="main-view-region main-view-log-region"
            id="log-region"
            ref=${logRegionRef}
            style=${safeViewMode === VIEW_MODE_LIST
              ? { flex: "1 1 auto" }
              : safeViewMode === VIEW_MODE_CHART
                ? { flex: "0 0 0" }
                : {}}
          >
            <div class="pane-body log-body" id="log-body">
              <div id="log-spacer"></div>
              <div class="mono-block" id="log-list"></div>
            </div>
          </section>
        </div>
      </div>
    `;
  };

  ui.components.LogMainViewShell = MainViewShell;
})();
