(function () {
  const ui = window.EventLog2UI || {};
  const html = ui.html;
  const useEffect = ui.hooks?.useEffect || null;
  const useLayoutEffect = ui.hooks?.useLayoutEffect || null;
  const useRef = ui.hooks?.useRef || null;
  const useState = ui.hooks?.useState || null;

  ui.components = ui.components || {};

  const VIEW_MODE_SPLIT = "split";
  const VIEW_MODE_CHART = "chart";
  const VIEW_MODE_LIST = "list";

  const buildEventByRowId = (events) => {
    const map = new Map();
    events.forEach((event) => {
      map.set(String(event.row_id), event);
    });
    return map;
  };

  const buildIndexByRowId = (events) => {
    const map = new Map();
    events.forEach((event, index) => {
      map.set(String(event.row_id), index);
    });
    return map;
  };

  const findClosestIndexBySeconds = (events, targetSeconds) => {
    if (!events.length) return null;
    let lo = 0;
    let hi = events.length - 1;

    while (lo <= hi) {
      const mid = Math.floor((lo + hi) / 2);
      const seconds = events[mid].norm_time;
      if (seconds === targetSeconds) return mid;
      if (seconds < targetSeconds) lo = mid + 1;
      else hi = mid - 1;
    }

    if (lo >= events.length) return events.length - 1;
    if (hi < 0) return 0;
    return Math.abs(events[lo].norm_time - targetSeconds) < Math.abs(events[hi].norm_time - targetSeconds)
      ? lo
      : hi;
  };

  const createRenderedLogRow = ({ event, services, selectedRowId }) => {
    const rowTemplate = services?.rowTemplate || null;
    const renderRow = window.EventLog2?.resolveRowRenderer
      ? window.EventLog2.resolveRowRenderer(services?.plugin || null)
      : null;

    if (!rowTemplate || typeof renderRow !== "function") return null;

    const row = renderRow(event, rowTemplate, {
      bookmarks: services?.bookmarks || null,
      view: services?.view || null,
    });

    if (!row) return null;
    row.classList.toggle("log-selected", String(selectedRowId ?? "") === String(event.row_id));
    return row;
  };

  const attachRenderedLogRow = ({ row, event, services, eventByRowId }) => {
    if (!row) return null;

    row.querySelector(".bookmark-toggle")?.addEventListener("click", (eventClick) => {
      eventClick.stopPropagation();
      const next = services?.bookmarks?.cycle(event.row_id) || 0;
      row.classList.toggle("is-bookmarked", next > 0);
      row.dataset.bookmarkColor = String(next);
      services?.bus?.emit("bookmarks:changed", services?.bookmarks?.getAllWithColors() || {});
    });

    row.querySelectorAll(".match-link").forEach((button) => {
      button.addEventListener("click", (eventClick) => {
        eventClick.stopPropagation();
        const linkedRowId = button.dataset.linkedRowId;
        if (!linkedRowId || !services?.bus) return;
        const linkedEvent = eventByRowId.get(String(linkedRowId)) || null;
        if (linkedEvent) services.bus.emit("event:selected", linkedEvent);
        services.bus.emit("log:jump", { rowId: linkedRowId });
      });
    });

    row.addEventListener("click", () => {
      services?.bus?.emit("event:selected", event);
    });

    return row;
  };

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

  const MainLogRow = ({
    event,
    services,
    eventByRowId,
    selectedRowId,
    highlightRowId,
    highlightNonce,
    bookmarkVersion,
  }) => {
    const ref = useRef ? useRef(null) : { current: null };

    if (typeof useLayoutEffect === "function") {
      useLayoutEffect(() => {
        const host = ref.current;
        if (!host) return;

        host.innerHTML = "";
        const row = createRenderedLogRow({ event, services, selectedRowId });
        if (!row) return;

        if (String(highlightRowId ?? "") === String(event.row_id)) {
          row.classList.remove("log-highlight");
          void row.offsetWidth;
          row.classList.add("log-highlight");
        }

        const mountedRow = attachRenderedLogRow({
          row,
          event,
          services,
          eventByRowId,
        });

        if (mountedRow) host.appendChild(mountedRow);
      }, [
        event?.row_id,
        services,
        eventByRowId,
        selectedRowId,
        highlightRowId,
        highlightNonce,
        bookmarkVersion,
      ]);
    }

    return html`<div ref=${ref}></div>`;
  };

  const MainLogContent = ({
    filteredEvents,
    rowStride,
    virtual,
    visibleItems,
    services,
    eventByRowId,
    selectedRowId,
    highlightState,
    bookmarkVersion,
    logListRef,
  }) => {
    if (!filteredEvents.length) {
      return html`
        <div id="log-spacer"></div>
        <div id="log-list" ref=${logListRef} class="mono-block">
          <div class="no-results">No Results</div>
        </div>
      `;
    }

    return html`
      <div id="log-spacer" style=${{ height: `${filteredEvents.length * rowStride}px` }}></div>
      <div
        id="log-list"
        ref=${logListRef}
        class="mono-block"
        style=${{ transform: `translateY(${virtual.offsetY}px)` }}
      >
        ${visibleItems.map(
          (event) => html`
            <${MainLogRow}
              event=${event}
              services=${services}
              eventByRowId=${eventByRowId}
              selectedRowId=${selectedRowId}
              highlightRowId=${highlightState.rowId}
              highlightNonce=${highlightState.nonce}
              bookmarkVersion=${bookmarkVersion}
            />
          `
        )}
      </div>
    `;
  };

  const MainLogPane = ({
    logRegionRef,
    logBodyRef,
    measureRef,
    viewMode,
    filteredEvents,
    rowStride,
    virtual,
    visibleItems,
    services,
    eventByRowId,
    selectedRowId,
    highlightState,
    bookmarkVersion,
    logListRef,
  }) => html`
    <section
      class="main-view-region main-view-log-region"
      id="log-region"
      ref=${logRegionRef}
      style=${viewMode === VIEW_MODE_LIST
        ? { flex: "1 1 auto" }
        : viewMode === VIEW_MODE_CHART
          ? { flex: "0 0 0" }
          : {}}
    >
      <div class="pane-body log-body" id="log-body" ref=${logBodyRef}>
        <${MainLogContent}
          filteredEvents=${filteredEvents}
          rowStride=${rowStride}
          virtual=${virtual}
          visibleItems=${visibleItems}
          services=${services}
          eventByRowId=${eventByRowId}
          selectedRowId=${selectedRowId}
          highlightState=${highlightState}
          bookmarkVersion=${bookmarkVersion}
          logListRef=${logListRef}
        />
      </div>
      <div ref=${measureRef} style=${{ position: "absolute", visibility: "hidden", pointerEvents: "none" }}></div>
    </section>
  `;

  const MainViewShell = () => {
    const services = ui.appHooks.useAppServices();
    const events = Array.isArray(services?.logData?.events) ? services.logData.events : [];
    const rootRef = useRef ? useRef(null) : { current: null };
    const chartRegionRef = useRef ? useRef(null) : { current: null };
    const logRegionRef = useRef ? useRef(null) : { current: null };
    const logBodyRef = useRef ? useRef(null) : { current: null };
    const logListRef = useRef ? useRef(null) : { current: null };
    const measureRef = useRef ? useRef(null) : { current: null };
    const commandBarRef = useRef ? useRef(null) : { current: null };
    const chartControllerRef = useRef ? useRef(null) : { current: null };
    const scrollFrameRef = useRef ? useRef(0) : { current: 0 };
    const pendingFilterRef = useRef ? useRef(0) : { current: 0 };
    const pendingJumpRef = useRef ? useRef(null) : { current: null };
    const highlightNonceRef = useRef ? useRef(0) : { current: 0 };
    const filteredRef = useRef ? useRef(events) : { current: events };
    const indexByRowIdRef = useRef ? useRef(buildIndexByRowId(events)) : { current: buildIndexByRowId(events) };
    const eventByRowIdRef = useRef ? useRef(buildEventByRowId(events)) : { current: buildEventByRowId(events) };

    const [viewMode, setViewMode] = ui.appHooks.useLocalStorageState(STORAGE_KEYS.mainViewMode, VIEW_MODE_SPLIT, {
      parse: (raw) => String(raw || VIEW_MODE_SPLIT),
      serialize: (value) => String(value || VIEW_MODE_SPLIT),
    });
    const [chartSplit, setChartSplit] = useState ? useState(() => loadSizes(STORAGE_KEYS.mainViewSplit, [36, 64])) : [[36, 64], () => {}];
    const [chartTypes, setChartTypes] = useState ? useState([]) : [[], () => {}];
    const [selectedChartType, setSelectedChartType] = useState ? useState("") : ["", () => {}];
    const [filteredEvents, setFilteredEvents] = useState ? useState(() => events) : [events, () => {}];
    const [rowStride, setRowStride] = useState ? useState(38) : [38, () => {}];
    const [selectedRowId, setSelectedRowId] = useState ? useState(null) : [null, () => {}];
    const [bookmarkVersion, setBookmarkVersion] = useState ? useState(0) : [0, () => {}];
    const [highlightState, setHighlightState] = useState ? useState({ rowId: null, nonce: 0 }) : [{ rowId: null, nonce: 0 }, () => {}];

    filteredRef.current = filteredEvents;

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

    const safeViewMode = new Set([VIEW_MODE_SPLIT, VIEW_MODE_CHART, VIEW_MODE_LIST]).has(viewMode)
      ? viewMode
      : VIEW_MODE_SPLIT;

    const smoothScrollToIndex = (index, duration = 180) => {
      const container = logBodyRef.current;
      const list = filteredRef.current;
      if (index == null || !container || !list.length) return null;

      const targetTop = index * rowStride - container.clientHeight / 2 + rowStride / 2;
      const clamped = Math.max(0, Math.min(targetTop, container.scrollHeight));
      const selected = list[index] || null;

      smoothScrollTo(container, clamped, duration, () => {
        if (!selected) return;
        highlightNonceRef.current += 1;
        setHighlightState({
          rowId: selected.row_id,
          nonce: highlightNonceRef.current,
        });
      });

      if (services?.bus && selected) services.bus.emit("event:selected", selected);
      return selected;
    };

    const applyFilterQueries = (queries) => {
      const terms = (queries || []).map((query) => String(query || "").trim()).filter(Boolean);
      const requestId = ++pendingFilterRef.current;

      if (!terms.length) {
        setFilteredEvents(events);
        return;
      }

      const query = terms.join(" OR ");
      if (services?.searchWorker) {
        LogSearch.runQuery(services.searchWorker, query, (indices) => {
          if (requestId !== pendingFilterRef.current) return;
          setFilteredEvents(indices.map((index) => events[index]).filter(Boolean));
        });
        return;
      }

      const predicates = terms.map((term) => LogSearch.getQueryPredicate(term));
      setFilteredEvents(
        events.filter((event) => predicates.some((predicate) => predicate(event)))
      );
    };

    const handleLogJump = (payload) => {
      if (!payload) return;

      if (payload.rowId != null) {
        const index = indexByRowIdRef.current.get(String(payload.rowId));
        if (index != null) {
          smoothScrollToIndex(index);
          return;
        }

        pendingFilterRef.current += 1;
        pendingJumpRef.current = { rowId: payload.rowId, seconds: payload.seconds ?? null };
        setFilteredEvents(events);
        return;
      }

      if (payload.seconds != null) {
        smoothScrollToIndex(findClosestIndexBySeconds(filteredRef.current, payload.seconds));
      }
    };

    const emitScrollState = () => {
      const container = logBodyRef.current;
      const list = filteredRef.current;
      if (!container || !list.length || !services?.bus) return;

      const index = Math.max(
        0,
        Math.min(
          list.length - 1,
          Math.floor((container.scrollTop + container.clientHeight / 2) / rowStride)
        )
      );
      const current = list[index] || null;
      if (current) {
        services.bus.emit("log:scroll", { seconds: current.norm_time, rowId: current.row_id });
      }
    };

    const virtual = ui.appHooks.useVirtualList({
      containerRef: logBodyRef,
      itemCount: filteredEvents.length,
      rowHeight: rowStride,
      overscan: 10,
      maxVisible: 180,
      dependencies: [bookmarkVersion],
    });

    const visibleItems = filteredEvents.slice(virtual.startIndex, virtual.endIndex);

    ui.appHooks.useBusSubscription("filters:apply", (queries) => {
      applyFilterQueries(queries || []);
    }, [services, events.length]);

    ui.appHooks.useBusSubscription("log:jump", (payload) => {
      handleLogJump(payload);
    }, [services, events.length, rowStride]);

    ui.appHooks.useBusSubscription("event:selected", (event) => {
      setSelectedRowId(event?.row_id ?? null);
    });

    ui.appHooks.useBusSubscription("bookmarks:changed", () => {
      setBookmarkVersion((value) => value + 1);
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
      }, [services]);

      useLayoutEffect(() => {
        const host = measureRef.current;
        const list = logListRef.current;
        if (!host || !list || !events.length) return;

        host.innerHTML = "";
        const sample = createRenderedLogRow({
          event: events[0],
          services,
          selectedRowId: null,
        });
        if (!sample) return;

        sample.style.visibility = "hidden";
        host.appendChild(sample);

        const rowHeight = sample.getBoundingClientRect().height || 38;
        const listStyle = getComputedStyle(list);
        const gap = parseFloat(listStyle.rowGap || listStyle.gap || "0") || 0;

        host.innerHTML = "";

        if (rowHeight > 0) {
          setRowStride(rowHeight + gap);
          virtual.invalidate();
        }
      }, [services, events.length]);
    }

    if (typeof useEffect === "function") {
      useEffect(() => {
        eventByRowIdRef.current = buildEventByRowId(events);
        indexByRowIdRef.current = buildIndexByRowId(events);
        filteredRef.current = events;
        setFilteredEvents(events);
        setSelectedRowId(null);
        pendingFilterRef.current += 1;
        pendingJumpRef.current = null;
      }, [events]);

      useEffect(() => {
        filteredRef.current = filteredEvents;
        indexByRowIdRef.current = buildIndexByRowId(filteredEvents);
      }, [filteredEvents]);

      useEffect(() => {
        services?.bus?.emit("log:filtered", filteredEvents);
      }, [filteredEvents, services]);

      useEffect(() => {
        const pendingJump = pendingJumpRef.current;
        if (!pendingJump) return;

        if (pendingJump.rowId != null) {
          const rowIndex = indexByRowIdRef.current.get(String(pendingJump.rowId));
          if (rowIndex != null) {
            pendingJumpRef.current = null;
            smoothScrollToIndex(rowIndex);
            return;
          }
          if (pendingJump.seconds == null) {
            pendingJumpRef.current = null;
            return;
          }
        }

        if (pendingJump.seconds != null) {
          pendingJumpRef.current = null;
          smoothScrollToIndex(findClosestIndexBySeconds(filteredEvents, pendingJump.seconds));
        }
      }, [filteredEvents, rowStride]);

      useEffect(() => {
        const container = logBodyRef.current;
        if (!container) return undefined;

        const onScroll = () => {
          if (scrollFrameRef.current) return;
          scrollFrameRef.current = requestAnimationFrame(() => {
            scrollFrameRef.current = 0;
            emitScrollState();
          });
        };

        container.addEventListener("scroll", onScroll, { passive: true });
        return () => {
          container.removeEventListener("scroll", onScroll);
          if (scrollFrameRef.current) {
            cancelAnimationFrame(scrollFrameRef.current);
            scrollFrameRef.current = 0;
          }
        };
      }, [services, rowStride, filteredEvents.length]);

      useEffect(() => {
        chartControllerRef.current?.resize?.();
        requestAnimationFrame(() => window.dispatchEvent(new Event("resize")));
      }, [safeViewMode, chartSplit[0], chartSplit[1]]);

      useEffect(() => {
        const controller = chartControllerRef.current;
        if (!controller) return;
        controller.attachToolbar({ commandBarEl: commandBarRef.current });
        if (selectedChartType) {
          controller.setType(selectedChartType);
        }
      }, [selectedChartType, chartTypes.length]);
    }

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
          <${MainLogPane}
            logRegionRef=${logRegionRef}
            logBodyRef=${logBodyRef}
            measureRef=${measureRef}
            viewMode=${safeViewMode}
            filteredEvents=${filteredEvents}
            rowStride=${rowStride}
            virtual=${virtual}
            visibleItems=${visibleItems}
            services=${services}
            eventByRowId=${eventByRowIdRef.current}
            selectedRowId=${selectedRowId}
            highlightState=${highlightState}
            bookmarkVersion=${bookmarkVersion}
            logListRef=${logListRef}
          />
        </div>
      </div>
    `;
  };

  ui.components.LogMainViewShell = MainViewShell;
})();
