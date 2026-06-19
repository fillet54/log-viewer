import { smoothScrollTo } from "../../../shared.js";
import "../chart/LogMainChart.js";
import {
  MainLogPane,
  buildEventByRowId,
  buildIndexByRowId,
  findClosestIndexBySeconds,
} from "./LogVirtualList.js";
import { MainViewToolbar } from "./MainViewToolbar.js";
import { html, hooks, appHooks } from "logview/lib";
import { getQueryPredicate } from "../../../services/search.js";

const LogMainViewChart = window.LogMainViewChart || null;
const { useEffect, useLayoutEffect, useRef, useState } = hooks;

const VIEW_MODE_SPLIT = "split";
const VIEW_MODE_CHART = "chart";
const VIEW_MODE_LIST = "list";

export const MainViewShell = () => {
    const services = appHooks.useAppServices();
    const viewerStore = services?.viewerStore || null;
    const events = Array.isArray(services?.logData?.events) ? services.logData.events : [];
    const activeFilterQueries = viewerStore?.activeFilterQueries?.value || [];
    const selectedEvent = viewerStore?.selectedEvent?.value || null;
    const filteredEvents = viewerStore?.filteredEvents?.value || events;
    const jumpTarget = viewerStore?.logJump?.value || null;
    const bookmarkState = viewerStore?.bookmarks?.value || null;
    
    const rootRef = useRef(null);
    const chartRegionRef = useRef(null);
    const logRegionRef = useRef(null);
    const logBodyRef = useRef(null);
    const logListRef = useRef(null);
    const measureRef = useRef(null);
    const commandBarRef = useRef(null);
    const chartControllerRef = useRef(null);
    const scrollFrameRef = useRef(0);
    const pendingFilterRef = useRef(0);
    const pendingJumpRef = useRef(null);
    const highlightNonceRef = useRef(0);
    
    const filteredRef = useRef(events);
    const indexByRowIdRef = useRef(buildIndexByRowId(events));
    const eventByRowIdRef = useRef(buildEventByRowId(events));

    const viewMode = viewerStore?.mainViewMode?.value || VIEW_MODE_SPLIT;
    const chartSplit = viewerStore?.mainViewSplitSizes?.value || [36, 64];
    const [chartTypes, setChartTypes] = useState([]);
    const selectedChartType = viewerStore?.chartType?.value || "";
    const [rowStride, setRowStride] = useState(38);
    const [highlightState, setHighlightState] = useState({ rowId: null, nonce: 0 });

    filteredRef.current = filteredEvents;
    const selectedRowId = selectedEvent?.row_id ?? null;

    appHooks.useSplit({
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
          viewerStore?.setMainViewSplitSizes?.(sizes);
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

      if (selected) {
        viewerStore?.setSelectedEvent(selected);
      }
      return selected;
    };

    const applyFilterQueries = (queries) => {
      const terms = (queries || []).map((query) => String(query || "").trim()).filter(Boolean);
      const requestId = ++pendingFilterRef.current;

      if (!terms.length) {
        viewerStore?.setFilteredEvents(events);
        return;
      }

      const query = terms.join(" OR ");
      const predicates = terms
        .map((term) => getQueryPredicate(term))
        .filter((predicate) => typeof predicate === "function");
      if (requestId !== pendingFilterRef.current) return;
      if (!predicates.length) {
        viewerStore?.setFilteredEvents(events);
        return;
      }
      viewerStore?.setFilteredEvents(
        events.filter((event) => predicates.some((predicate) => predicate(event)))
      );
    };

    const emitScrollState = () => {
      const container = logBodyRef.current;
      const list = filteredRef.current;
      if (!container || !list.length) return;

      const index = Math.max(
        0,
        Math.min(
          list.length - 1,
          Math.floor((container.scrollTop + container.clientHeight / 2) / rowStride)
        )
      );
      const current = list[index] || null;
      if (current) {
        viewerStore?.setLogScroll?.({ seconds: current.norm_time, rowId: current.row_id });
      }
    };

    const virtual = appHooks.useVirtualList({
      containerRef: logBodyRef,
      itemCount: filteredEvents.length,
      rowHeight: rowStride,
      overscan: 10,
      maxVisible: 180,
      dependencies: [bookmarkState],
    });

    const visibleItems = filteredEvents.slice(virtual.startIndex, virtual.endIndex);

    if (typeof useLayoutEffect === "function") {
      useLayoutEffect(() => {
        const root = rootRef.current;
        if (!root || !services?.logData) return undefined;

        const controller = LogMainViewChart?.mount?.(root, services);
        if (!controller) return undefined;

        chartControllerRef.current = controller;
        controller.attachToolbar({ commandBarEl: commandBarRef.current });

        const types = controller.listTypes();
        setChartTypes(types);

        if (!types.length) {
          viewerStore?.setChartType("");
          return () => {
            controller.destroy?.();
            chartControllerRef.current = null;
          };
        }

        const preferredType = viewerStore?.chartType?.value || "";
        const nextType = types.some((type) => type.id === preferredType)
          ? preferredType
          : types.some((type) => type.id === controller.getCurrentType())
            ? controller.getCurrentType()
            : types[0].id;
        viewerStore?.setChartType(nextType);
        controller.setType(nextType);

        return () => {
          controller.destroy?.();
          chartControllerRef.current = null;
        };
      }, [services]);

      useLayoutEffect(() => {
        const host = measureRef.current;
        const list = logListRef.current;
        if (!host || !list || !events.length) return;

        const sample = host.firstElementChild;
        if (!sample) return;

        const rowHeight = sample.getBoundingClientRect().height || 38;
        const listStyle = getComputedStyle(list);
        const gap = parseFloat(listStyle.rowGap || listStyle.gap || "0") || 0;

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
        pendingFilterRef.current += 1;
        pendingJumpRef.current = null;
      }, [events]);

      useEffect(() => {
        filteredRef.current = filteredEvents;
        indexByRowIdRef.current = buildIndexByRowId(filteredEvents);
      }, [filteredEvents]);

      useEffect(() => {
        if (!viewerStore) return;
        applyFilterQueries(activeFilterQueries);
      }, [viewerStore, events, activeFilterQueries.join("\u0000")]);

      useEffect(() => {
        const pendingJump = pendingJumpRef.current || jumpTarget;
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
      }, [filteredEvents, rowStride, jumpTarget?.nonce ?? 0]);

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
          onChartTypeChange=${(nextType) => {
            viewerStore?.setChartType(nextType);
          }}
          onViewModeChange=${(nextMode) => {
            viewerStore?.setMainViewMode(nextMode);
          }}
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
            bookmarkState=${bookmarkState}
            logListRef=${logListRef}
          />
        </div>
      </div>
    `;
};
