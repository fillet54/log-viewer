import { html, Fragment, hooks, appHooks } from "logview/lib";
import { SearchPanelHeader, SEARCH_TAB_HISTORY, SEARCH_TAB_BOOKMARKS } from "./SearchPanelHeader.js";
import { SearchSidebar } from "./SearchSidebar.js";
import { SearchResultsPane } from "./SearchResultsPane.js";
import { SearchHelpDialog } from "./SearchHelpDialog.js";
import { SearchControls } from "./SearchControls.js";
import { getQueryPredicate, describeQueryError } from "../../../services/search.js";
import { buildOffsets } from "../../row-metrics.js";

const { useEffect, useRef, useState } = hooks;

// Top-level fields per log type, for the help dialog: the union over (up to) the first 500 rows of
// each type, since rows of one type don't all carry the same optional fields.
const getSearchFieldGroups = (events, logTypes) => {
    const groups = new Map();
    (Array.isArray(events) ? events : []).forEach((event) => {
      if (!event || typeof event !== "object") return;
      const id = String(event.log_type || "");
      const group = groups.get(id) || { id, name: logTypes?.[id]?.name || id || "Rows", fields: new Map(), seen: 0 };
      groups.set(id, group);
      if (group.seen >= 500) return;
      group.seen += 1;
      Object.entries(event).forEach(([key, value]) => {
        const nested =
          value != null &&
          typeof value === "object" &&
          (!Array.isArray(value) ? Object.keys(value).length > 0 : value.some((item) => item && typeof item === "object"));
        group.fields.set(key, Boolean(group.fields.get(key)) || nested);
      });
    });
    return Array.from(groups.values()).map((group) => ({
      id: group.id,
      name: group.name,
      fields: Array.from(group.fields, ([name, nested]) => ({ name, nested })).sort((a, b) => a.name.localeCompare(b.name)),
    }));
  };

const getBookmarkEvents = ({ events, bookmarkState, commentState }) => {
    const bookmarkIds = Object.keys(bookmarkState || {});
    const commentRowIds = Array.isArray(commentState)
      ? commentState.map((item) => String(item?.row_id ?? ""))
      : [];
    const ids = new Set([...bookmarkIds, ...commentRowIds.filter(Boolean)]);
    return Array.from(ids)
      .map((id) => events.find((entry) => String(entry.row_id) === String(id)))
      .filter(Boolean)
      .sort((left, right) => Date.parse(left.time || "") - Date.parse(right.time || ""));
  };

export const SearchPanel = () => {
    const services = appHooks.useAppServices();
    const viewerStore = services?.viewerStore || null;
    const events = Array.isArray(services?.logData?.events) ? services.logData.events : [];
    const logTypes = services?.logTypes || {};
    const activityEnabled = viewerStore?.activityEnabled !== false;
    const currentTab = viewerStore?.searchTab?.value || SEARCH_TAB_HISTORY;
    const [query, setQuery] = useState ? useState("") : ["", () => {}];
    const [results, setResults] = useState ? useState([]) : [[], () => {}];
    const [searchedQuery, setSearchedQuery] = useState ? useState(null) : [null, () => {}];
    const [searchError, setSearchError] = useState ? useState(null) : [null, () => {}];
    const [rowStride, setRowStride] = useState ? useState(28) : [28, () => {}];
    const rowHeights = new Map(
      Object.entries(services?.logTypes || {}).map(([id, config]) => [
        id,
        Number(config.rowHeight) || rowStride,
      ])
    );
    const offsets = buildOffsets(results, rowHeights, rowStride);
    const [resultsVersion, setResultsVersion] = useState ? useState(0) : [0, () => {}];
    const bookmarkState = viewerStore?.bookmarks?.value || null;
    const commentState = viewerStore?.comments?.value || [];
    const history = viewerStore?.searchHistory?.value || [];
    const pinned = viewerStore?.searchPinned?.value || [];
    const filters = viewerStore?.searchFilters?.value || [];
    const searchSplitSizes = viewerStore?.searchSplitSizes?.value || [28, 72];
    const bookmarkRevision = Object.keys(bookmarkState || {}).sort().join("\u0000");
    const commentRevision = Array.isArray(commentState)
      ? commentState
          .map((item) => `${item?.id || ""}:${item?.row_id || ""}:${item?.parent_id || ""}`)
          .join("\u0000")
      : "";

    const pendingSearchRef = useRef(0);
    const initializedRef = useRef(false);
    const rowMeasuredRef = useRef(false);
    const splitLeftRef = useRef(null);
    const splitRightRef = useRef(null);
    const resultsRef = useRef(null);
    const measureRef = useRef(null);
    const helpDialogRef = useRef(null);
    const queryInputRef = useRef(null);

    const bookmarkEvents = activityEnabled
      ? getBookmarkEvents({
          events,
          bookmarkState,
          commentState,
        })
      : [];

    const rowVersion = resultsVersion;
    const fieldGroups = getSearchFieldGroups(events, services?.logTypes);

    appHooks.useSplit({
      refs: [splitLeftRef, splitRightRef],
      enabled: true,
      options: {
        sizes: searchSplitSizes,
        minSize: [160, 320],
        gutterSize: 8,
        elementStyle: (dimension, size, gutterSizeValue) => ({
          "flex-basis": `calc(${size}% - ${gutterSizeValue}px)`,
        }),
        gutterStyle: (dimension, gutterSizeValue) => ({
          "flex-basis": `${gutterSizeValue}px`,
        }),
        onDragEnd: (sizes) => viewerStore?.setSearchSplitSizes?.(sizes),
      },
      dependencies: [searchSplitSizes[0], searchSplitSizes[1]],
    });

    const virtual = appHooks.useVirtualList({
      containerRef: resultsRef,
      itemCount: currentTab === SEARCH_TAB_BOOKMARKS ? 0 : results.length,
      rowHeight: rowStride,
      offsets,
      overscan: 4,
      maxVisible: 80,
      dependencies: [currentTab, rowVersion],
    });

    const normalizeHistoryItem = (searchQuery, count, color) => ({
      query: searchQuery,
      count,
      color: color || "search",
      label: searchQuery || "(all events)",
    });

    const addHistory = (searchQuery, count, color) => {
      if (searchQuery === "" && !count) return;
      const item = normalizeHistoryItem(searchQuery, count, color);
      viewerStore?.setSearchHistory((current) => {
        const next = current.filter((entry) => entry.query !== searchQuery);
        next.unshift(item);
        return next.slice(0, 50);
      });
    };

    const executeSearch = (commitHistory = false, activeTab = currentTab, rawQuery = query) => {
      const trimmedQuery = String(rawQuery || "").trim();
      const isBookmarks = activeTab === SEARCH_TAB_BOOKMARKS;
      const source = isBookmarks ? bookmarkEvents : events;

      setSearchError(null);
      if (!trimmedQuery) {
        // An empty search would only repeat the log above; bookmarks still list everything.
        setResults(isBookmarks ? source : []);
        setSearchedQuery(null);
        setResultsVersion((value) => value + 1);
        return;
      }
      setSearchedQuery(trimmedQuery);
      // A query that doesn't parse says why instead of doing nothing.
      try {
        getQueryPredicate(trimmedQuery);
      } catch (error) {
        setSearchError(describeQueryError(trimmedQuery, error));
        setResults([]);
        setResultsVersion((value) => value + 1);
        return;
      }

      const requestId = ++pendingSearchRef.current;
      const predicatesByConfig = new Map();
      const getPredicate = (config) => {
        const key = JSON.stringify(config || {});
        if (!predicatesByConfig.has(key)) {
          predicatesByConfig.set(key, getQueryPredicate(trimmedQuery, config));
        }
        return predicatesByConfig.get(key);
      };
      const filtered = source.filter((event) => {
        const config = logTypes[event?.log_type]?.search || undefined;
        const predicate = getPredicate(config);
        return typeof predicate === "function" && predicate(event);
      });
      if (requestId !== pendingSearchRef.current) return;
      setResults(filtered);
      setResultsVersion((value) => value + 1);
      if (commitHistory && !isBookmarks) addHistory(trimmedQuery, filtered.length, filtered[0]?.color);
    };

    if (typeof useEffect === "function") {
      useEffect(() => {
        if (!initializedRef.current) {
          initializedRef.current = true;
          if (currentTab !== SEARCH_TAB_HISTORY) {
            executeSearch(false, currentTab, query);
          }
          return;
        }
        executeSearch(false, currentTab, query);
      }, [currentTab]);

      // Re-run search when events change (live streaming appends new events).
      useEffect(() => {
        if (!initializedRef.current) return;
        executeSearch(false, currentTab, query);
      }, [events]);

      useEffect(() => {
        if (currentTab === SEARCH_TAB_BOOKMARKS) {
          executeSearch(false, currentTab, query);
        }
      }, [currentTab, bookmarkRevision, commentRevision, query, events]);

      useEffect(() => {
        const host = measureRef.current;
        if (!host || !events.length) return;
        const sample = host.firstElementChild;
        if (!sample) return;
        const rowHeight = sample.getBoundingClientRect().height || 28;
        const listStyle = getComputedStyle(host.parentNode || host);
        const gap = parseFloat(listStyle.rowGap || listStyle.gap || "0") || 0;
        if (rowHeight > 0) {
          setRowStride(rowHeight + gap);
          virtual.invalidate();
          // Only jump to top on the initial measurement, not when live
          // streaming appends new events to an already-rendered list.
          if (!rowMeasuredRef.current) {
            rowMeasuredRef.current = true;
            virtual.scrollToIndex(0, "start");
          }
        }
      }, [services, events.length, viewerStore?.density?.value]);
    }

    const resultRowStride = rowStride;
    const visibleItems =
      currentTab === SEARCH_TAB_BOOKMARKS
        ? []
        : results.slice(virtual.startIndex, virtual.endIndex);

    const togglePin = (searchQuery) => {
      viewerStore?.setSearchPinned((currentPinned) => {
        const index = currentPinned.findIndex((item) => item.query === searchQuery);
        if (index >= 0) {
          return currentPinned.filter((item) => item.query !== searchQuery);
        }
        const existing = history.find((entry) => entry.query === searchQuery) || normalizeHistoryItem(searchQuery, 0);
        return [existing, ...currentPinned];
      });
    };

    const promoteFilter = (searchQuery) => {
      if (!searchQuery) return;
      viewerStore?.setSearchFilters((currentFilters) => {
        const existingIndex = currentFilters.findIndex((item) => item.query === searchQuery);
        if (existingIndex >= 0) {
          return currentFilters.map((item, index) =>
            index === existingIndex ? { ...item, enabled: true } : item
          );
        }
        return [{ query: searchQuery, enabled: true }, ...currentFilters];
      });
    };

    const clearHistory = () => {
      const pinnedQueries = new Set(pinned.map((item) => item.query));
      const filterQueries = new Set(filters.map((item) => item.query));
      viewerStore?.setSearchHistory((currentHistory) =>
        currentHistory.filter(
          (item) => pinnedQueries.has(item.query) || filterQueries.has(item.query)
        )
      );
    };

    const openHelp = () => {
      if (helpDialogRef.current && !helpDialogRef.current.open) {
        helpDialogRef.current.showModal();
      }
    };

    // Clicking an example runs it.
    const onSelectExample = (example) => {
      setQuery(example);
      helpDialogRef.current?.close?.();
      if (currentTab === SEARCH_TAB_BOOKMARKS) viewerStore?.setSearchTab?.(SEARCH_TAB_HISTORY);
      executeSearch(true, SEARCH_TAB_HISTORY, example);
      viewerStore?.setBottomCollapsed?.(false);
      queryInputRef.current?.focus?.();
    };

    const selectStoredQuery = (nextQuery) => {
      setQuery(nextQuery);
      executeSearch(false, currentTab, nextQuery);
      viewerStore?.setBottomCollapsed?.(false);
    };

    const toggleFilter = (targetQuery) => {
      viewerStore?.setSearchFilters((currentFilters) =>
        currentFilters.map((item) =>
          item.query === targetQuery ? { ...item, enabled: !item.enabled } : item
        )
      );
    };

    const removeFilter = (targetQuery) => {
      viewerStore?.setSearchFilters((currentFilters) =>
        currentFilters.filter((item) => item.query !== targetQuery)
      );
    };

    const onQueryInput = (eventInput) => {
      setQuery(eventInput.currentTarget.value);
      setSearchedQuery(null);
      setSearchError(null);
      setResults([]);
      setResultsVersion((value) => value + 1);
    };

    // Running a search opens the pane; until then it is only the search bar.
    const runSearch = () => {
      executeSearch(true);
      if (String(query || "").trim()) viewerStore?.setBottomCollapsed?.(false);
    };

    const onQueryKeyDown = (eventKey) => {
      if (eventKey.key === "Enter") runSearch();
    };

    const emptyMessage = searchError
      ? html`<span class="search-error" role="alert">${searchError}</span>`
      : searchedQuery
      ? `No events match “${searchedQuery}”.`
      : "Type a query and press Enter. Results show here; the log above keeps every row.";

    return html`
      <${Fragment}>
        <${SearchPanelHeader}
          currentTab=${currentTab}
          activityEnabled=${activityEnabled}
          onSelectTab=${(tab) =>
            viewerStore?.setSearchTab(
              tab === SEARCH_TAB_BOOKMARKS && !activityEnabled ? SEARCH_TAB_HISTORY : tab
            )}
        >
          <${SearchControls}
            query=${query}
            invalid=${Boolean(searchError)}
            queryInputRef=${queryInputRef}
            onQueryInput=${onQueryInput}
            onQueryKeyDown=${onQueryKeyDown}
            onOpenHelp=${openHelp}
            onRunSearch=${runSearch}
          />
        </${SearchPanelHeader}>
        <div class="pane-body search-pane">
          <div id="search-split" class=${`search-split${currentTab === SEARCH_TAB_BOOKMARKS ? " search-single" : ""}`}>
            <${SearchSidebar}
              currentTab=${currentTab}
              activityEnabled=${activityEnabled}
              splitLeftRef=${splitLeftRef}
              pinned=${pinned}
              history=${history}
              filters=${filters}
              bookmarkEvents=${bookmarkEvents}
              services=${services}
              events=${events}
              comments=${viewerStore || null}
              rowVersion=${rowVersion}
              bookmarkState=${bookmarkState}
              commentState=${commentState}
              onTogglePin=${togglePin}
              onPromoteFilter=${promoteFilter}
              onSelectQuery=${selectStoredQuery}
              onClearHistory=${clearHistory}
              onToggleFilter=${toggleFilter}
              onRemoveFilter=${removeFilter}
            />
            <${SearchResultsPane}
              splitRightRef=${splitRightRef}
              emptyMessage=${emptyMessage}
              resultsRef=${resultsRef}
              currentTab=${currentTab}
              results=${results}
            resultRowStride=${resultRowStride}
            offsets=${offsets}
              virtual=${virtual}
              visibleItems=${visibleItems}
              services=${services}
              events=${events}
              comments=${viewerStore || null}
              activityEnabled=${activityEnabled}
              rowVersion=${rowVersion}
              bookmarkState=${bookmarkState}
              commentState=${commentState}
              measureRef=${measureRef}
            />
          </div>
        </div>
        <${SearchHelpDialog} dialogRef=${helpDialogRef} fieldGroups=${fieldGroups} onSelectExample=${onSelectExample} />
      </${Fragment}>
    `;
};
