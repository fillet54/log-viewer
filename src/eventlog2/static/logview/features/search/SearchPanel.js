import { SearchPanelHeader, SEARCH_TAB_HISTORY, SEARCH_TAB_BOOKMARKS } from "./SearchPanelHeader.js";
import { SearchSidebar } from "./SearchSidebar.js";
import { SearchResultsPane } from "./SearchResultsPane.js";
import { SearchHelpDialog } from "./SearchHelpDialog.js";
import { createRenderedRow } from "./RenderedRow.js";

const ui = window.EventLog2UI || {};
const LogSearch = window.LogSearch || null;
const html = ui.html;
const Fragment = ui.Fragment;
const useEffect = ui.hooks?.useEffect || null;
const useRef = ui.hooks?.useRef || null;
const useState = ui.hooks?.useState || null;

ui.components = ui.components || {};

const getSearchFieldPaths = (events) => {
    const source = Array.isArray(events) ? events : [];
    const sample = source.find((event) => event && typeof event === "object") || null;
    if (!sample) return [];
    return Object.entries(sample)
      .map(([key, value]) => ({
        name: key,
        nested:
          value != null &&
          typeof value === "object" &&
          (!Array.isArray(value) ? Object.keys(value).length > 0 : value.length > 0),
      }))
      .sort((left, right) => left.name.localeCompare(right.name));
  };

const getBookmarkEvents = ({ events, bookmarks, comments }) => {
    const bookmarkIds = new Set(bookmarks?.getAll() || []);
    const commentRows = comments?.getByRowId() || new Map();
    const ids = new Set([...bookmarkIds, ...Array.from(commentRows.keys())]);
    return Array.from(ids)
      .map((id) => events.find((entry) => String(entry.row_id) === String(id)))
      .filter(Boolean)
      .sort((left, right) => (left.norm_time || 0) - (right.norm_time || 0));
  };

export const SearchPanel = () => {
    const services = ui.appHooks.useAppServices();
    const viewerStore = services?.viewerStore || null;
    const events = Array.isArray(services?.logData?.events) ? services.logData.events : [];
    const activityEnabled = viewerStore?.activityEnabled !== false;
    const currentTab = viewerStore?.searchTab?.value || SEARCH_TAB_HISTORY;
    const [query, setQuery] = useState ? useState("") : ["", () => {}];
    const [results, setResults] = useState ? useState(() => events.slice(0, 200)) : [[], () => {}];
    const [rowStride, setRowStride] = useState ? useState(28) : [28, () => {}];
    const [resultsVersion, setResultsVersion] = useState ? useState(0) : [0, () => {}];
    const bookmarkState = viewerStore?.bookmarks?.value || null;
    const commentState = viewerStore?.comments?.value || [];
    const history = viewerStore?.searchHistory?.value || [];
    const pinned = viewerStore?.searchPinned?.value || [];
    const filters = viewerStore?.searchFilters?.value || [];
    const searchSplitSizes = viewerStore?.searchSplitSizes?.value || [28, 72];

    const pendingSearchRef = useRef ? useRef(0) : { current: 0 };
    const initializedRef = useRef ? useRef(false) : { current: false };
    const splitLeftRef = useRef ? useRef(null) : { current: null };
    const splitRightRef = useRef ? useRef(null) : { current: null };
    const resultsRef = useRef ? useRef(null) : { current: null };
    const measureRef = useRef ? useRef(null) : { current: null };
    const helpDialogRef = useRef ? useRef(null) : { current: null };
    const queryInputRef = useRef ? useRef(null) : { current: null };

    const bookmarkEvents = activityEnabled
      ? getBookmarkEvents({
          events,
          bookmarks: viewerStore || null,
          comments: viewerStore || null,
        })
      : [];

    const rowVersion = resultsVersion;
    const fields = getSearchFieldPaths(events);

    ui.appHooks.useSplit({
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

    const virtual = ui.appHooks.useVirtualList({
      containerRef: resultsRef,
      itemCount: currentTab === SEARCH_TAB_BOOKMARKS ? 0 : results.length,
      rowHeight: rowStride,
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

      if (!trimmedQuery) {
        setResults(source);
        setResultsVersion((value) => value + 1);
        if (commitHistory && !isBookmarks) addHistory(trimmedQuery, source.length, source[0]?.color);
        return;
      }

      if (services?.searchWorker && !isBookmarks && LogSearch?.runQuery) {
        const requestId = ++pendingSearchRef.current;
        LogSearch.runQuery(services.searchWorker, trimmedQuery, (indices) => {
          if (requestId !== pendingSearchRef.current) return;
          const filtered = indices.map((idx) => events[idx]);
          setResults(filtered);
          setResultsVersion((value) => value + 1);
          if (commitHistory) addHistory(trimmedQuery, filtered.length, filtered[0]?.color);
        });
        return;
      }

      const predicate = LogSearch?.getQueryPredicate?.(trimmedQuery);
      const filtered = typeof predicate === "function" ? source.filter(predicate) : source;
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

      useEffect(() => {
        if (currentTab === SEARCH_TAB_BOOKMARKS) {
          executeSearch(false, currentTab, query);
        }
      }, [bookmarkState, commentState]);

      useEffect(() => {
        const host = measureRef.current;
        if (!host || !events.length) return;
        host.innerHTML = "";
        const sample = createRenderedRow({
          event: events[0],
          services,
          extraClasses: ["search-result-row"],
        });
        if (!sample) return;
        sample.style.visibility = "hidden";
        host.appendChild(sample);
        const rowHeight = sample.getBoundingClientRect().height || 28;
        const listStyle = getComputedStyle(host.parentNode || host);
        const gap = parseFloat(listStyle.rowGap || listStyle.gap || "0") || 0;
        host.innerHTML = "";
        if (rowHeight > 0) {
          setRowStride(rowHeight + gap);
          virtual.invalidate();
          virtual.scrollToIndex(0, "start");
        }
      }, [services, events.length]);
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

    const onSelectExample = (example) => {
      setQuery(example);
      helpDialogRef.current?.close?.();
      queryInputRef.current?.focus?.();
    };

    const selectStoredQuery = (nextQuery) => {
      setQuery(nextQuery);
      executeSearch(false, currentTab, nextQuery);
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
      setResults([]);
      setResultsVersion((value) => value + 1);
    };

    const onQueryKeyDown = (eventKey) => {
      if (eventKey.key === "Enter") executeSearch(true);
    };

    return html`
      <${Fragment}>
        <${SearchPanelHeader}
          currentTab=${currentTab}
          activityEnabled=${activityEnabled}
          onSelectTab=${(tab) =>
            viewerStore?.setSearchTab(
              tab === SEARCH_TAB_BOOKMARKS && !activityEnabled ? SEARCH_TAB_HISTORY : tab
            )}
        />
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
              query=${query}
              queryInputRef=${queryInputRef}
              onQueryInput=${onQueryInput}
              onQueryKeyDown=${onQueryKeyDown}
              onOpenHelp=${openHelp}
              onRunSearch=${() => executeSearch(true)}
              resultsRef=${resultsRef}
              currentTab=${currentTab}
              results=${results}
              resultRowStride=${resultRowStride}
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
        <${SearchHelpDialog} dialogRef=${helpDialogRef} fields=${fields} onSelectExample=${onSelectExample} />
      </${Fragment}>
    `;
  };

ui.components.LogSearchPanel = SearchPanel;
