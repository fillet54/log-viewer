(function () {
  const ui = window.EventLog2UI || {};
  const html = ui.html;
  const Fragment = ui.Fragment;
  const useEffect = ui.hooks?.useEffect || null;
  const useLayoutEffect = ui.hooks?.useLayoutEffect || null;
  const useRef = ui.hooks?.useRef || null;
  const useState = ui.hooks?.useState || null;

  ui.components = ui.components || {};

  const HELP_EXAMPLES = [
    "name:temp_core",
    "system:Power",
    "color:Red",
    "data.bus.load_pct>=68",
    "description~timeout",
    "system:Power OR system:Thermal",
    "NOT color:Green",
    "$:sensor",
    "$.*:writer",
  ];

  const SEARCH_TAB_HISTORY = "history";
  const SEARCH_TAB_FILTERS = "filters";
  const SEARCH_TAB_BOOKMARKS = "bookmarks";

  const SearchPanelHeader = ({ currentTab, activityEnabled, onSelectTab }) => html`
    <div class="pane-header compact-header pane-header-spread">
      <span id="search-header-text" class="section-label">Search</span>
      <div id="search-header-tabs" class="search-tabs hidden">
        <button
          id="tab-history"
          class=${`button button-ghost button-xs search-tab${currentTab === SEARCH_TAB_HISTORY ? " is-active" : ""}`}
          onClick=${() => onSelectTab(SEARCH_TAB_HISTORY)}
        >
          Search History
        </button>
        <button
          id="tab-filters"
          class=${`button button-ghost button-xs search-tab${currentTab === SEARCH_TAB_FILTERS ? " is-active" : ""}`}
          onClick=${() => onSelectTab(SEARCH_TAB_FILTERS)}
        >
          Filters
        </button>
        <button
          id="tab-bookmarks"
          class=${`button button-ghost button-xs search-tab${currentTab === SEARCH_TAB_BOOKMARKS ? " is-active" : ""}${activityEnabled ? "" : " hidden"}`}
          onClick=${() => onSelectTab(SEARCH_TAB_BOOKMARKS)}
        >
          Bookmarks
        </button>
      </div>
      <button id="toggle-bottom" class="button button-ghost button-xs" title="Toggle search pane">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="tool-icon">
          <path stroke-linecap="round" stroke-linejoin="round" d="M6 10l6 6 6-6" />
        </svg>
      </button>
    </div>
  `;

  const SearchItemRow = ({ item, isPinned, onPinToggle, onPromote, onSelect }) => html`
    <div class="search-item search-history-item" onClick=${onSelect}>
      <span class="search-query">${item.label}</span>
      <button
        type="button"
        class=${`pin-button${isPinned ? " is-pinned" : ""}`}
        title=${isPinned ? "Unpin" : "Pin"}
        onClick=${(event) => {
          event.stopPropagation();
          onPinToggle();
        }}
      >
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7">
          <path stroke-linecap="round" stroke-linejoin="round" d="M9 3h6l-1 6 3 3-1.5 1.5L12 10l-3.5 3.5L7 12l3-3-1-6Z" />
          <path stroke-linecap="round" stroke-linejoin="round" d="M12 10v9" />
        </svg>
      </button>
      <button
        type="button"
        class="pin-button promote-button"
        title="Promote to filter"
        onClick=${(event) => {
          event.stopPropagation();
          onPromote();
        }}
      >
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7">
          <path stroke-linecap="round" stroke-linejoin="round" d="M4 5h16l-6 7v6l-4 2v-8L4 5Z" />
        </svg>
      </button>
      <span class="search-time">${item.count}</span>
    </div>
  `;

  const FilterItemRow = ({ filter, onToggle, onRemove }) => html`
    <div class="search-item search-filter-item">
      <span>${filter.query}</span>
      <button
        type="button"
        class=${`filter-toggle${filter.enabled ? " is-on" : ""}`}
        aria-pressed=${String(filter.enabled)}
        onClick=${onToggle}
      ></button>
      <button type="button" class="pin-button" title="Remove filter" onClick=${onRemove}>
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7">
          <path stroke-linecap="round" stroke-linejoin="round" d="M6 6l12 12M18 6l-12 12" />
        </svg>
      </button>
    </div>
  `;

  const ReadOnlyCommentThread = ({ threads, depth = 0 }) => {
    if (!threads.length) return null;
    return html`
      <div class="comment-thread">
        ${threads.map(
          (comment) => html`
            <div class="comment-item" style=${{ marginLeft: `${depth * 16}px` }}>
              <div class="comment-meta">
                <span class="comment-time">${comment.created_at}</span>
              </div>
              <div class="comment-body">${comment.body}</div>
              ${comment.replies?.length
                ? html`<${ReadOnlyCommentThread} threads=${comment.replies} depth=${depth + 1} />`
                : null}
            </div>
          `
        )}
      </div>
    `;
  };

  const SearchHelpDialog = ({ dialogRef, fields, onSelectExample }) => html`
    <dialog ref=${dialogRef} id="search-help-dialog" class="search-help-dialog">
      <form method="dialog" class="search-help-card">
        <div class="search-help-header">
          <div>
            <div class="section-label">Search Help</div>
            <div class="search-help-title">Query Syntax</div>
          </div>
          <button class="button button-ghost button-xs" value="close" aria-label="Close search help">Close</button>
        </div>
        <div class="search-help-body">
          <div class="search-help-section">
            <div class="search-help-section-title">Basics</div>
            <div class="support-text">
              Bare terms search across the main event fields and also prefix-match the event <code>name</code>.
              For example, typing <code>temp</code> will match names that start with <code>temp</code> and exact
              matching values in common fields.
            </div>
            <div class="support-text">
              Use <code>field:value</code> for exact field matching and <code>field~text</code> for substring matching.
              Numeric fields support <code>&gt;</code>, <code>&gt;=</code>, <code>&lt;</code>, and <code>&lt;=</code>.
            </div>
            <div class="support-text">
              Boolean logic is supported with <code>AND</code>, <code>OR</code>, and <code>NOT</code>. If you omit an
              operator between filters, it behaves like an <code>AND</code>.
            </div>
            <div class="support-text">
              Use <code>*</code> as a wildcard for text matching. Use <code>$:key</code> to search object key names and
              <code> $.*:value</code> to search deeply across nested object values.
            </div>
          </div>
          <div class="search-help-section">
            <div class="search-help-section-title">Examples</div>
            <div class="search-help-examples">
              ${HELP_EXAMPLES.map(
                (example) => html`
                  <button
                    type="button"
                    class="search-help-example"
                    data-search-example=${example}
                    onClick=${() => onSelectExample(example)}
                  >
                    ${example}
                  </button>
                `
              )}
            </div>
          </div>
          <div class="search-help-section">
            <div class="search-help-section-title">Searchable Columns</div>
            <div class="support-text">
              These are top-level event fields. Fields marked <code>map</code> contain nested object data and can still
              be queried with dotted paths like <code>data.bus.load_pct</code> when needed.
            </div>
            <div class="search-help-fields">
              ${fields.map(
                (field) => html`
                  <span class="search-help-field">
                    <code>${field.name}</code>${field.nested
                      ? html`<span class="search-help-field-badge">map</span>`
                      : null}
                  </span>
                `
              )}
            </div>
          </div>
        </div>
      </form>
    </dialog>
  `;

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

  const createRenderedRow = ({ event, services, extraClasses = [] }) => {
    const rowTemplate = services?.rowTemplate || null;
    const renderRow = window.EventLog2?.resolveRowRenderer
      ? window.EventLog2.resolveRowRenderer(services?.plugin || null)
      : null;
    if (!rowTemplate || typeof renderRow !== "function") return null;
    return renderRow(event, rowTemplate, {
      extraClasses,
      bookmarks: services?.viewerStore || null,
      view: services?.view || null,
    });
  };

  const attachRowActions = ({ row, event, services, events, activityEnabled }) => {
    if (!row) return null;

    row.querySelector(".bookmark-toggle")?.addEventListener("click", (eventClick) => {
      if (!activityEnabled) return;
      eventClick.stopPropagation();
      const next = services?.viewerStore?.cycle(event.row_id) || 0;
      row.classList.toggle("is-bookmarked", next > 0);
      row.dataset.bookmarkColor = String(next);
    });

    row.querySelectorAll(".match-link").forEach((button) => {
      button.addEventListener("click", (eventClick) => {
        eventClick.stopPropagation();
        const linkedRowId = button.dataset.linkedRowId;
        if (!linkedRowId) return;
        const linkedEvent = events.find((entry) => String(entry.row_id) === String(linkedRowId)) || null;
        if (linkedEvent) {
          services?.viewerStore?.setSelectedEvent(linkedEvent);
        }
        services?.viewerStore?.setLogJump({ rowId: linkedRowId });
      });
    });

    row.addEventListener("click", () => {
      services?.viewerStore?.setSelectedEvent(event);
      services?.viewerStore?.setLogJump({ rowId: event.row_id });
    });

    return row;
  };

  const RenderedRow = ({
    event,
    services,
    events,
    activityEnabled,
    version,
    bookmarkState,
    commentState,
    className = "",
  }) => {
    const ref = useRef();

    useLayoutEffect(() => {
      const host = ref.current;
      if (!host) return;
      host.innerHTML = "";
      const row = createRenderedRow({
        event,
        services,
        extraClasses: ["search-result-row"],
      });
      if (!row) return;
      const mountedRow = attachRowActions({ row, event, services, events, activityEnabled });
      if (mountedRow) host.appendChild(mountedRow);
    }, [event?.row_id, services, events, activityEnabled, version, bookmarkState, commentState]);

    return html`<div ref=${ref} class=${className}></div>`;
  };

  const ActivityItem = ({
    event,
    services,
    events,
    comments,
    activityEnabled,
    version,
    bookmarkState,
    commentState,
  }) => {
    const threads = comments?.buildThreads(event.row_id) || [];
    return html`
      <div class="activity-item">
        <${RenderedRow}
          event=${event}
          services=${services}
          events=${events}
          activityEnabled=${activityEnabled}
          version=${version}
          bookmarkState=${bookmarkState}
          commentState=${commentState}
        />
        ${threads.length
          ? html`
              <div class="activity-thread">
                <${ReadOnlyCommentThread} threads=${threads} />
              </div>
            `
          : null}
      </div>
    `;
  };

  const SearchHistoryView = ({
    currentTab,
    pinned,
    history,
    onTogglePin,
    onPromoteFilter,
    onSelectQuery,
    onClearHistory,
  }) => html`
    <div id="search-history-view" class=${`search-view${currentTab === SEARCH_TAB_HISTORY ? "" : " hidden"}`}>
      <div class="search-section">
        <div class="search-section-title">Pinned</div>
        <div id="search-pinned" class="search-list search-list-compact">
          ${pinned.slice(0, 24).map(
            (item) => html`
              <${SearchItemRow}
                item=${item}
                isPinned=${true}
                onPinToggle=${() => onTogglePin(item.query)}
                onPromote=${() => onPromoteFilter(item.query)}
                onSelect=${() => onSelectQuery(item.query)}
              />
            `
          )}
        </div>
      </div>
      <div class="search-section">
        <div class="search-section-title history-header">
          <span>History</span>
          <button id="clear-history" class="button button-ghost button-xs" onClick=${onClearHistory}>
            Clear
          </button>
        </div>
        <div id="search-history" class="search-list search-list-compact">
          ${history.slice(0, 50).map(
            (item) => html`
              <${SearchItemRow}
                item=${item}
                isPinned=${false}
                onPinToggle=${() => onTogglePin(item.query)}
                onPromote=${() => onPromoteFilter(item.query)}
                onSelect=${() => onSelectQuery(item.query)}
              />
            `
          )}
        </div>
      </div>
    </div>
  `;

  const SearchFilterView = ({ currentTab, filters, onToggleFilter, onRemoveFilter }) => html`
    <div id="search-filter-view" class=${`search-view${currentTab === SEARCH_TAB_FILTERS ? "" : " hidden"}`}>
      <div class="search-section">
        <div class="search-section-title">Filters</div>
        <div id="search-filters" class="search-list search-list-compact">
          ${filters.map(
            (filter) => html`
              <${FilterItemRow}
                filter=${filter}
                onToggle=${() => onToggleFilter(filter.query)}
                onRemove=${() => onRemoveFilter(filter.query)}
              />
            `
          )}
        </div>
      </div>
    </div>
  `;

  const SearchBookmarksView = ({
    currentTab,
    activityEnabled,
    bookmarkEvents,
    services,
    events,
    comments,
    rowVersion,
    bookmarkState,
    commentState,
  }) => html`
    <div
      id="search-bookmark-view"
      class=${`search-view${currentTab === SEARCH_TAB_BOOKMARKS ? "" : " hidden"}${activityEnabled ? "" : " hidden"}`}
    >
      <div class="search-section">
        <div class="search-section-title">Bookmarks</div>
        <div id="search-bookmarks" class="search-list search-list-compact">
          ${bookmarkEvents.map(
            (event) => html`
              <${ActivityItem}
                event=${event}
                services=${services}
                events=${events}
                comments=${comments}
                activityEnabled=${activityEnabled}
                version=${rowVersion}
                bookmarkState=${bookmarkState}
                commentState=${commentState}
              />
            `
          )}
        </div>
      </div>
    </div>
  `;

  const SearchSidebar = ({
    currentTab,
    activityEnabled,
    splitLeftRef,
    pinned,
    history,
    filters,
    bookmarkEvents,
    services,
    events,
    comments,
    rowVersion,
    bookmarkState,
    commentState,
    onTogglePin,
    onPromoteFilter,
    onSelectQuery,
    onClearHistory,
    onToggleFilter,
    onRemoveFilter,
  }) => html`
    <aside id="search-history-pane" ref=${splitLeftRef} class="search-history">
      <${SearchHistoryView}
        currentTab=${currentTab}
        pinned=${pinned}
        history=${history}
        onTogglePin=${onTogglePin}
        onPromoteFilter=${onPromoteFilter}
        onSelectQuery=${onSelectQuery}
        onClearHistory=${onClearHistory}
      />
      <${SearchFilterView}
        currentTab=${currentTab}
        filters=${filters}
        onToggleFilter=${onToggleFilter}
        onRemoveFilter=${onRemoveFilter}
      />
      <${SearchBookmarksView}
        currentTab=${currentTab}
        activityEnabled=${activityEnabled}
        bookmarkEvents=${bookmarkEvents}
        services=${services}
        events=${events}
        comments=${comments}
        rowVersion=${rowVersion}
        bookmarkState=${bookmarkState}
        commentState=${commentState}
      />
    </aside>
  `;

  const SearchControls = ({
    query,
    queryInputRef,
    onQueryInput,
    onQueryKeyDown,
    onOpenHelp,
    onRunSearch,
  }) => html`
    <div class="search-controls">
      <input
        id="search-query"
        ref=${queryInputRef}
        class="text-input text-input-small search-input"
        placeholder="Search logs, faults, codes..."
        value=${query}
        onInput=${onQueryInput}
        onKeyDown=${onQueryKeyDown}
      />
      <button
        type="button"
        id="open-search-help"
        class="button button-ghost button-small search-help-button"
        aria-label="Search syntax help"
        title="Search syntax help"
        onClick=${onOpenHelp}
      >
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true">
          <path stroke-linecap="round" stroke-linejoin="round" d="M9.5 9a2.5 2.5 0 1 1 4.2 1.8c-.8.6-1.2 1-1.2 2.2" />
          <path stroke-linecap="round" stroke-linejoin="round" d="M12 17h.01" />
          <circle cx="12" cy="12" r="9" />
        </svg>
      </button>
      <button id="run-search" class="button button-primary button-small" onClick=${onRunSearch}>
        Search
      </button>
    </div>
  `;

  const SearchResultsContent = ({
    currentTab,
    results,
    resultRowStride,
    virtual,
    visibleItems,
    services,
    events,
    comments,
    activityEnabled,
    rowVersion,
    bookmarkState,
    commentState,
  }) => {
    if (currentTab === SEARCH_TAB_BOOKMARKS) {
      return html`
        <div id="search-results-spacer"></div>
        <div id="search-results-list" class="mono-block">
          ${results.length
            ? results.map(
                (event) => html`
                  <${ActivityItem}
                    event=${event}
                    services=${services}
                    events=${events}
                    comments=${comments}
                    activityEnabled=${activityEnabled}
                    version=${rowVersion}
                    bookmarkState=${bookmarkState}
                    commentState=${commentState}
                  />
                `
              )
            : html`<div class="no-results">No Results</div>`}
        </div>
      `;
    }

    return html`
      <div id="search-results-spacer" style=${{ height: `${results.length * resultRowStride}px` }}></div>
      <div
        id="search-results-list"
        class="mono-block"
        style=${{ transform: `translateY(${virtual.startIndex * resultRowStride}px)` }}
      >
        ${results.length
          ? visibleItems.map(
              (event) => html`
                <${RenderedRow}
                  event=${event}
                  services=${services}
                  events=${events}
                  activityEnabled=${activityEnabled}
                  version=${rowVersion}
                  bookmarkState=${bookmarkState}
                  commentState=${commentState}
                />
              `
            )
          : html`<div class="no-results">No Results</div>`}
      </div>
    `;
  };

  const SearchResultsPane = ({
    splitRightRef,
    query,
    queryInputRef,
    onQueryInput,
    onQueryKeyDown,
    onOpenHelp,
    onRunSearch,
    resultsRef,
    currentTab,
    results,
    resultRowStride,
    virtual,
    visibleItems,
    services,
    events,
    comments,
    activityEnabled,
    rowVersion,
    bookmarkState,
    commentState,
    measureRef,
  }) => html`
    <section id="search-results-pane" ref=${splitRightRef} class="search-results">
      <${SearchControls}
        query=${query}
        queryInputRef=${queryInputRef}
        onQueryInput=${onQueryInput}
        onQueryKeyDown=${onQueryKeyDown}
        onOpenHelp=${onOpenHelp}
        onRunSearch=${onRunSearch}
      />
      <div id="search-results" ref=${resultsRef} class="search-list search-results-list">
        <${SearchResultsContent}
          currentTab=${currentTab}
          results=${results}
          resultRowStride=${resultRowStride}
          virtual=${virtual}
          visibleItems=${visibleItems}
          services=${services}
          events=${events}
          comments=${comments}
          activityEnabled=${activityEnabled}
          rowVersion=${rowVersion}
          bookmarkState=${bookmarkState}
          commentState=${commentState}
        />
      </div>
      <div ref=${measureRef} style=${{ position: "absolute", visibility: "hidden", pointerEvents: "none" }}></div>
    </section>
  `;

  const SearchPanelApp = () => {
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

      if (services?.searchWorker && !isBookmarks) {
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

      const filtered = source.filter(LogSearch.getQueryPredicate(trimmedQuery));
      setResults(filtered);
      setResultsVersion((value) => value + 1);
      if (commitHistory && !isBookmarks) addHistory(trimmedQuery, filtered.length, filtered[0]?.color);
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

  ui.components.LogSearchPanel = SearchPanelApp;
})();
