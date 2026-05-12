window.LogServices = window.LogServices || {};

LogServices.isStandalone = () => document.body.classList.contains("app-body-standalone");

LogServices.createBookmarkService = ({ logData, viewerStore }) => {
  const events = Array.isArray(logData?.events) ? logData.events : [];
  const validIds = new Set(events.map((event) => String(event.row_id)));
  let bookmarks = {};

  const notify = () => {
    viewerStore?.bumpBookmarkVersion?.();
  };

  const load = () => {
    try {
      const payload = JSON.parse(localStorage.getItem(STORAGE_KEYS.bookmarks) || "{}");
      bookmarks = {};
      Object.entries(payload).forEach(([key, value]) => {
        if (!validIds.has(String(key))) return;
        const index = Math.max(0, Math.min(5, Number(value) || 0));
        if (index > 0) bookmarks[String(key)] = index;
      });
      notify();
    } catch (err) {
      bookmarks = {};
    }
  };
  load();

  const persist = () => {
    localStorage.setItem(STORAGE_KEYS.bookmarks, JSON.stringify(bookmarks));
  };

  const cycle = (rowId) => {
    const key = String(rowId);
    if (!validIds.has(key)) return 0;
    const current = Number(bookmarks[key]) || 0;
    const next = (current + 1) % 6;
    if (next === 0) {
      delete bookmarks[key];
    } else {
      bookmarks[key] = next;
    }
    persist();
    notify();
    return next;
  };

  const setColor = (rowId, colorIndex) => {
    const key = String(rowId);
    if (!validIds.has(key)) return 0;
    const next = Math.max(0, Math.min(5, Number(colorIndex) || 0));
    if (next === 0) {
      delete bookmarks[key];
    } else {
      bookmarks[key] = next;
    }
    persist();
    notify();
    return next;
  };

  const getColor = (rowId) => Number(bookmarks[String(rowId)]) || 0;
  const isBookmarked = (rowId) => getColor(rowId) > 0;
  const getAll = () => Object.keys(bookmarks);
  const getAllWithColors = () => ({ ...bookmarks });

  return { cycle, setColor, getColor, isBookmarked, getAll, getAllWithColors };
};

LogServices.createCommentService = ({ logData, viewerStore }) => {
  const events = Array.isArray(logData?.events) ? logData.events : [];
  const validIds = new Set(events.map((event) => String(event.row_id)));
  let comments = [];

  const notify = () => {
    viewerStore?.bumpCommentVersion?.();
  };

  const load = () => {
    try {
      const payload = JSON.parse(localStorage.getItem(STORAGE_KEYS.comments) || "[]");
      comments = Array.isArray(payload)
        ? payload
            .filter((item) => item && validIds.has(String(item.row_id)) && typeof item.body === "string")
            .map((item) => ({
              id: item.id || `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
              row_id: Number(item.row_id),
              parent_id: item.parent_id ?? null,
              body: item.body,
              created_at: item.created_at || new Date().toISOString(),
            }))
        : [];
      notify();
    } catch (err) {
      comments = [];
    }
  };
  load();

  const persist = () => {
    localStorage.setItem(STORAGE_KEYS.comments, JSON.stringify(comments));
  };

  const addComment = async (rowId, body, parentId = null) => {
    const normalizedRowId = String(rowId);
    if (!validIds.has(normalizedRowId)) return null;
    const comment = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      row_id: Number(rowId),
      parent_id: parentId,
      body,
      created_at: new Date().toISOString(),
    };
    comments = [...comments, comment];
    persist();
    notify();
    return comment;
  };

  const getByRowId = () => {
    const map = new Map();
    comments.forEach((comment) => {
      const key = String(comment.row_id);
      if (!map.has(key)) map.set(key, []);
      map.get(key).push(comment);
    });
    return map;
  };

  const buildThreads = (rowId) => {
    const items = (getByRowId().get(String(rowId)) || []).slice();
    const byId = new Map();
    items.forEach((item) => byId.set(item.id, { ...item, replies: [] }));
    const roots = [];
    items.forEach((item) => {
      const node = byId.get(item.id);
      if (item.parent_id && byId.has(item.parent_id)) {
        byId.get(item.parent_id).replies.push(node);
      } else {
        roots.push(node);
      }
    });
    return roots;
  };

  return { addComment, getByRowId, buildThreads };
};

LogServices.createDisabledBookmarkService = () => ({
  enabled: false,
  cycle() {
    return 0;
  },
  setColor() {
    return 0;
  },
  getColor() {
    return 0;
  },
  isBookmarked() {
    return false;
  },
  getAll() {
    return [];
  },
  getAllWithColors() {
    return {};
  },
});

LogServices.createDisabledCommentService = () => ({
  enabled: false,
  async addComment() {
    return null;
  },
  getByRowId() {
    return new Map();
  },
  buildThreads() {
    return [];
  },
});

LogServices.createRootServices = ({ pageData }) => {
  const pluginValue = pageData && typeof pageData === "object" ? pageData.plugin : null;
  const plugin =
    pluginValue && typeof pluginValue === "object"
      ? pluginValue
      : typeof pluginValue === "string"
        ? { id: pluginValue, name: pluginValue }
        : null;
  const logData = (() => {
    if (!pageData || typeof pageData !== "object") return null;
    if (pageData.logData && typeof pageData.logData === "object") return pageData.logData;
    if (pageData.payload && typeof pageData.payload === "object") return pageData.payload;
    return null;
  })();
  const view =
    pageData && typeof pageData === "object" && pageData.view && typeof pageData.view === "object"
      ? pageData.view
      : {};
  const events = Array.isArray(logData?.events) ? logData.events : [];
  const viewerStore =
    typeof window.EventLog2?.createViewerStore === "function"
      ? window.EventLog2.createViewerStore({ logData })
      : null;
  const standalone = LogServices.isStandalone();
  return {
    plugin,
    view,
    logData,
    viewerStore,
    searchWorker: LogSearch.createWorker(events),
    bookmarks: standalone
      ? LogServices.createDisabledBookmarkService()
      : LogServices.createBookmarkService({ logData, viewerStore }),
    comments: standalone
      ? LogServices.createDisabledCommentService()
      : LogServices.createCommentService({ logData, viewerStore }),
  };
};
