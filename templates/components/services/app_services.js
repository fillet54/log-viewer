window.LogServices = window.LogServices || {};

LogServices.createEventBus = () => {
  const listeners = new Map();
  return {
    on(event, handler) {
      if (!listeners.has(event)) listeners.set(event, new Set());
      listeners.get(event).add(handler);
      return () => listeners.get(event)?.delete(handler);
    },
    emit(event, payload) {
      const handlers = listeners.get(event);
      if (!handlers) return;
      handlers.forEach((handler) => handler(payload));
    },
  };
};

LogServices.createBookmarkService = ({ logData, bus, isLoggedIn }) => {
  const events = Array.isArray(logData?.events) ? logData.events : [];
  const validIds = new Set(events.map((event) => String(event.row_id)));
  const datasetId = logData?.dataset_id;
  const bootId = logData?.boot_id;
  const canPersist = Boolean(isLoggedIn && datasetId && bootId);
  let lastLoginNotice = 0;
  let bookmarks = {};

  const notify = () => {
    if (bus) bus.emit("bookmarks:changed", getAllWithColors());
  };

  const load = async () => {
    if (!canPersist) return;
    try {
      const response = await fetch(
        `/api/bookmarks?dataset_id=${encodeURIComponent(datasetId)}&boot_id=${encodeURIComponent(bootId)}`
      );
      if (!response.ok) return;
      const payload = await response.json();
      const incoming = payload?.bookmarks || {};
      bookmarks = {};
      Object.entries(incoming).forEach(([key, value]) => {
        if (!validIds.has(String(key))) return;
        const index = Math.max(0, Math.min(5, Number(value) || 0));
        if (index > 0) bookmarks[String(key)] = index;
      });
      notify();
    } catch (err) {
      return;
    }
  };
  load();

  const notifyLoginRequired = () => {
    const now = Date.now();
    if (now - lastLoginNotice < 2000) return;
    lastLoginNotice = now;
    window.alert("Please log in to create bookmarks.");
  };

  const persist = async (rowId, colorIndex, previous) => {
    if (!canPersist) return;
    try {
      const response = await fetch("/api/bookmarks", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          dataset_id: datasetId,
          boot_id: bootId,
          row_id: rowId,
          color_index: colorIndex,
        }),
      });
      if (!response.ok) throw new Error("bookmark save failed");
    } catch (err) {
      if (previous === 0) {
        delete bookmarks[String(rowId)];
      } else {
        bookmarks[String(rowId)] = previous;
      }
      notify();
    }
  };

  const cycle = (rowId) => {
    const key = String(rowId);
    if (!validIds.has(key)) return 0;
    const current = Number(bookmarks[key]) || 0;
    if (!canPersist) {
      if (!isLoggedIn) notifyLoginRequired();
      return current;
    }
    const next = (current + 1) % 6;
    if (next === 0) {
      delete bookmarks[key];
    } else {
      bookmarks[key] = next;
    }
    notify();
    persist(rowId, next, current);
    return next;
  };

  const setColor = (rowId, colorIndex) => {
    const key = String(rowId);
    if (!validIds.has(key)) return 0;
    const current = Number(bookmarks[key]) || 0;
    if (!canPersist) {
      if (!isLoggedIn) notifyLoginRequired();
      return current;
    }
    const next = Math.max(0, Math.min(5, Number(colorIndex) || 0));
    if (next === 0) {
      delete bookmarks[key];
    } else {
      bookmarks[key] = next;
    }
    notify();
    persist(rowId, next, current);
    return next;
  };

  const getColor = (rowId) => Number(bookmarks[String(rowId)]) || 0;
  const isBookmarked = (rowId) => getColor(rowId) > 0;
  const getAll = () => Object.keys(bookmarks);
  const getAllWithColors = () => ({ ...bookmarks });

  return { cycle, setColor, getColor, isBookmarked, getAll, getAllWithColors };
};

LogServices.createCommentService = ({ logData, bus, isLoggedIn }) => {
  const events = Array.isArray(logData?.events) ? logData.events : [];
  const datasetId = logData?.dataset_id;
  const bootId = logData?.boot_id;
  const canPersist = Boolean(isLoggedIn && datasetId && bootId);
  let lastLoginNotice = 0;
  let comments = [];

  const notify = () => {
    if (bus) bus.emit("comments:changed", getByRowId());
  };

  const notifyLoginRequired = () => {
    const now = Date.now();
    if (now - lastLoginNotice < 2000) return;
    lastLoginNotice = now;
    window.alert("Please log in to comment.");
  };

  const load = async () => {
    if (!datasetId || !bootId) return;
    try {
      const response = await fetch(
        `/api/comments?dataset_id=${encodeURIComponent(datasetId)}&boot_id=${encodeURIComponent(bootId)}`
      );
      if (!response.ok) return;
      const payload = await response.json();
      comments = Array.isArray(payload?.comments) ? payload.comments : [];
      notify();
    } catch (err) {
      return;
    }
  };
  load();

  const addComment = async (rowId, body, parentId = null) => {
    if (!canPersist) {
      if (!isLoggedIn) notifyLoginRequired();
      return null;
    }
    try {
      const response = await fetch("/api/comments", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          dataset_id: datasetId,
          boot_id: bootId,
          row_id: rowId,
          parent_id: parentId,
          body,
        }),
      });
      if (!response.ok) throw new Error("comment save failed");
      const payload = await response.json();
      if (payload?.comment) {
        comments = [...comments, payload.comment];
        notify();
        return payload.comment;
      }
    } catch (err) {
      return null;
    }
    return null;
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

  const getActivityRows = () => {
    const map = getByRowId();
    const withComments = events.filter((event) => map.has(String(event.row_id)));
    return withComments.sort((a, b) => (a.norm_time || 0) - (b.norm_time || 0));
  };

  return { addComment, getByRowId, buildThreads, getActivityRows, reload: load };
};

LogServices.createRootServices = ({ logData, isLoggedIn }) => {
  const bus = LogServices.createEventBus();
  return {
    bus,
    logData,
    searchWorker: LogSearch.createWorker(logData?.events || []),
    bookmarks: LogServices.createBookmarkService({ logData, bus, isLoggedIn }),
    comments: LogServices.createCommentService({ logData, bus, isLoggedIn }),
  };
};
