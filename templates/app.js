{# JS bundle rendered via Jinja to keep components co-located #}
{% include 'components/layout.js' %}

{% include 'components/log_view.js' %}

{% include 'components/top_chart.js' %}

{% include 'components/search_pane.js' %}

{% include 'components/right_pane.js' %}

{% include 'components/search.js' %}

window.LogApp = window.LogApp || {};
LogApp.isLoggedIn = {{ "true" if current_user else "false" }};
LogApp.currentUser = {% if current_user %}{{ {"id": current_user["id"], "name": current_user.get("name"), "email": current_user.get("email")} | tojson }}{% else %}null{% endif %};

LogApp.STORAGE_KEYS = {
  root: "loglayout.split.root",
  top: "loglayout.split.top",
  bottomCollapsed: "loglayout.split.bottom.collapsed",
  search: "loglayout.split.search",
  rootExpanded: "loglayout.split.root.expanded",
  searchHistory: "loglayout.search.history",
  searchPinned: "loglayout.search.pinned",
  searchFilters: "loglayout.search.filters",
  chartTooltips: "loglayout.chart.tooltips",
  bookmarks: "loglayout.bookmarks",
  bookmarkNotes: "loglayout.bookmark.notes",
};

LogApp.createEventBus = () => {
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

LogApp.loadSizes = (key, fallback) => {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return fallback;
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed) || parsed.length !== fallback.length) return fallback;
    if (parsed.some((value) => typeof value !== "number")) return fallback;
    return parsed;
  } catch (err) {
    return fallback;
  }
};

LogApp.saveSizes = (key, sizes) => {
  localStorage.setItem(key, JSON.stringify(sizes));
};

LogApp.loadLogData = () => {
  const logPayload = document.getElementById("log-data");
  if (!logPayload) return null;
  try {
    return JSON.parse(logPayload.textContent || "{}");
  } catch (err) {
    return null;
  }
};

LogApp.smoothScrollTo = (container, targetTop, durationMs = 200, onComplete = null) => {
  const startTop = container.scrollTop;
  const delta = targetTop - startTop;
  if (Math.abs(delta) < 2) {
    container.scrollTop = targetTop;
    if (onComplete) onComplete();
    return;
  }
  const startTime = performance.now();
  const easeOut = (t) => 1 - Math.pow(1 - t, 3);
  const step = (now) => {
    const elapsed = now - startTime;
    const progress = Math.min(1, elapsed / durationMs);
    container.scrollTop = startTop + delta * easeOut(progress);
    if (progress < 1) {
      requestAnimationFrame(step);
    } else if (onComplete) {
      onComplete();
    }
  };
  requestAnimationFrame(step);
};

LogApp.createBookmarkStore = (logData, bus) => {
  const events = Array.isArray(logData?.events) ? logData.events : [];
  const validIds = new Set(events.map((event) => String(event.row_id)));
  const shardId = logData?.shard_id;
  const bootId = logData?.boot_id;
  const canPersist = Boolean(LogApp.isLoggedIn && shardId && bootId);
  let lastLoginNotice = 0;
  let bookmarks = {};

  const notify = () => {
    if (bus) bus.emit("bookmarks:changed", getAllWithColors());
  };

  const load = async () => {
    if (!canPersist) return;
    try {
      const response = await fetch(
        `/api/bookmarks?shard_id=${encodeURIComponent(shardId)}&boot_id=${encodeURIComponent(bootId)}`
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
          shard_id: shardId,
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
      if (!LogApp.isLoggedIn) notifyLoginRequired();
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
      if (!LogApp.isLoggedIn) notifyLoginRequired();
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

LogApp.createCommentStore = (logData, bus) => {
  const events = Array.isArray(logData?.events) ? logData.events : [];
  const shardId = logData?.shard_id;
  const bootId = logData?.boot_id;
  const canPersist = Boolean(LogApp.isLoggedIn && shardId && bootId);
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
    if (!shardId || !bootId) return;
    try {
      const response = await fetch(
        `/api/comments?shard_id=${encodeURIComponent(shardId)}&boot_id=${encodeURIComponent(bootId)}`
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
      if (!LogApp.isLoggedIn) notifyLoginRequired();
      return null;
    }
    try {
      const response = await fetch("/api/comments", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          shard_id: shardId,
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

window.addEventListener("DOMContentLoaded", () => {
  const bus = LogApp.createEventBus();
  const logData = LogApp.loadLogData();
  LogApp.searchWorker = LogApp.createSearchWorker(logData?.events || []);
  LogApp.bookmarks = LogApp.createBookmarkStore(logData, bus);
  LogApp.comments = LogApp.createCommentStore(logData, bus);

  LogApp.initSplits();
  LogApp.initLogList(logData, bus);
  LogApp.initChart(logData, bus);
  LogApp.initSearchPane(logData, bus);
  LogApp.initRightPane(bus);
});
