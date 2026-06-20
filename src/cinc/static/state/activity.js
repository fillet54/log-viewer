import { signal, computed } from "preact/signals";
import { readStorage, syncSignalToStorage, setSignal, STORAGE_KEYS } from "./storage.js";

const sameBookmarkMap = (left, right) => {
  if (left === right) return true;
  const leftKeys = left ? Object.keys(left) : [];
  const rightKeys = right ? Object.keys(right) : [];
  if (leftKeys.length !== rightKeys.length) return false;
  for (const key of leftKeys) {
    if (!Object.prototype.hasOwnProperty.call(right || {}, key)) return false;
    if (!Object.is(Number(left[key]) || 0, Number(right[key]) || 0)) return false;
  }
  return true;
};

const sameComments = (left, right) => {
  if (left === right) return true;
  if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) return false;
  for (let i = 0; i < left.length; i++) {
    const l = left[i]; const r = right[i];
    if (l?.id !== r?.id || String(l?.row_id) !== String(r?.row_id) || l?.parent_id !== r?.parent_id || l?.body !== r?.body || l?.created_at !== r?.created_at) return false;
  }
  return true;
};

export const createActivityState = ({ logData, standalone = false }) => {
  const events = Array.isArray(logData?.events) ? logData.events : [];
  const validIds = new Set(events.map((event) => String(event.row_id)));
  const enabled = !standalone;

  const normalizeBookmarks = (value) => {
    if (!enabled || !value || typeof value !== "object" || Array.isArray(value)) return {};
    const next = {};
    Object.entries(value).forEach(([key, colorIndex]) => {
      const normalizedKey = String(key);
      if (!validIds.has(normalizedKey)) return;
      const index = Math.max(0, Math.min(5, Number(colorIndex) || 0));
      if (index > 0) next[normalizedKey] = index;
    });
    return next;
  };

  const normalizeComments = (value) => {
    if (!enabled || !Array.isArray(value)) return [];
    return value
      .filter((item) => item && validIds.has(String(item.row_id)) && typeof item.body === "string")
      .map((item) => ({
        id: item.id || `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
        row_id: Number(item.row_id),
        parent_id: item.parent_id ?? null,
        body: item.body,
        created_at: item.created_at || new Date().toISOString(),
      }));
  };

  const bookmarks = signal(enabled ? normalizeBookmarks(readStorage(STORAGE_KEYS.bookmarks, {})) : {});
  const comments = signal(enabled ? normalizeComments(readStorage(STORAGE_KEYS.comments, [])) : []);
  const commentsByRowId = computed(() => {
    const map = new Map();
    comments.value.forEach((comment) => {
      const key = String(comment.row_id);
      if (!map.has(key)) map.set(key, []);
      map.get(key).push(comment);
    });
    return map;
  });

  if (enabled) {
    syncSignalToStorage(STORAGE_KEYS.bookmarks, bookmarks);
    syncSignalToStorage(STORAGE_KEYS.comments, comments);
  }

  const setBookmarks = (v) => setSignal(bookmarks, v, { normalize: normalizeBookmarks, equals: sameBookmarkMap });
  const setComments = (v) => setSignal(comments, v, { normalize: normalizeComments, equals: sameComments });

  const getColor = (rowId) => Number(bookmarks.value[String(rowId)]) || 0;
  const isBookmarked = (rowId) => getColor(rowId) > 0;
  const getAll = () => Object.keys(bookmarks.value);
  const getAllWithColors = () => ({ ...(bookmarks.value || {}) });

  const setColor = (rowId, colorIndex) => {
    if (!enabled) return 0;
    const key = String(rowId);
    if (!validIds.has(key)) return 0;
    const next = Math.max(0, Math.min(5, Number(colorIndex) || 0));
    setBookmarks((current) => {
      const updated = { ...current };
      if (next === 0) delete updated[key];
      else updated[key] = next;
      return updated;
    });
    return next;
  };

  const cycle = (rowId) => {
    if (!enabled) return 0;
    return setColor(rowId, (getColor(rowId) + 1) % 6);
  };

  const getByRowId = () => commentsByRowId.value;

  const buildThreads = (rowId) => {
    const items = (commentsByRowId.value.get(String(rowId)) || []).slice();
    const byId = new Map();
    items.forEach((item) => byId.set(item.id, { ...item, replies: [] }));
    const roots = [];
    items.forEach((item) => {
      const node = byId.get(item.id);
      if (item.parent_id && byId.has(item.parent_id)) byId.get(item.parent_id).replies.push(node);
      else roots.push(node);
    });
    return roots;
  };

  const addComment = async (rowId, body, parentId = null) => {
    if (!enabled) return null;
    const key = String(rowId);
    if (!validIds.has(key)) return null;
    const nextBody = String(body || "").trim();
    if (!nextBody) return null;
    const comment = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      row_id: Number(rowId),
      parent_id: parentId ?? null,
      body: nextBody,
      created_at: new Date().toISOString(),
    };
    setComments((current) => [...current, comment]);
    return comment;
  };

  return {
    enabled, activityEnabled: enabled, bookmarksEnabled: enabled, commentsEnabled: enabled,
    bookmarks, comments, commentsByRowId,
    setBookmarks, setComments, cycle, setColor, getColor, isBookmarked,
    getAll, getAllWithColors, addComment, getByRowId, buildThreads,
  };
};
