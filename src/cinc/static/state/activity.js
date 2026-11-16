import { effect, signal, computed } from "preact/signals";
import { readStorage, setSignal, activityKeys, removeLegacyActivityKeys } from "./storage.js";

// Bookmarks and comments are keyed by an event's `row_key` ("<type>:<index>"),
// which, unlike `row_id`, does not change when a live log gains a type.

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
    if (l?.id !== r?.id || l?.row_key !== r?.row_key || l?.parent_id !== r?.parent_id || l?.body !== r?.body || l?.created_at !== r?.created_at) return false;
  }
  return true;
};

// Keep a value under `key` only while it has content.
const syncActivityToStorage = (key, sig, isEmpty) => {
  effect(() => {
    const value = sig.value;
    try {
      if (isEmpty(value)) localStorage.removeItem(key);
      else localStorage.setItem(key, JSON.stringify(value));
    } catch {
      // ignore
    }
  });
};

// An event, or a row key itself.
const rowKeyOf = (eventOrKey) =>
  eventOrKey && typeof eventOrKey === "object" ? String(eventOrKey.row_key ?? "") : String(eventOrKey ?? "");

const newCommentId = () => `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

// Bookmarks and comments belong to one log (`logId`); without one (standalone
// pages) they are disabled. Every function taking a row takes the event (or
// its row_key).
export const createActivityState = ({ logData, allEventsSignal, standalone = false, logId = null }) => {
  const eventsByKey = computed(() => {
    const map = new Map();
    (allEventsSignal?.value || logData?.events || []).forEach((event) => {
      if (event?.row_key != null) map.set(String(event.row_key), event);
    });
    return map;
  });
  const isKnown = (key) => eventsByKey.value.has(key);
  const enabled = !standalone && logId != null && logId !== "";
  const keys = enabled ? activityKeys(logId) : null;
  removeLegacyActivityKeys();

  const normalizeBookmarks = (value) => {
    if (!enabled || !value || typeof value !== "object" || Array.isArray(value)) return {};
    const next = {};
    Object.entries(value).forEach(([key, colorIndex]) => {
      if (!isKnown(key)) return;
      const index = Math.max(0, Math.min(5, Number(colorIndex) || 0));
      if (index > 0) next[key] = index;
    });
    return next;
  };

  const normalizeComments = (value) => {
    if (!enabled || !Array.isArray(value)) return [];
    return value
      .filter((item) => item && isKnown(String(item.row_key)) && typeof item.body === "string")
      .map((item) => ({
        id: item.id || newCommentId(),
        row_key: String(item.row_key),
        parent_id: item.parent_id ?? null,
        body: item.body,
        created_at: item.created_at || new Date().toISOString(),
      }));
  };

  const bookmarks = signal(enabled ? normalizeBookmarks(readStorage(keys.bookmarks, {})) : {});
  const comments = signal(enabled ? normalizeComments(readStorage(keys.comments, [])) : []);
  const commentsByRowKey = computed(() => {
    const map = new Map();
    comments.value.forEach((comment) => {
      if (!map.has(comment.row_key)) map.set(comment.row_key, []);
      map.get(comment.row_key).push(comment);
    });
    return map;
  });

  if (enabled) {
    syncActivityToStorage(keys.bookmarks, bookmarks, (value) => !Object.keys(value || {}).length);
    syncActivityToStorage(keys.comments, comments, (value) => !value?.length);
  }

  const setBookmarks = (v) => setSignal(bookmarks, v, { normalize: normalizeBookmarks, equals: sameBookmarkMap });
  const setComments = (v) => setSignal(comments, v, { normalize: normalizeComments, equals: sameComments });

  const getColor = (event) => Number(bookmarks.value[rowKeyOf(event)]) || 0;
  const isBookmarked = (event) => getColor(event) > 0;
  // Row keys of bookmarked rows.
  const getAll = () => Object.keys(bookmarks.value);
  const getAllWithColors = () => ({ ...(bookmarks.value || {}) });
  // The current event for a row key (it may have been re-derived since).
  const eventForKey = (key) => eventsByKey.value.get(String(key)) || null;

  const setColor = (event, colorIndex) => {
    if (!enabled) return 0;
    const key = rowKeyOf(event);
    if (!isKnown(key)) return 0;
    const next = Math.max(0, Math.min(5, Number(colorIndex) || 0));
    setBookmarks((current) => {
      const updated = { ...current };
      if (next === 0) delete updated[key];
      else updated[key] = next;
      return updated;
    });
    return next;
  };

  const cycle = (event) => {
    if (!enabled) return 0;
    return setColor(event, (getColor(event) + 1) % 6);
  };

  // Map of row key -> comments on that row.
  const getByRowKey = () => commentsByRowKey.value;

  const buildThreads = (event) => {
    const items = (commentsByRowKey.value.get(rowKeyOf(event)) || []).slice();
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

  const addComment = async (event, body, parentId = null) => {
    if (!enabled) return null;
    const key = rowKeyOf(event);
    if (!isKnown(key)) return null;
    const nextBody = String(body || "").trim();
    if (!nextBody) return null;
    const comment = {
      id: newCommentId(),
      row_key: key,
      parent_id: parentId ?? null,
      body: nextBody,
      created_at: new Date().toISOString(),
    };
    setComments((current) => [...current, comment]);
    return comment;
  };

  return {
    enabled, activityEnabled: enabled, bookmarksEnabled: enabled, commentsEnabled: enabled,
    bookmarks, comments, commentsByRowKey,
    setBookmarks, setComments, cycle, setColor, getColor, isBookmarked,
    getAll, getAllWithColors, eventForKey, addComment, getByRowKey, buildThreads,
  };
};
