import { STORAGE_KEYS } from "../shared.js";

(function () {
  const shared = window.EventLog2.viewerStoreShared;

  const sameBookmarkMap = (left, right) => {
    if (left === right) return true;
    const leftKeys = left ? Object.keys(left) : [];
    const rightKeys = right ? Object.keys(right) : [];
    if (leftKeys.length !== rightKeys.length) return false;
    for (let index = 0; index < leftKeys.length; index += 1) {
      const key = leftKeys[index];
      if (!Object.prototype.hasOwnProperty.call(right || {}, key)) return false;
      if (!Object.is(Number(left[key]) || 0, Number(right[key]) || 0)) return false;
    }
    return true;
  };

  const sameComments = (left, right) => {
    if (left === right) return true;
    if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) return false;
    for (let index = 0; index < left.length; index += 1) {
      const leftItem = left[index];
      const rightItem = right[index];
      if (
        leftItem?.id !== rightItem?.id ||
        String(leftItem?.row_id) !== String(rightItem?.row_id) ||
        leftItem?.parent_id !== rightItem?.parent_id ||
        leftItem?.body !== rightItem?.body ||
        leftItem?.created_at !== rightItem?.created_at
      ) {
        return false;
      }
    }
    return true;
  };

  window.EventLog2 = window.EventLog2 || {};
  window.EventLog2.createViewerActivityState = ({ logData, standalone = false }) => {
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

    const bookmarks = shared.signalFactory(
      enabled ? normalizeBookmarks(shared.readStorage(STORAGE_KEYS.bookmarks, {})) : {}
    );
    const comments = shared.signalFactory(
      enabled ? normalizeComments(shared.readStorage(STORAGE_KEYS.comments, [])) : []
    );
    const commentsByRowId = shared.createComputed(() => {
      const map = new Map();
      comments.value.forEach((comment) => {
        const key = String(comment.row_id);
        if (!map.has(key)) map.set(key, []);
        map.get(key).push(comment);
      });
      return map;
    });

    if (enabled) {
      shared.syncSignalToStorage(STORAGE_KEYS.bookmarks, bookmarks);
      shared.syncSignalToStorage(STORAGE_KEYS.comments, comments);
    }

    const setBookmarks = (nextValue) => {
      return shared.setSignalValue(bookmarks, nextValue, {
        current: (value) => ({ ...(value || {}) }),
        normalize: normalizeBookmarks,
        equals: sameBookmarkMap,
      });
    };

    const setComments = (nextValue) => {
      return shared.setSignalValue(comments, nextValue, {
        current: (value) => value.slice(),
        normalize: normalizeComments,
        equals: sameComments,
      });
    };

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
      const next = (getColor(rowId) + 1) % 6;
      return setColor(rowId, next);
    };

    const getByRowId = () => commentsByRowId.value;

    const buildThreads = (rowId) => {
      const items = (commentsByRowId.value.get(String(rowId)) || []).slice();
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
      enabled,
      activityEnabled: enabled,
      bookmarksEnabled: enabled,
      commentsEnabled: enabled,
      bookmarks,
      comments,
      commentsByRowId,
      setBookmarks,
      setComments,
      cycle,
      setColor,
      getColor,
      isBookmarked,
      getAll,
      getAllWithColors,
      addComment,
      getByRowId,
      buildThreads,
    };
  };
})();
