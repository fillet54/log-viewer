window.LogServices = window.LogServices || {};

const MATCH_CHANNELS = ["A", "B", "C", "D"];

const normalizeAction = (value) => String(value || "").trim().toLowerCase();

const eventChannelList = (event) => {
  const incoming = Array.isArray(event?.channels) ? event.channels : [];
  const unique = new Set(
    incoming
      .map((channel) => String(channel || "").trim().toUpperCase())
      .filter((channel) => MATCH_CHANNELS.includes(channel))
  );
  return MATCH_CHANNELS.filter((channel) => unique.has(channel));
};

const formatDurationLabel = (seconds) => {
  const value = Number(seconds);
  if (!Number.isFinite(value) || value < 0) return "";
  return `${Math.round(value)}s`;
};

const buildPairKey = (event, channel) =>
  [
    String(event?.system || "").trim(),
    String(event?.subsystem || "").trim(),
    String(event?.unit || "").trim(),
    String(event?.code || "").trim(),
    channel,
  ].join("|");

const summarizeMatches = (event, channelMatches) => {
  const channels = eventChannelList(event);
  if (!channels.length) return { items: [], collapsed: false };

  const matches = channels
    .map((channel) => channelMatches[channel])
    .filter((item) => item && item.linkedRowId != null && item.durationSeconds != null);

  if (!matches.length) return { items: [], collapsed: false };

  const reference = matches[0];
  const allChannelsMatched =
    matches.length === channels.length &&
    matches.every(
      (item) =>
        item.linkedRowId === reference.linkedRowId &&
        item.direction === reference.direction &&
        item.label === reference.label
    );

  if (allChannelsMatched && channels.length > 1) {
    return {
      collapsed: true,
      items: [
        {
          channelLabel: "ALL",
          label: reference.label,
          linkedRowId: reference.linkedRowId,
          direction: reference.direction,
          title: reference.title,
          durationSeconds: reference.durationSeconds,
        },
      ],
    };
  }

  return {
    collapsed: false,
    items: channels
      .map((channel) => {
        const item = channelMatches[channel];
        if (!item || item.linkedRowId == null || item.durationSeconds == null) return null;
        return {
          channelLabel: channel,
          label: item.label,
          linkedRowId: item.linkedRowId,
          direction: item.direction,
          title: item.title,
          durationSeconds: item.durationSeconds,
        };
      })
      .filter(Boolean),
  };
};

LogServices.enrichEvents = (events) => {
  const list = Array.isArray(events) ? events : [];
  const enriched = list.map((event) => ({
    ...event,
    channels: eventChannelList(event),
    pairedChannels: {},
    matchSummary: { items: [], collapsed: false },
  }));

  const ordered = enriched
    .slice()
    .sort((a, b) => (Number(a.norm_time) || 0) - (Number(b.norm_time) || 0) || (Number(a.row_id) || 0) - (Number(b.row_id) || 0));

  const openSets = new Map();

  ordered.forEach((event) => {
    const action = normalizeAction(event.set_clear);
    event.channels.forEach((channel) => {
      const key = buildPairKey(event, channel);
      if (!openSets.has(key)) openSets.set(key, []);
      const queue = openSets.get(key);

      if (action === "set") {
        queue.push(event);
        return;
      }

      if (action !== "clear" || !queue.length) return;

      const setEvent = queue.shift();
      const durationSeconds = Math.max(0, (Number(event.norm_time) || 0) - (Number(setEvent.norm_time) || 0));
      const label = formatDurationLabel(durationSeconds);

      setEvent.pairedChannels[channel] = {
        channel,
        linkedRowId: event.row_id,
        linkedSeconds: event.norm_time,
        durationSeconds,
        direction: "forward",
        label,
        title: `Jump to clear event for channel ${channel}`,
      };

      event.pairedChannels[channel] = {
        channel,
        linkedRowId: setEvent.row_id,
        linkedSeconds: setEvent.norm_time,
        durationSeconds,
        direction: "back",
        label,
        title: `Jump to set event for channel ${channel}`,
      };
    });
  });

  enriched.forEach((event) => {
    event.matchSummary = summarizeMatches(event, event.pairedChannels);
  });

  return enriched;
};

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
  const events = LogServices.enrichEvents(logData?.events || []);
  const enrichedLogData = {
    ...(logData || {}),
    events,
  };
  return {
    bus,
    logData: enrichedLogData,
    searchWorker: LogSearch.createWorker(events),
    bookmarks: LogServices.createBookmarkService({ logData: enrichedLogData, bus, isLoggedIn }),
    comments: LogServices.createCommentService({ logData: enrichedLogData, bus, isLoggedIn }),
  };
};
