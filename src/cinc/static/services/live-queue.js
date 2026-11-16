// Live stream messages for the viewer store.

// Run `fn` once, on the next frame or after 100 ms, whichever comes first.
const defaultSchedule = (fn) => {
  let done = false;
  const run = () => { if (!done) { done = true; fn(); } };
  requestAnimationFrame(run);
  setTimeout(run, 100);
};

// Messages are queued and applied together once per frame, so a fast stream
// re-renders (and copies the event array) once per frame, not once per message.
// A timer stands in when no frames run (hidden tab).
export const createLiveQueue = (store, { schedule = defaultSchedule } = {}) => {
  let pending = [];
  let scheduled = false;
  const flush = () => {
    scheduled = false;
    const messages = pending;
    pending = [];
    if (!messages.length) return;
    // An update may be for an event added by an earlier queued message.
    const added = [];
    const addedIndex = new Map();
    const updates = new Map();
    const tracks = [];
    messages.forEach((msg) => {
      (Array.isArray(msg.updates) ? msg.updates : []).forEach((event) => {
        const key = String(event.row_id);
        if (addedIndex.has(key)) added[addedIndex.get(key)] = event;
        else updates.set(key, event);
      });
      msg.events.forEach((event) => {
        addedIndex.set(String(event.row_id), added.length);
        added.push(event);
      });
      if (Array.isArray(msg.tracks)) tracks.push(...msg.tracks);
    });
    store.applyLiveEvents(added, Array.from(updates.values()));
    if (tracks.length) store.applyLiveTracks(tracks);
  };
  return (msg) => {
    pending.push(msg);
    if (scheduled) return;
    scheduled = true;
    schedule(flush);
  };
};
