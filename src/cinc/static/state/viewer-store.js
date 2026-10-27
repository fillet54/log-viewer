import { signal } from "preact/signals";
import { createNavigationState } from "./navigation.js";
import { createLayoutState } from "./layout.js";
import { createSearchState } from "./search.js";
import { createChartState } from "./chart.js";
import { createActivityState } from "./activity.js";

export const createViewerStore = ({ logData, standalone = false }) => {
  const initialEvents = Array.isArray(logData?.events) ? logData.events : [];
  const allEventsSignal = signal(initialEvents);
  const navigation = createNavigationState({ allEvents: initialEvents });
  // null means "the first row"; a row id pins the reference to that row.
  const timeReferenceRowId = signal(null);
  const findEvent = (rowId) =>
    allEventsSignal.value.find((entry) => String(entry?.row_id) === String(rowId)) || null;
  const getTimeReferenceEvent = () =>
    timeReferenceRowId.value == null
      ? allEventsSignal.value[0] || null
      : findEvent(timeReferenceRowId.value) || allEventsSignal.value[0] || null;
  const isTimeReference = (event) =>
    event != null && String(getTimeReferenceEvent()?.row_id) === String(event.row_id);

  // Clicking the reference row again goes back to the first row.
  const setTimeReference = (eventOrRowId) => {
    const rowId = eventOrRowId?.row_id ?? eventOrRowId;
    const same = rowId != null && String(rowId) === String(timeReferenceRowId.value);
    timeReferenceRowId.value = rowId == null || same ? null : rowId;
  };
  const clearTimeReference = () => { timeReferenceRowId.value = null; };

  const getRelativeTime = (event) => {
    const reference = getTimeReferenceEvent();
    const eventMs = Date.parse(event?.time || event?.utctime || "");
    const referenceMs = Date.parse(reference?.time || reference?.utctime || "");
    if (!Number.isFinite(eventMs) || !Number.isFinite(referenceMs)) return null;
    return (eventMs - referenceMs) / 1000;
  };

  // Live mode: add new events and replace earlier ones the server re-derived
  // (e.g. a SET row once its CLEAR arrives), in one update.
  const applyLiveEvents = (newEvents = [], updates = []) => {
    const added = Array.isArray(newEvents) ? newEvents : [];
    const changed = new Map((Array.isArray(updates) ? updates : []).map((e) => [String(e.row_id), e]));
    if (!added.length && !changed.size) return;
    const current = changed.size
      ? allEventsSignal.value.map((event) => changed.get(String(event.row_id)) || event)
      : allEventsSignal.value;
    allEventsSignal.value = [...current, ...added];
    const selected = navigation.selectedEvent.value;
    const replacement = selected && changed.get(String(selected.row_id));
    if (replacement) navigation.selectedEvent.value = replacement;
  };
  const appendEvents = (newEvents) => applyLiveEvents(newEvents, []);

  return {
    get allEvents() { return allEventsSignal.value; },
    allEventsSignal,
    appendEvents,
    applyLiveEvents,
    timeReferenceRowId,
    setTimeReference,
    clearTimeReference,
    getTimeReferenceEvent,
    isTimeReference,
    getRelativeTime,
    ...navigation,
    ...createLayoutState(),
    ...createSearchState(),
    ...createChartState(),
    ...createActivityState({ logData, allEventsSignal, standalone }),
  };
};
