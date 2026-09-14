import { signal } from "preact/signals";
import { createNavigationState } from "./navigation.js";
import { createLayoutState } from "./layout.js";
import { createSearchState } from "./search.js";
import { createChartState } from "./chart.js";
import { createActivityState } from "./activity.js";

export const createViewerStore = ({ logData, standalone = false }) => {
  const initialEvents = Array.isArray(logData?.events) ? logData.events : [];
  const allEventsSignal = signal(initialEvents);
  const timeReferenceRowId = signal(initialEvents[0]?.row_id ?? null);

  const setTimeReference = (eventOrRowId) => {
    const rowId = eventOrRowId?.row_id ?? eventOrRowId;
    timeReferenceRowId.value = rowId == null ? null : rowId;
  };

  const getRelativeTime = (event) => {
    const reference = allEventsSignal.value.find(
      (entry) => String(entry?.row_id) === String(timeReferenceRowId.value)
    );
    const eventMs = Date.parse(event?.time || event?.utctime || "");
    const referenceMs = Date.parse(reference?.time || reference?.utctime || "");
    if (!Number.isFinite(eventMs) || !Number.isFinite(referenceMs)) return null;
    return (eventMs - referenceMs) / 1000;
  };

  const appendEvents = (newEvents) => {
    if (!Array.isArray(newEvents) || !newEvents.length) return;
    allEventsSignal.value = [...allEventsSignal.value, ...newEvents];
  };

  return {
    get allEvents() { return allEventsSignal.value; },
    allEventsSignal,
    appendEvents,
    timeReferenceRowId,
    setTimeReference,
    getRelativeTime,
    ...createNavigationState({ allEvents: initialEvents }),
    ...createLayoutState(),
    ...createSearchState(),
    ...createChartState(),
    ...createActivityState({ logData, allEventsSignal, standalone }),
  };
};
