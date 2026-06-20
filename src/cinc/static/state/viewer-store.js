import { signal } from "preact/signals";
import { createNavigationState } from "./navigation.js";
import { createLayoutState } from "./layout.js";
import { createSearchState } from "./search.js";
import { createChartState } from "./chart.js";
import { createActivityState } from "./activity.js";

export const createViewerStore = ({ logData, standalone = false }) => {
  const initialEvents = Array.isArray(logData?.events) ? logData.events : [];
  const allEventsSignal = signal(initialEvents);

  const appendEvents = (newEvents) => {
    if (!Array.isArray(newEvents) || !newEvents.length) return;
    allEventsSignal.value = [...allEventsSignal.value, ...newEvents];
  };

  return {
    get allEvents() { return allEventsSignal.value; },
    allEventsSignal,
    appendEvents,
    ...createNavigationState({ allEvents: initialEvents }),
    ...createLayoutState(),
    ...createSearchState(),
    ...createChartState(),
    ...createActivityState({ logData, standalone }),
  };
};
