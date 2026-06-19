import { createNavigationState } from "./navigation.js";
import { createLayoutState } from "./layout.js";
import { createSearchState } from "./search.js";
import { createChartState } from "./chart.js";
import { createActivityState } from "./activity.js";

export const createViewerStore = ({ logData, standalone = false }) => {
  const allEvents = Array.isArray(logData?.events) ? logData.events : [];

  return {
    allEvents,
    ...createNavigationState({ allEvents }),
    ...createLayoutState(),
    ...createSearchState(),
    ...createChartState(),
    ...createActivityState({ logData, standalone }),
  };
};
