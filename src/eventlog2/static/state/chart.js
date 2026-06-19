import { signal } from "preact/signals";
import { readStorage, syncSignalToStorage, setSignal, STRING_STORAGE, STORAGE_KEYS } from "./storage.js";

export const createChartState = () => {
  const chartType = signal(readStorage(STORAGE_KEYS.chartType, "", STRING_STORAGE));
  const timelineView = signal(readStorage(STORAGE_KEYS.chartTimelineView, "events", STRING_STORAGE) || "events");

  syncSignalToStorage(STORAGE_KEYS.chartType, chartType, STRING_STORAGE);
  syncSignalToStorage(STORAGE_KEYS.chartTimelineView, timelineView, STRING_STORAGE);

  const setChartType = (v) => setSignal(chartType, v, { normalize: (val) => String(val || "") });
  const setTimelineView = (v) => setSignal(timelineView, v, { normalize: (val) => String(val || "events") });

  return { chartType, timelineView, setChartType, setTimelineView };
};
