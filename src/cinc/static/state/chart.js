import { signal } from "preact/signals";
import {
  readStorage,
  syncSignalToStorage,
  setSignal,
  ARRAY_STORAGE,
  OBJECT_STORAGE,
  STRING_STORAGE,
  STORAGE_KEYS,
} from "./storage.js";

const RIGHT_SPLIT_DEFAULT = [50, 50];

export const createChartState = () => {
  const chartType = signal(readStorage(STORAGE_KEYS.chartType, "", STRING_STORAGE));
  const timelineView = signal(readStorage(STORAGE_KEYS.chartTimelineView, "events", STRING_STORAGE) || "events");
  // Where the user put each chart: { chartId: "main" | "side" }. Charts not listed use their default slot.
  const chartPlacement = signal(readStorage(STORAGE_KEYS.chartPlacement, {}, OBJECT_STORAGE));
  // Side-dock sections the user collapsed (chart ids).
  const sideCollapsed = signal(readStorage(STORAGE_KEYS.sideCollapsed, [], ARRAY_STORAGE));
  // Right pane: row details above, side dock below.
  const rightSplitSizes = signal(readStorage(STORAGE_KEYS.rightSplit, RIGHT_SPLIT_DEFAULT, ARRAY_STORAGE));

  syncSignalToStorage(STORAGE_KEYS.chartType, chartType, STRING_STORAGE);
  syncSignalToStorage(STORAGE_KEYS.chartTimelineView, timelineView, STRING_STORAGE);
  syncSignalToStorage(STORAGE_KEYS.chartPlacement, chartPlacement, OBJECT_STORAGE);
  syncSignalToStorage(STORAGE_KEYS.sideCollapsed, sideCollapsed, ARRAY_STORAGE);
  syncSignalToStorage(STORAGE_KEYS.rightSplit, rightSplitSizes, ARRAY_STORAGE);

  const setChartType = (v) => setSignal(chartType, v, { normalize: (val) => String(val || "") });
  const setTimelineView = (v) => setSignal(timelineView, v, { normalize: (val) => String(val || "events") });
  const setChartPlacement = (chartId, slot) => {
    chartPlacement.value = { ...chartPlacement.value, [String(chartId)]: slot === "side" ? "side" : "main" };
  };
  const setSideCollapsed = (chartId, collapsed) => {
    const id = String(chartId);
    const rest = sideCollapsed.value.filter((item) => item !== id);
    sideCollapsed.value = collapsed ? [...rest, id] : rest;
  };
  const setRightSplitSizes = (sizes) => {
    if (Array.isArray(sizes) && sizes.length === 2 && sizes.every(Number.isFinite)) rightSplitSizes.value = sizes;
  };

  return {
    chartType,
    timelineView,
    chartPlacement,
    sideCollapsed,
    rightSplitSizes,
    setChartType,
    setTimelineView,
    setChartPlacement,
    setSideCollapsed,
    setRightSplitSizes,
  };
};
