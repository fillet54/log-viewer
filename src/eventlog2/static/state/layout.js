import { signal } from "preact/signals";
import { readStorage, syncSignalToStorage, setSignal, BOOLEAN_STORAGE, STRING_STORAGE, ARRAY_STORAGE, STORAGE_KEYS } from "./storage.js";

const normalizeSizePair = (value, fallback) => {
  if (!Array.isArray(value) || value.length !== fallback.length) return fallback.slice();
  const normalized = value.map(Number);
  if (normalized.some((n) => !Number.isFinite(n) || n <= 0)) return fallback.slice();
  return normalized;
};

const sameArray = (left, right) => {
  if (left === right) return true;
  if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) return false;
  for (let i = 0; i < left.length; i++) if (!Object.is(left[i], right[i])) return false;
  return true;
};

export const createLayoutState = () => {
  const defaultTopSplit = [72, 28];
  const defaultRootSplit = [70, 30];
  const defaultSearchSplit = [28, 72];
  const defaultMainViewSplit = [36, 64];

  const detailCollapsed = signal(readStorage(STORAGE_KEYS.detailCollapsed, false, BOOLEAN_STORAGE));
  const bottomCollapsed = signal(readStorage(STORAGE_KEYS.bottomCollapsed, false, BOOLEAN_STORAGE));
  const topSplitSizes = signal(normalizeSizePair(readStorage(STORAGE_KEYS.top, defaultTopSplit, ARRAY_STORAGE), defaultTopSplit));
  const rootSplitSizes = signal(normalizeSizePair(readStorage(STORAGE_KEYS.root, defaultRootSplit, ARRAY_STORAGE), defaultRootSplit));
  const rootExpandedSizes = signal(normalizeSizePair(readStorage(STORAGE_KEYS.rootExpanded, defaultRootSplit, ARRAY_STORAGE), defaultRootSplit));
  const searchSplitSizes = signal(normalizeSizePair(readStorage(STORAGE_KEYS.search, defaultSearchSplit, ARRAY_STORAGE), defaultSearchSplit));
  const mainViewMode = signal(readStorage(STORAGE_KEYS.mainViewMode, "split", STRING_STORAGE) || "split");
  const mainViewSplitSizes = signal(normalizeSizePair(readStorage(STORAGE_KEYS.mainViewSplit, defaultMainViewSplit, ARRAY_STORAGE), defaultMainViewSplit));

  syncSignalToStorage(STORAGE_KEYS.detailCollapsed, detailCollapsed, BOOLEAN_STORAGE);
  syncSignalToStorage(STORAGE_KEYS.bottomCollapsed, bottomCollapsed, BOOLEAN_STORAGE);
  syncSignalToStorage(STORAGE_KEYS.top, topSplitSizes, ARRAY_STORAGE);
  syncSignalToStorage(STORAGE_KEYS.root, rootSplitSizes, ARRAY_STORAGE);
  syncSignalToStorage(STORAGE_KEYS.rootExpanded, rootExpandedSizes, ARRAY_STORAGE);
  syncSignalToStorage(STORAGE_KEYS.search, searchSplitSizes, ARRAY_STORAGE);
  syncSignalToStorage(STORAGE_KEYS.mainViewMode, mainViewMode, STRING_STORAGE);
  syncSignalToStorage(STORAGE_KEYS.mainViewSplit, mainViewSplitSizes, ARRAY_STORAGE);

  const setDetailCollapsed = (v) => setSignal(detailCollapsed, v, { normalize: Boolean });
  const setBottomCollapsed = (v) => setSignal(bottomCollapsed, v, { normalize: Boolean });
  const setTopSplitSizes = (v) => setSignal(topSplitSizes, v, { normalize: (val) => normalizeSizePair(val, defaultTopSplit), equals: sameArray });
  const setRootSplitSizes = (v) => setSignal(rootSplitSizes, v, { normalize: (val) => normalizeSizePair(val, defaultRootSplit), equals: sameArray });
  const setRootExpandedSizes = (v) => setSignal(rootExpandedSizes, v, { normalize: (val) => normalizeSizePair(val, defaultRootSplit), equals: sameArray });
  const setSearchSplitSizes = (v) => setSignal(searchSplitSizes, v, { normalize: (val) => normalizeSizePair(val, defaultSearchSplit), equals: sameArray });
  const setMainViewMode = (v) => setSignal(mainViewMode, v, { normalize: (val) => String(val || "split") });
  const setMainViewSplitSizes = (v) => setSignal(mainViewSplitSizes, v, { normalize: (val) => normalizeSizePair(val, defaultMainViewSplit), equals: sameArray });

  return {
    detailCollapsed, bottomCollapsed, topSplitSizes, rootSplitSizes, rootExpandedSizes,
    searchSplitSizes, mainViewMode, mainViewSplitSizes,
    setDetailCollapsed, setBottomCollapsed, setTopSplitSizes, setRootSplitSizes,
    setRootExpandedSizes, setSearchSplitSizes, setMainViewMode, setMainViewSplitSizes,
  };
};
