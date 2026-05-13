// ESM Bridge for UMD/Global libraries and shared utilities
import {
  useAppServices,
  useAppLogData,
  useAppView,
  useAppPlugin,
  useAppSearchWorker,
  useAppBookmarks,
  useAppComments,
  useAppViewerStore,
} from "../hooks/use-app-services.js";
import { useSplit } from "../hooks/use-split.js";
import { useVirtualList } from "../hooks/use-virtual-list.js";

const ui = window.EventLog2UI || {};

export const html = ui.html;
export const render = ui.render;
export const hydrate = ui.hydrate;
export const Fragment = ui.Fragment;
export const createElement = ui.createElement;
export const createContext = ui.createContext;
export const hooks = ui.hooks;
export const signals = ui.signals;
export const appHooks = {
  useAppServices,
  useAppLogData,
  useAppView,
  useAppPlugin,
  useAppSearchWorker,
  useAppBookmarks,
  useAppComments,
  useAppViewerStore,
  useSplit,
  useVirtualList,
};

// Non-Preact libraries
export const Split = window.Split;
export const Chart = window.Chart;
