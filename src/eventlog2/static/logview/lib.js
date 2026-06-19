// ESM Bridge for UMD/Global libraries and shared utilities
import { h, render as preactRender, hydrate as preactHydrate, Fragment as preactFragment, createContext as preactCreateContext } from "preact";
import * as hooksApi from "preact/hooks";
import * as signalsApi from "preact/signals";
import { html as htmHtml } from "htm/preact";

import {
  useAppServices,
  useAppLogData,
  useAppView,
  useAppPlugin,
  useAppBookmarks,
  useAppComments,
  useAppViewerStore,
} from "../hooks/use-app-services.js";
import { useSplit } from "../hooks/use-split.js";
import { useVirtualList } from "../hooks/use-virtual-list.js";

export const html = htmHtml;
export const render = preactRender;
export const hydrate = preactHydrate;
export const Fragment = preactFragment;
export const createElement = h;
export const createContext = preactCreateContext;
export const hooks = hooksApi;
export const signals = signalsApi;
export const appHooks = {
  useAppServices,
  useAppLogData,
  useAppView,
  useAppPlugin,
  useAppBookmarks,
  useAppComments,
  useAppViewerStore,
  useSplit,
  useVirtualList,
};

// Non-Preact libraries
export const Split = window.Split;
export const Chart = window.Chart;
