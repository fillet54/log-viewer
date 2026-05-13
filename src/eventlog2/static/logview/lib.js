// ESM Bridge for UMD/Global libraries and shared utilities
const ui = window.EventLog2UI || {};

export const html = ui.html;
export const render = ui.render;
export const hydrate = ui.hydrate;
export const Fragment = ui.Fragment;
export const createElement = ui.createElement;
export const createContext = ui.createContext;
export const hooks = ui.hooks;
export const signals = ui.signals;
export const appHooks = ui.appHooks;

// Non-Preact libraries
export const Split = window.Split;
export const Chart = window.Chart;

// Constants from the UI registry (populated by components)
export const constants = ui.constants || {};

// Shared utils
export const utils = ui.utils || {};
