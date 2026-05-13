import { AppServicesContext } from "../context.js";

const ui = window.EventLog2UI || {};
const useContext = ui.hooks?.useContext || null;

const requireHook = (hookName, hookValue) => {
  if (typeof hookValue === "function") return hookValue;
  throw new Error(`EventLog2UI.${hookName} requires a working local Preact runtime.`);
};

export const useAppServices = () => {
  const hook = requireHook("appHooks.useAppServices", useContext);
  if (!AppServicesContext) {
    throw new Error("EventLog2UI.appHooks.useAppServices requires EventLog2UI.context.AppServicesContext.");
  }
  return hook(AppServicesContext);
};

const selectService = (selector) => {
  return () => {
    const services = useAppServices();
    return selector(services);
  };
};

export const useAppLogData = selectService((services) => services?.logData || null);
export const useAppView = selectService((services) => services?.view || null);
export const useAppPlugin = selectService((services) => services?.plugin || null);
export const useAppSearchWorker = selectService((services) => services?.searchWorker || null);
export const useAppBookmarks = selectService((services) => services?.viewerStore || null);
export const useAppComments = selectService((services) => services?.viewerStore || null);
export const useAppViewerStore = selectService((services) => services?.viewerStore || null);

ui.appHooks = ui.appHooks || {};
ui.appHooks.useAppServices = useAppServices;
ui.appHooks.useAppLogData = useAppLogData;
ui.appHooks.useAppView = useAppView;
ui.appHooks.useAppPlugin = useAppPlugin;
ui.appHooks.useAppSearchWorker = useAppSearchWorker;
ui.appHooks.useAppBookmarks = useAppBookmarks;
ui.appHooks.useAppComments = useAppComments;
ui.appHooks.useAppViewerStore = useAppViewerStore;
