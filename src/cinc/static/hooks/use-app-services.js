import { AppServicesContext } from "../context.js";
import { useContext } from "preact/hooks";

export const useAppServices = () => {
  if (!AppServicesContext) {
    throw new Error("useAppServices requires AppServicesContext.");
  }
  return useContext(AppServicesContext);
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
export const useAppBookmarks = selectService((services) => services?.viewerStore || null);
export const useAppComments = selectService((services) => services?.viewerStore || null);
export const useAppViewerStore = selectService((services) => services?.viewerStore || null);
