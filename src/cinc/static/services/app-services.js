import { createViewerStore } from "../state/viewer-store.js";

export const isStandalone = () => document.body.classList.contains("app-body-standalone");

export const createRootServices = ({ pageData }) => {
  const logType =
    pageData?.logType && typeof pageData.logType === "object"
      ? pageData.logType
      : null;
  const pluginValue = pageData && typeof pageData === "object" ? pageData.plugin : null;
  const plugin =
    pluginValue && typeof pluginValue === "object"
      ? pluginValue
      : typeof pluginValue === "string"
        ? { id: pluginValue, name: pluginValue }
        : null;
  const logData = (() => {
    if (!pageData || typeof pageData !== "object") return null;
    if (pageData.logData && typeof pageData.logData === "object") return pageData.logData;
    if (pageData.payload && typeof pageData.payload === "object") return pageData.payload;
    return null;
  })();
  const view =
    pageData && typeof pageData === "object" && pageData.view && typeof pageData.view === "object"
      ? pageData.view
      : {};
  const standalone = isStandalone();
  const viewerStore = createViewerStore({ logData, standalone });

  // Make logData.events a reactive getter so components that read it
  // during render automatically re-render when events are appended.
  const reactiveLogData = logData
    ? Object.defineProperty({ ...logData }, "events", {
        get: () => viewerStore.allEventsSignal.value,
        enumerable: true,
      })
    : null;

  return {
    logType,
    plugin,
    view,
    logData: reactiveLogData,
    viewerStore,
  };
};
