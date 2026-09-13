import { createViewerStore } from "../state/viewer-store.js";

export const isStandalone = () => document.body.classList.contains("app-body-standalone");

export const createRootServices = ({ pageData }) => {
  const logType = pageData?.logType ?? null;
  const logData = (() => {
    if (!pageData || typeof pageData !== "object") return null;
    if (pageData.logData && typeof pageData.logData === "object") return pageData.logData;
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
    search: pageData?.search || {},
    view,
    logData: reactiveLogData,
    viewerStore,
  };
};
