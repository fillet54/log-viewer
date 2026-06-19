export const isStandalone = () => document.body.classList.contains("app-body-standalone");

export const createRootServices = ({ pageData }) => {
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
  const viewerStore =
    typeof window.EventLog2?.createViewerStore === "function"
      ? window.EventLog2.createViewerStore({ logData, standalone })
      : null;

  return {
    plugin,
    view,
    logData,
    viewerStore,
  };
};
