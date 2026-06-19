import { h, render } from "preact";

import { STORAGE_KEYS } from "./state/storage.js";
import { queryById, smoothScrollTo } from "./shared.js";
import { AppServicesContext } from "./context.js";
import { ViewerRoot } from "./logview/ViewerRoot.js";
import { createRootServices } from "./services/app-services.js";

const loadPageData = () => window.EVENTLOG2_PAGE_DATA || null;

const renderStartupError = (host, message) => {
  if (!host) return;
  render(h("div", { class: "empty-panel-message" }, String(message || "Unable to load event log viewer.")), host);
};

const loadPageStyles = (pageData) => {
  const styles = Array.isArray(pageData?.view?.styles) ? pageData.view.styles : [];
  if (!styles.length) return;
  const el = document.createElement("style");
  el.dataset.role = "plugin-styles";
  el.textContent = styles.join("\n");
  document.head.appendChild(el);
};

const loadPageScripts = async (pageData) => {
  const scripts = Array.isArray(pageData?.view?.scripts) ? pageData.view.scripts : [];
  const urls = [];
  await Promise.all(
    scripts.map((source) => {
      if (typeof source !== "string" || !source.trim()) return Promise.resolve();
      const blob = new Blob([source], { type: "text/javascript" });
      const url = URL.createObjectURL(blob);
      urls.push(url);
      return import(url);
    })
  );
  urls.forEach((url) => URL.revokeObjectURL(url));
};

export const bootstrapViewer = async ({ host = document.getElementById("log-viewer-root"), pageData = loadPageData() } = {}) => {
  if (!host || !pageData) return false;

  try {
    loadPageStyles(pageData);
    await loadPageScripts(pageData);
    const services = createRootServices({ pageData });
    host._services = services;
    render(
      h(AppServicesContext.Provider, { value: services }, h(ViewerRoot, {})),
      host
    );
    host.dispatchEvent(new CustomEvent("logapp:ready", { bubbles: true, composed: true }));
    return true;
  } catch (error) {
    console.error(error);
    renderStartupError(host, error?.message || "Unable to load event log viewer.");
    return false;
  }
};

bootstrapViewer();

export { STORAGE_KEYS, queryById, smoothScrollTo };
