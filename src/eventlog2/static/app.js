// app.js - Main entry point
import "./state/viewer-store-shared.js";
import "./state/viewer-store-navigation.js";
import "./state/viewer-store-layout.js";
import "./state/viewer-store-search.js";
import "./state/viewer-store-chart.js";
import "./state/viewer-store-activity.js";
import "./state/viewer-store.js";

import { h, render } from "preact";

import { STORAGE_KEYS, queryById, smoothScrollTo } from "./shared.js";
import { AppServicesContext } from "./context.js";
import { ViewerRoot } from "./logview/ViewerRoot.js";
import { createRootServices } from "./services/app-services.js";

const loadPageData = () => window.EVENTLOG2_PAGE_DATA || null;

const renderStartupError = (host, message) => {
  if (!host) return;
  render(h("div", { class: "empty-panel-message" }, String(message || "Unable to load event log viewer.")), host);
};

const loadPageScripts = (host, pageData) => {
  if (!host) return;
  host.querySelectorAll('script[data-role="plugin-view-script"]').forEach((node) => node.remove());
  const scripts = Array.isArray(pageData?.view?.scripts) ? pageData.view.scripts : [];
  scripts.forEach((source) => {
    if (typeof source !== "string" || !source.trim()) return;
    const script = document.createElement("script");
    script.dataset.role = "plugin-view-script";
    script.textContent = source;
    host.appendChild(script);
  });
};

export const bootstrapViewer = ({ host = document.getElementById("log-viewer-root"), pageData = loadPageData() } = {}) => {
  if (!host || !pageData) return false;

  try {
    loadPageScripts(host, pageData);
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
