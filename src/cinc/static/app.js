import { h, render } from "preact";

import { STORAGE_KEYS } from "./state/storage.js";
import { queryById, smoothScrollTo } from "./shared.js";
import { AppServicesContext } from "./context.js";
import { ViewerRoot } from "./logview/ViewerRoot.js";
import { createRootServices } from "./services/app-services.js";

const loadPageData = () => window["CINC_" + "PAGE_DATA"] || null;
const isStandalone = () => document.body.classList.contains("app-body-standalone");

const renderStartupError = (host, message) => {
  if (!host) return;
  render(h("div", { class: "empty-panel-message" }, String(message || "Unable to load event log viewer.")), host);
};

// Live mode: append events posted to the current log, and swap in earlier
// events the server re-derived ("updates", e.g. a SET once its CLEAR arrives).
// The server says
// "reload" when the page needs fresh type config, and "log" when a new log
// starts (the live page follows it).
const connectLiveStream = (services, live) => {
  let seq = live.seq || 0;
  const known = new Set(Object.keys(services.logTypes || {}));
  const open = () => {
    const es = new EventSource(`/api/stream?log=${encodeURIComponent(live.logId)}&after=${seq}`);
    es.onmessage = (e) => {
      let msg;
      try { msg = JSON.parse(e.data); } catch { return; }
      if (msg.type === "log") { es.close(); window.location.href = "/"; return; }
      if (msg.type === "reload") { es.close(); window.location.reload(); return; }
      if (msg.type !== "events" || !Array.isArray(msg.events)) return;
      if (msg.events.some((event) => !known.has(event.log_type))) {
        es.close();
        window.location.reload();
        return;
      }
      seq = msg.seq;
      services.viewerStore.applyLiveEvents(msg.events, msg.updates);
      if (Array.isArray(msg.tracks)) services.viewerStore.applyLiveTracks(msg.tracks);
    };
    es.onerror = () => {
      es.close();
      setTimeout(open, 3000);
    };
  };
  open();
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
    if (!isStandalone() && pageData?.live?.logId) {
      connectLiveStream(services, pageData.live);
    }
    return true;
  } catch (error) {
    console.error(error);
    renderStartupError(host, error?.message || "Unable to load event log viewer.");
    return false;
  }
};

bootstrapViewer();

export { STORAGE_KEYS, queryById, smoothScrollTo };
