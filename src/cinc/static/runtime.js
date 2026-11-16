// The browser plugin API, imported as "cinc" (see docs/plugins.md).
//
// A plugin script is an ES module whose default export is `setup(ctx)`. The viewer calls it once for each
// log type that uses the script, so the same script serves a type and its subclasses:
//
//   import { html } from "cinc";
//   export default function setup({ logTypeId, config, registerRow, registerChart }) { ... }
//
// Scripts may import other files of their package with relative paths.
import { html as htmHtml } from "htm/preact";
import * as hooksApi from "preact/hooks";
import * as signalsApi from "preact/signals";
import { themeColor, themeFont, onThemeChange, TONE_COLORS } from "./theme.js";

export const API_VERSION = 4;
export const html = htmHtml;
export const hooks = hooksApi;
export const signals = signalsApi;
export { themeColor, themeFont, onThemeChange, TONE_COLORS };

const rowComponents = new Map();
const pendingViewRegistrations = [];
const detailSummaries = new Map();
const idVariants = (id) => {
  const text = String(id || "").trim();
  if (!text) return [];
  return Array.from(new Set([text, text.replace(/_/g, "-"), text.replace(/-/g, "_")]));
};
const requireTypeId = (logTypeId, what) => {
  const id = String(logTypeId || "").trim();
  if (!id) throw new Error(`${what} must name a log type.`);
  return id;
};

// Charts and timeline views register into LogMainChart's registries; before it has loaded they wait here.
const registerView = (kind, logTypeId, definition) => {
  const registry = kind === "chart" ? window.LogMainViewChart : window.LogMainViewTimeline;
  const method = kind === "chart" ? "registerPluginType" : "registerPluginView";
  const globalMethod = kind === "chart" ? "registerType" : "registerView";
  if (registry && typeof registry[method] === "function") {
    if (logTypeId == null) registry[globalMethod](definition);
    else registry[method](logTypeId, definition);
  } else {
    pendingViewRegistrations.push({ kind, pluginId: logTypeId, definition });
  }
  return definition;
};

export const Cinc = {
  apiVersion: API_VERSION,
  _rowComponents: rowComponents,
  _pendingViewRegistrations: pendingViewRegistrations,
  registerLogRowComponent(logTypeId, Component) {
    const id = requireTypeId(logTypeId, "Row components");
    if (typeof Component !== "function") throw new Error("Row components must be functions.");
    idVariants(id).forEach((variant) => rowComponents.set(variant, Component));
    return Component;
  },
  resolveRowComponent(plugin) {
    const pluginId =
      plugin && typeof plugin === "object" ? String(plugin.id || "").trim() : String(plugin || "").trim();
    for (const id of idVariants(pluginId)) {
      if (rowComponents.has(id)) return rowComponents.get(id);
    }
    return null;
  },
  registerDetailSummary(logTypeId, Component) {
    detailSummaries.set(requireTypeId(logTypeId, "Detail summaries"), Component);
    return Component;
  },
  resolveDetailSummary(logTypeId) {
    return detailSummaries.get(String(logTypeId)) || null;
  },
  // A chart offered when a log of this type is open.
  registerChartType(logTypeId, definition) {
    return registerView("chart", requireTypeId(logTypeId, "Log chart types"), definition);
  },
  // A timeline view (histogram or line over time) for this type's events.
  registerTimelineView(logTypeId, definition) {
    return registerView("timeline", requireTypeId(logTypeId, "Log timeline views"), definition);
  },
  // A chart for any log; `definition.available(services)` may hide it.
  registerGlobalChart(definition) {
    return registerView("chart", null, definition);
  },
  registerGlobalTimelineView(definition) {
    return registerView("timeline", null, definition);
  },
};

// The `ctx` passed to a plugin script's setup: registration functions bound to the log type it runs for
// (or global ones when logTypeId is null, for scripts added with ctx.add_script).
// What a served page knows about its server: {logId, live, url(path)}; null on a standalone page.
// `url("/acme/api/...")` adds the app's mount prefix, if any.
export const createServerInfo = (server, live = null) => {
  if (!server || typeof server !== "object" || server.logId == null) return null;
  const root = String(server.root || "").replace(/\/+$/, "");
  return Object.freeze({
    logId: String(server.logId),
    live: Boolean(live),
    url: (path) => `${root}${String(path).startsWith("/") ? "" : "/"}${path}`,
  });
};

export const createSetupContext = ({ logTypeId = null, config = {}, plugin = null, server = null } = {}) => {
  const typed = logTypeId != null;
  return {
    apiVersion: API_VERSION,
    logTypeId,
    config: config || {},
    plugin,
    server,
    registerRow: (Component) => {
      if (!typed) throw new Error("registerRow needs a log type; this script was added globally.");
      return Cinc.registerLogRowComponent(logTypeId, Component);
    },
    registerDetail: (Component) => {
      if (!typed) throw new Error("registerDetail needs a log type; this script was added globally.");
      return Cinc.registerDetailSummary(logTypeId, Component);
    },
    registerChart: (definition) =>
      typed ? Cinc.registerChartType(logTypeId, definition) : Cinc.registerGlobalChart(definition),
    registerTimelineView: (definition) =>
      typed ? Cinc.registerTimelineView(logTypeId, definition) : Cinc.registerGlobalTimelineView(definition),
  };
};

// Import each plugin module and call its setup for every log type that uses it (see app.js).
export const checkApiVersion = (pageData) => {
  const pageVersion = pageData?.apiVersion;
  if (pageVersion === API_VERSION) return true;
  console.error(
    `cinc: this page's data is API version ${pageVersion ?? "(none)"} but the viewer is ` +
      `version ${API_VERSION}; plugins may not work. Rebuild the page or reload the server.`
  );
  return false;
};

export const setupPlugins = async (pageData) => {
  checkApiVersion(pageData);
  const logTypes = pageData?.logTypes || {};
  const server = createServerInfo(pageData?.server, pageData?.live);
  const jobs = [];
  Object.entries(logTypes).forEach(([logTypeId, entry]) => {
    (entry?.modules || []).forEach((moduleId) =>
      jobs.push({ moduleId, context: { logTypeId, config: entry.config } })
    );
    (entry?.extensions || []).forEach((extension) =>
      (extension?.modules || []).forEach((moduleId) =>
        jobs.push({ moduleId, context: { logTypeId, config: extension.config, plugin: extension.plugin } })
      )
    );
  });
  (pageData?.view?.modules || []).forEach((moduleId) => jobs.push({ moduleId, context: {} }));

  const loaded = new Map();
  const load = (moduleId) => {
    if (!loaded.has(moduleId)) loaded.set(moduleId, import(moduleId));
    return loaded.get(moduleId);
  };
  // Load in parallel, then run setups in page order so registration order is stable.
  const modules = await Promise.all(
    jobs.map(({ moduleId }) =>
      load(moduleId).catch((error) => {
        console.error(`Cinc: could not load plugin module ${moduleId}`, error);
        return null;
      })
    )
  );
  for (let i = 0; i < jobs.length; i += 1) {
    const module = modules[i];
    const { moduleId, context } = jobs[i];
    if (!module) continue;
    if (typeof module.default !== "function") {
      console.warn(`Cinc: plugin module ${moduleId} has no default setup(ctx) export.`);
      continue;
    }
    try {
      await module.default(createSetupContext({ ...context, server }));
    } catch (error) {
      console.error(`Cinc: setup failed in ${moduleId}`, error);
    }
  }
};
