const rowComponents = new Map();
const pendingViewRegistrations = [];

export const EventLog2 = {
  _rowComponents: rowComponents,
  _pendingViewRegistrations: pendingViewRegistrations,
  registerPluginRowComponent(pluginId, Component) {
    const normalizedPluginId = String(pluginId || "").trim();
    if (!normalizedPluginId) throw new Error("Plugin row components must define a plugin id.");
    if (typeof Component !== "function") throw new Error("Plugin row components must be functions.");
    rowComponents.set(normalizedPluginId, Component);
    return Component;
  },
  resolveRowComponent(plugin) {
    const pluginId =
      plugin && typeof plugin === "object"
        ? String(plugin.id || "").trim()
        : String(plugin || "").trim();
    return rowComponents.get(pluginId) || null;
  },
  registerPluginChartType(pluginId, definition) {
    const normalizedPluginId = String(pluginId || "").trim();
    if (!normalizedPluginId) throw new Error("Plugin chart types must define a plugin id.");
    const chartRegistry = window.LogMainViewChart || null;
    if (chartRegistry && typeof chartRegistry.registerPluginType === "function") {
      return chartRegistry.registerPluginType(normalizedPluginId, definition);
    }
    pendingViewRegistrations.push({ kind: "chart", pluginId: normalizedPluginId, definition });
    return definition;
  },
  registerPluginTimelineView(pluginId, definition) {
    const normalizedPluginId = String(pluginId || "").trim();
    if (!normalizedPluginId) throw new Error("Plugin timeline views must define a plugin id.");
    const timelineRegistry = window.LogMainViewTimeline || null;
    if (timelineRegistry && typeof timelineRegistry.registerPluginView === "function") {
      return timelineRegistry.registerPluginView(normalizedPluginId, definition);
    }
    pendingViewRegistrations.push({ kind: "timeline", pluginId: normalizedPluginId, definition });
    return definition;
  },
};
