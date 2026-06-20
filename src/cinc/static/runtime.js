const rowComponents = new Map();
const pendingViewRegistrations = [];
const idVariants = (id) => {
  const text = String(id || "").trim();
  if (!text) return [];
  return Array.from(new Set([text, text.replace(/_/g, "-"), text.replace(/-/g, "_")]));
};

export const EventLog2 = {
  _rowComponents: rowComponents,
  _pendingViewRegistrations: pendingViewRegistrations,
  registerLogRowComponent(logTypeId, Component) {
    const normalizedPluginId = String(logTypeId || "").trim();
    if (!normalizedPluginId) throw new Error("Log row components must define a log type id.");
    if (typeof Component !== "function") throw new Error("Plugin row components must be functions.");
    idVariants(normalizedPluginId).forEach((id) => rowComponents.set(id, Component));
    return Component;
  },
  registerPluginRowComponent(pluginId, Component) {
    return EventLog2.registerLogRowComponent(pluginId, Component);
  },
  resolveRowComponent(plugin) {
    const pluginId =
      plugin && typeof plugin === "object"
        ? String(plugin.id || "").trim()
        : String(plugin || "").trim();
    for (const id of idVariants(pluginId)) {
      if (rowComponents.has(id)) return rowComponents.get(id);
    }
    return null;
  },
  registerLogChartType(logTypeId, definition) {
    const normalizedPluginId = String(logTypeId || "").trim();
    if (!normalizedPluginId) throw new Error("Log chart types must define a log type id.");
    const chartRegistry = window.LogMainViewChart || null;
    if (chartRegistry && typeof chartRegistry.registerPluginType === "function") {
      return chartRegistry.registerPluginType(normalizedPluginId, definition);
    }
    pendingViewRegistrations.push({ kind: "chart", pluginId: normalizedPluginId, definition });
    return definition;
  },
  registerPluginChartType(pluginId, definition) {
    return EventLog2.registerLogChartType(pluginId, definition);
  },
  registerLogTimelineView(logTypeId, definition) {
    const normalizedPluginId = String(logTypeId || "").trim();
    if (!normalizedPluginId) throw new Error("Log timeline views must define a log type id.");
    const timelineRegistry = window.LogMainViewTimeline || null;
    if (timelineRegistry && typeof timelineRegistry.registerPluginView === "function") {
      return timelineRegistry.registerPluginView(normalizedPluginId, definition);
    }
    pendingViewRegistrations.push({ kind: "timeline", pluginId: normalizedPluginId, definition });
    return definition;
  },
  registerPluginTimelineView(pluginId, definition) {
    return EventLog2.registerLogTimelineView(pluginId, definition);
  },
};
