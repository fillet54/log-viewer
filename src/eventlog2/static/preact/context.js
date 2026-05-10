(function () {
  const ui = window.EventLog2UI || {};
  const createContext = ui.createContext;
  const createElement = ui.createElement;

  ui.context = ui.context || {};

  if (!ui.context.AppServicesContext) {
    ui.context.AppServicesContext = typeof createContext === "function" ? createContext(null) : null;
  }

  ui.AppServicesProvider = function AppServicesProvider(props) {
    const services = props?.services ?? null;
    const children = props?.children ?? null;
    const Provider = ui.context.AppServicesContext?.Provider || null;
    if (!Provider || typeof createElement !== "function") {
      throw new Error("EventLog2UI.AppServicesProvider requires a working local Preact runtime.");
    }
    return createElement(Provider, { value: services }, children);
  };
})();
