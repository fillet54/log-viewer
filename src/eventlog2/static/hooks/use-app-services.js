(function () {
  const ui = window.EventLog2UI || {};
  const useContext = ui.hooks?.useContext || null;

  ui.appHooks = ui.appHooks || {};

  const requireHook = (hookName, hookValue) => {
    if (typeof hookValue === "function") return hookValue;
    throw new Error(`EventLog2UI.${hookName} requires a working local Preact runtime.`);
  };

  const useAppServices = () => {
    const context = ui.context?.AppServicesContext || null;
    const hook = requireHook("appHooks.useAppServices", useContext);
    if (!context) {
      throw new Error("EventLog2UI.appHooks.useAppServices requires EventLog2UI.context.AppServicesContext.");
    }
    return hook(context);
  };

  const selectService = (selector) => {
    return () => {
      const services = useAppServices();
      return selector(services);
    };
  };

  ui.appHooks.useAppServices = useAppServices;
  ui.appHooks.useAppLogData = selectService((services) => services?.logData || null);
  ui.appHooks.useAppView = selectService((services) => services?.view || null);
  ui.appHooks.useAppPlugin = selectService((services) => services?.plugin || null);
  ui.appHooks.useAppSearchWorker = selectService((services) => services?.searchWorker || null);
  ui.appHooks.useAppBookmarks = selectService((services) => services?.viewerStore || null);
  ui.appHooks.useAppComments = selectService((services) => services?.viewerStore || null);
  ui.appHooks.useAppViewerStore = selectService((services) => services?.viewerStore || null);
})();
