(function () {
  const ui = window.EventLog2UI || {};

  const buildTree = (Component, props, services) => {
    if (typeof ui.createElement !== "function") {
      throw new Error("EventLog2UI.mountComponent requires a working local Preact runtime.");
    }

    const componentNode = ui.createElement(Component, props || {});
    const Provider = ui.context?.AppServicesContext?.Provider || null;
    if (!Provider) return componentNode;

    return ui.createElement(Provider, { value: services ?? null }, componentNode);
  };

  const mountComponent = ({ host, Component, props = {}, services = null } = {}) => {
    if (!host || typeof Component !== "function") return false;
    if (!ui.available) {
      console.warn("EventLog2UI.mountComponent skipped because the local Preact runtime is not available.");
      return false;
    }
    ui.render(buildTree(Component, props, services), host);
    return true;
  };

  const unmountComponent = (host) => {
    if (!host || !ui.available) return false;
    ui.render(null, host);
    return true;
  };

  const createMountController = ({
    host,
    Component,
    getProps = () => ({}),
    getServices = () => null,
    onError = null,
  } = {}) => {
    return {
      render(extraProps = {}) {
        try {
          return mountComponent({
            host,
            Component,
            props: { ...getProps(), ...extraProps },
            services: getServices(),
          });
        } catch (error) {
          if (typeof onError === "function") {
            onError(error);
            return false;
          }
          throw error;
        }
      },
      destroy() {
        return unmountComponent(host);
      },
    };
  };

  ui.mountComponent = mountComponent;
  ui.unmountComponent = unmountComponent;
  ui.createMountController = createMountController;
})();
