import { render, createElement } from 'preact';
import { AppServicesContext } from './context.js';

const buildTree = (Component, props, services) => {
  const componentNode = createElement(Component, props || {});

  return createElement(AppServicesContext.Provider, { value: services ?? null }, componentNode);
};

export const mountComponent = ({ host, Component, props = {}, services = null } = {}) => {
  if (!host || typeof Component !== "function") return false;
  render(buildTree(Component, props, services), host);
  return true;
};

export const unmountComponent = (host) => {
  if (!host) return false;
  render(null, host);
  return true;
};

export const createMountController = ({
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

// Maintain global compatibility for now
window.EventLog2UI = window.EventLog2UI || {};
window.EventLog2UI.mountComponent = mountComponent;
window.EventLog2UI.unmountComponent = unmountComponent;
window.EventLog2UI.createMountController = createMountController;
