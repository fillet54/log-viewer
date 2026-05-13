import { createContext, createElement } from "preact";

export const AppServicesContext = createContext(null);

export const AppServicesProvider = (props) => {
  const services = props?.services ?? null;
  const children = props?.children ?? null;
  return createElement(AppServicesContext.Provider, { value: services }, children);
};

window.EventLog2UI = window.EventLog2UI || {};
window.EventLog2UI.context = window.EventLog2UI.context || {};
window.EventLog2UI.context.AppServicesContext = AppServicesContext;
window.EventLog2UI.AppServicesProvider = AppServicesProvider;
