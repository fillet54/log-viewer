import { createContext, createElement } from "preact";

export const AppServicesContext = createContext(null);

export const AppServicesProvider = (props) => {
  const services = props?.services ?? null;
  const children = props?.children ?? null;
  return createElement(AppServicesContext.Provider, { value: services }, children);
};
