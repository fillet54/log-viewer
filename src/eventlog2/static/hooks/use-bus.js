(function () {
  const ui = window.EventLog2UI || {};
  const useEffect = ui.hooks?.useEffect || null;
  const useMemo = ui.hooks?.useMemo || null;
  const useRef = ui.hooks?.useRef || null;
  const useState = ui.hooks?.useState || null;
  const signalFactory = ui.signals?.signal || null;

  ui.appHooks = ui.appHooks || {};

  const requireHook = (hookName, hookValue) => {
    if (typeof hookValue === "function") return hookValue;
    throw new Error(`EventLog2UI.${hookName} requires a working local Preact runtime.`);
  };

  const useBusSubscription = (eventName, handler, dependencies = []) => {
    const bus = ui.appHooks.useAppBus();
    const useEffectHook = requireHook("appHooks.useBusSubscription", useEffect);
    const useRefHook = requireHook("appHooks.useBusSubscription", useRef);
    const handlerRef = useRefHook(handler);

    useEffectHook(() => {
      handlerRef.current = handler;
    }, [handler]);

    useEffectHook(() => {
      if (!bus || !eventName) return undefined;
      return bus.on(eventName, (payload) => {
        if (typeof handlerRef.current === "function") {
          handlerRef.current(payload);
        }
      });
    }, [bus, eventName, ...dependencies]);
  };

  const useBusState = (eventName, initialValue, dependencies = []) => {
    const useStateHook = requireHook("appHooks.useBusState", useState);
    const [value, setValue] = useStateHook(initialValue);
    useBusSubscription(eventName, setValue, dependencies);
    return [value, setValue];
  };

  const useBusSignal = (eventName, initialValue, dependencies = []) => {
    const useMemoHook = requireHook("appHooks.useBusSignal", useMemo);
    if (typeof signalFactory !== "function") {
      throw new Error("EventLog2UI.appHooks.useBusSignal requires local @preact/signals.");
    }
    const payloadSignal = useMemoHook(() => signalFactory(initialValue), []);
    useBusSubscription(
      eventName,
      (payload) => {
        payloadSignal.value = payload;
      },
      dependencies
    );
    return payloadSignal;
  };

  ui.appHooks.useBusSubscription = useBusSubscription;
  ui.appHooks.useBusState = useBusState;
  ui.appHooks.useBusSignal = useBusSignal;
})();
