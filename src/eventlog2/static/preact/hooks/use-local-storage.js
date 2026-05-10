(function () {
  const ui = window.EventLog2UI || {};
  const useEffect = ui.hooks?.useEffect || null;
  const useMemo = ui.hooks?.useMemo || null;
  const useState = ui.hooks?.useState || null;
  const signalEffect = ui.signals?.effect || null;
  const signalFactory = ui.signals?.signal || null;

  ui.appHooks = ui.appHooks || {};

  const requireHook = (hookName, hookValue) => {
    if (typeof hookValue === "function") return hookValue;
    throw new Error(`EventLog2UI.${hookName} requires a working local Preact runtime.`);
  };

  const resolveInitial = (initialValue) => {
    return typeof initialValue === "function" ? initialValue() : initialValue;
  };

  const readStorage = (key, initialValue, options = {}) => {
    const fallback = resolveInitial(initialValue);
    if (!key || typeof localStorage === "undefined") return fallback;

    try {
      const raw = localStorage.getItem(key);
      if (raw == null) return fallback;
      const parse = typeof options.parse === "function" ? options.parse : JSON.parse;
      return parse(raw);
    } catch (error) {
      return fallback;
    }
  };

  const writeStorage = (key, value, options = {}) => {
    if (!key || typeof localStorage === "undefined") return;
    const serialize = typeof options.serialize === "function" ? options.serialize : JSON.stringify;
    localStorage.setItem(key, serialize(value));
  };

  const useLocalStorageState = (key, initialValue, options = {}) => {
    const useStateHook = requireHook("appHooks.useLocalStorageState", useState);
    const useEffectHook = requireHook("appHooks.useLocalStorageState", useEffect);
    const [value, setValue] = useStateHook(() => readStorage(key, initialValue, options));

    useEffectHook(() => {
      writeStorage(key, value, options);
    }, [key, value, options]);

    return [value, setValue];
  };

  const useLocalStorageSignal = (key, initialValue, options = {}) => {
    const useEffectHook = requireHook("appHooks.useLocalStorageSignal", useEffect);
    const useMemoHook = requireHook("appHooks.useLocalStorageSignal", useMemo);
    if (typeof signalFactory !== "function") {
      throw new Error("EventLog2UI.appHooks.useLocalStorageSignal requires local @preact/signals.");
    }

    const storageSignal = useMemoHook(() => signalFactory(readStorage(key, initialValue, options)), [key]);
    useEffectHook(() => {
      if (typeof signalEffect === "function") {
        return signalEffect(() => {
          writeStorage(key, storageSignal.value, options);
        });
      }
      writeStorage(key, storageSignal.value, options);
      return undefined;
    }, [key, options, storageSignal]);
    return storageSignal;
  };

  ui.appHooks.readStorage = readStorage;
  ui.appHooks.writeStorage = writeStorage;
  ui.appHooks.useLocalStorageState = useLocalStorageState;
  ui.appHooks.useLocalStorageSignal = useLocalStorageSignal;
})();
