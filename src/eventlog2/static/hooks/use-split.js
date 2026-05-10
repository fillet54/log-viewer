(function () {
  const ui = window.EventLog2UI || {};
  const useLayoutEffect = ui.hooks?.useLayoutEffect || null;
  const useRef = ui.hooks?.useRef || null;

  ui.appHooks = ui.appHooks || {};

  const requireHook = (hookName, hookValue) => {
    if (typeof hookValue === "function") return hookValue;
    throw new Error(`EventLog2UI.${hookName} requires a working local Preact runtime.`);
  };

  const destroySplit = (instanceRef) => {
    if (!instanceRef?.current || typeof instanceRef.current.destroy !== "function") return;
    instanceRef.current.destroy();
    instanceRef.current = null;
  };

  const useSplit = ({ refs = [], options = {}, enabled = true, dependencies = [] } = {}) => {
    const useLayoutEffectHook = requireHook("appHooks.useSplit", useLayoutEffect);
    const useRefHook = requireHook("appHooks.useSplit", useRef);
    const instanceRef = useRefHook(null);

    useLayoutEffectHook(() => {
      if (!enabled || typeof window.Split !== "function") {
        destroySplit(instanceRef);
        return undefined;
      }

      const elements = refs.map((ref) => ref?.current).filter(Boolean);
      if (elements.length !== refs.length || elements.length < 2) {
        return undefined;
      }

      destroySplit(instanceRef);
      instanceRef.current = window.Split(elements, options);

      return () => destroySplit(instanceRef);
    }, [enabled, ...dependencies]);

    return {
      instanceRef,
      setSizes(nextSizes) {
        if (typeof instanceRef.current?.setSizes === "function") {
          instanceRef.current.setSizes(nextSizes);
        }
      },
      destroy() {
        destroySplit(instanceRef);
      },
    };
  };

  ui.appHooks.useSplit = useSplit;
})();
