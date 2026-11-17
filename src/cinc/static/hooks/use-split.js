import { useLayoutEffect, useRef } from "preact/hooks";

const destroySplit = (instanceRef) => {
  if (!instanceRef?.current || typeof instanceRef.current.destroy !== "function") return;
  instanceRef.current.destroy();
  instanceRef.current = null;
};

export const useSplit = ({ refs = [], options = {}, enabled = true, dependencies = [] } = {}) => {
  const instanceRef = useRef(null);

  useLayoutEffect(() => {
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
