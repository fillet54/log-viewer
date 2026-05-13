const ui = window.EventLog2UI || {};
const useEffect = ui.hooks?.useEffect || null;
const useLayoutEffect = ui.hooks?.useLayoutEffect || null;
const useRef = ui.hooks?.useRef || null;
const useState = ui.hooks?.useState || null;

const requireHook = (hookName, hookValue) => {
  if (typeof hookValue === "function") return hookValue;
  throw new Error(`EventLog2UI.${hookName} requires a working local Preact runtime.`);
};

const buildRange = ({ scrollTop, clientHeight, itemCount, rowHeight, overscan, maxVisible }) => {
    const safeRowHeight = Math.max(1, Number(rowHeight) || 1);
    const safeOverscan = Math.max(0, Number(overscan) || 0);
    const safeMaxVisible = Math.max(1, Number(maxVisible) || itemCount || 1);
    const startIndex = Math.max(0, Math.floor(scrollTop / safeRowHeight) - safeOverscan);
    const visibleCount = Math.min(
      safeMaxVisible,
      Math.ceil(clientHeight / safeRowHeight) + safeOverscan * 2
    );
    const endIndex = Math.min(itemCount, startIndex + visibleCount);

    return {
      startIndex,
      endIndex,
      offsetY: startIndex * safeRowHeight,
      totalHeight: itemCount * safeRowHeight,
    };
};

const sameRange = (left, right) => {
    return (
      left.startIndex === right.startIndex &&
      left.endIndex === right.endIndex &&
      left.offsetY === right.offsetY &&
      left.totalHeight === right.totalHeight
    );
};

export const useVirtualList = ({
    containerRef,
    itemCount = 0,
    rowHeight = 28,
    overscan = 4,
    maxVisible = 80,
    dependencies = [],
} = {}) => {
    const useStateHook = requireHook("appHooks.useVirtualList", useState);
    const useEffectHook = requireHook("appHooks.useVirtualList", useEffect);
    const useLayoutEffectHook = requireHook("appHooks.useVirtualList", useLayoutEffect);
    const useRefHook = requireHook("appHooks.useVirtualList", useRef);

    const frameRef = useRefHook(0);
    const resizeObserverRef = useRefHook(null);
    const [range, setRange] = useStateHook(() =>
      buildRange({
        scrollTop: 0,
        clientHeight: 0,
        itemCount,
        rowHeight,
        overscan,
        maxVisible,
      })
    );

    const recompute = () => {
      const container = containerRef?.current || null;
      const nextRange = buildRange({
        scrollTop: container?.scrollTop || 0,
        clientHeight: container?.clientHeight || 0,
        itemCount,
        rowHeight,
        overscan,
        maxVisible,
      });
      setRange((current) => (sameRange(current, nextRange) ? current : nextRange));
    };

    const schedule = () => {
      if (frameRef.current) return;
      frameRef.current = requestAnimationFrame(() => {
        frameRef.current = 0;
        recompute();
      });
    };

    useLayoutEffectHook(() => {
      recompute();
    }, [itemCount, rowHeight, overscan, maxVisible, ...dependencies]);

    useEffectHook(() => {
      const container = containerRef?.current || null;
      if (!container) return undefined;

      const onScroll = () => schedule();
      container.addEventListener("scroll", onScroll, { passive: true });

      if (typeof ResizeObserver === "function") {
        resizeObserverRef.current = new ResizeObserver(() => schedule());
        resizeObserverRef.current.observe(container);
      }

      schedule();

      return () => {
        container.removeEventListener("scroll", onScroll);
        if (resizeObserverRef.current) {
          resizeObserverRef.current.disconnect();
          resizeObserverRef.current = null;
        }
        if (frameRef.current) {
          cancelAnimationFrame(frameRef.current);
          frameRef.current = 0;
        }
      };
    }, [containerRef, itemCount, rowHeight, overscan, maxVisible, ...dependencies]);

    return {
      ...range,
      visibleCount: Math.max(0, range.endIndex - range.startIndex),
      invalidate: schedule,
      scrollToIndex(index, align = "center") {
        const container = containerRef?.current || null;
        if (!container || index == null) return;

        const safeRowHeight = Math.max(1, Number(rowHeight) || 1);
        const targetTop = Math.max(0, Number(index) || 0) * safeRowHeight;
        let nextTop = targetTop;

        if (align === "center") {
          nextTop = targetTop - container.clientHeight / 2 + safeRowHeight / 2;
        } else if (align === "end") {
          nextTop = targetTop - container.clientHeight + safeRowHeight;
        }

        container.scrollTop = Math.max(0, nextTop);
        schedule();
      },
    };
};

ui.appHooks = ui.appHooks || {};
ui.appHooks.useVirtualList = useVirtualList;
