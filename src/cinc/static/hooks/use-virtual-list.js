import { useEffect, useLayoutEffect, useRef, useState } from "preact/hooks";

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

const indexAtOffset = (offsets, value) => {
    let low = 0;
    let high = offsets.length - 2;
    while (low < high) {
      const middle = Math.floor((low + high) / 2);
      if (offsets[middle + 1] <= value) low = middle + 1;
      else high = middle;
    }
    return Math.max(0, low);
};

const buildOffsetRange = ({ scrollTop, clientHeight, itemCount, offsets, overscan }) => {
    const startIndex = Math.max(0, indexAtOffset(offsets, scrollTop) - overscan);
    const endIndex = Math.min(itemCount, indexAtOffset(offsets, scrollTop + clientHeight) + overscan + 1);
    return { startIndex, endIndex, offsetY: offsets[startIndex] || 0, totalHeight: offsets[itemCount] || 0 };
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
    offsets = null,
    dependencies = [],
} = {}) => {
    const frameRef = useRef(0);
    const resizeObserverRef = useRef(null);
    const [range, setRange] = useState(() =>
      (offsets ? buildOffsetRange : buildRange)({
        scrollTop: 0,
        clientHeight: 0,
        itemCount,
        rowHeight,
        overscan,
        maxVisible,
        offsets,
      })
    );

    const recompute = () => {
      const container = containerRef?.current || null;
      const nextRange = (offsets ? buildOffsetRange : buildRange)({
        scrollTop: container?.scrollTop || 0,
        clientHeight: container?.clientHeight || 0,
        itemCount,
        rowHeight,
        overscan,
        maxVisible,
        offsets,
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

    useLayoutEffect(() => {
      recompute();
    }, [itemCount, rowHeight, overscan, maxVisible, offsets, ...dependencies]);

    useEffect(() => {
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
        const targetIndex = Math.max(0, Number(index) || 0);
        const targetTop = offsets ? offsets[targetIndex] || 0 : targetIndex * safeRowHeight;
        const targetHeight = offsets ? (offsets[targetIndex + 1] - offsets[targetIndex]) : safeRowHeight;
        let nextTop = targetTop;

        if (align === "center") {
          nextTop = targetTop - container.clientHeight / 2 + targetHeight / 2;
        } else if (align === "end") {
          nextTop = targetTop - container.clientHeight + targetHeight;
        }

        container.scrollTop = Math.max(0, nextTop);
        schedule();
      },
    };
};
