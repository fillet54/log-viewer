export const queryById = (root, id) => {
  if (!root || !id) return null;
  if (typeof root.getElementById === "function") return root.getElementById(id);
  if (typeof root.querySelector === "function") return root.querySelector(`#${id}`);
  return null;
};

export const smoothScrollTo = (container, targetTop, durationMs = 200, onComplete = null) => {
  const startTop = container.scrollTop;
  const delta = targetTop - startTop;
  if (Math.abs(delta) < 2) {
    container.scrollTop = targetTop;
    if (onComplete) onComplete();
    return;
  }
  const startTime = performance.now();
  const easeOut = (t) => 1 - Math.pow(1 - t, 3);
  const step = (now) => {
    const elapsed = now - startTime;
    const progress = Math.min(1, elapsed / durationMs);
    container.scrollTop = startTop + delta * easeOut(progress);
    if (progress < 1) requestAnimationFrame(step);
    else if (onComplete) onComplete();
  };
  requestAnimationFrame(step);
};
