// Theme helpers shared by charts (core and plugins): canvas can't read CSS variables, so colours written as
// "var(--token)" are resolved through a probe element into rgba(). Cached per colour scheme; onThemeChange
// clears the cache and tells a chart to redraw with the new values.
const colorCache = new Map();
let colorProbe = null;

const toRgba = (computed) => {
  const srgb = computed.match(/^color\(srgb ([\d.e-]+) ([\d.e-]+) ([\d.e-]+)(?: \/ ([\d.e-]+))?\)$/);
  if (!srgb) return computed;
  const [r, g, b] = srgb.slice(1, 4).map((v) => Math.round(Number(v) * 255));
  return `rgba(${r}, ${g}, ${b}, ${srgb[4] ?? 1})`;
};

export const themeColor = (value, fallback = "rgba(128, 128, 128, 0.6)") => {
  if (typeof value !== "string" || !value.includes("var(")) return value || fallback;
  if (colorCache.has(value)) return colorCache.get(value);
  if (!colorProbe) {
    colorProbe = document.createElement("span");
    colorProbe.style.display = "none";
    document.body.appendChild(colorProbe);
  }
  colorProbe.style.color = "";
  colorProbe.style.color = value;
  const resolved = toRgba(getComputedStyle(colorProbe).color) || fallback;
  colorCache.set(value, resolved);
  return resolved;
};

export const themeFont = () => getComputedStyle(document.documentElement).getPropertyValue("--font-body").trim();

// Calls `callback` when the light/dark theme changes; returns an unsubscribe function.
export const onThemeChange = (callback) => {
  const run = () => {
    colorCache.clear();
    callback();
  };
  const observer = new MutationObserver(run);
  observer.observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });
  const media = window.matchMedia("(prefers-color-scheme: dark)");
  media.addEventListener("change", run);
  return () => {
    observer.disconnect();
    media.removeEventListener("change", run);
  };
};

// The viewer's severity tones (core_event severities and text_log levels map onto these).
export const TONE_COLORS = {
  green: "var(--event-green)",
  yellow: "var(--event-yellow)",
  red: "var(--event-red)",
  "flashing-red": "var(--event-dark-red)",
};
