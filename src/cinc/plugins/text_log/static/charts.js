import { TONE_COLORS } from "cinc";

const LEVEL_COLORS = { DEBUG: "var(--faint)", INFO: TONE_COLORS.green, WARN: TONE_COLORS.yellow, ERROR: TONE_COLORS.red };

export default function setup({ config, registerTimelineView }) {
  const levels = Array.isArray(config.levels) && config.levels.length ? config.levels : Object.keys(LEVEL_COLORS);
  registerTimelineView({
    id: "levels",
    label: "Levels",
    kind: "histogram",
    stacked: true,
    datasets: levels.map((level) => ({
      label: level,
      color: LEVEL_COLORS[level] || "var(--series-2)",
      filter: (event) => event.level === level,
    })),
  });
}
