import { Cinc } from "static/runtime.js";
Cinc.registerTimelineView("text_log", { id: "levels", label: "Levels", kind: "histogram", stacked: true, datasets: [["DEBUG", "var(--faint)"], ["INFO", "var(--event-green)"], ["WARN", "var(--event-yellow)"], ["ERROR", "var(--event-red)"]].map(([level, color]) => ({ label: level, color, filter: (event) => event.level === level })) });
