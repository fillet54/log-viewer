import { Cinc } from "static/runtime.js";
Cinc.registerTimelineView("text_log", { id: "levels", label: "Levels", kind: "histogram", stacked: true, datasets: ["DEBUG", "INFO", "WARN", "ERROR"].map((level) => ({ label: level, filter: (event) => event.level === level })) });
