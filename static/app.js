const STORAGE_KEYS = {
  root: "loglayout.split.root",
  top: "loglayout.split.top",
};

const loadSizes = (key, fallback) => {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return fallback;
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed) || parsed.length !== fallback.length) return fallback;
    if (parsed.some((value) => typeof value !== "number")) return fallback;
    return parsed;
  } catch (err) {
    return fallback;
  }
};

const saveSizes = (key, sizes) => {
  localStorage.setItem(key, JSON.stringify(sizes));
};

const initSplits = () => {
  const rootSizes = loadSizes(STORAGE_KEYS.root, [70, 30]);
  const topSizes = loadSizes(STORAGE_KEYS.top, [22, 56, 22]);

  Split(["#pane-top", "#pane-bottom"], {
    sizes: rootSizes,
    direction: "vertical",
    gutterSize: 10,
    minSize: [240, 160],
    onDragEnd: (sizes) => saveSizes(STORAGE_KEYS.root, sizes),
  });

  Split(["#pane-left", "#pane-center", "#pane-right"], {
    sizes: topSizes,
    direction: "horizontal",
    gutterSize: 10,
    minSize: [180, 320, 200],
    onDragEnd: (sizes) => saveSizes(STORAGE_KEYS.top, sizes),
  });
};

const initChart = () => {
  const canvas = document.getElementById("chart");
  if (!canvas) return;

  const context = canvas.getContext("2d");
  new Chart(context, {
    type: "line",
    data: {
      labels: ["12:00", "12:05", "12:10", "12:15", "12:20", "12:25", "12:30"],
      datasets: [
        {
          label: "Errors",
          data: [2, 3, 1, 4, 2, 5, 3],
          borderColor: "#ef4444",
          backgroundColor: "rgba(239, 68, 68, 0.15)",
          tension: 0.35,
          fill: true,
        },
        {
          label: "Warnings",
          data: [4, 2, 3, 2, 4, 3, 2],
          borderColor: "#f59e0b",
          backgroundColor: "rgba(245, 158, 11, 0.15)",
          tension: 0.35,
          fill: true,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: "bottom" },
      },
      scales: {
        y: { beginAtZero: true },
      },
    },
  });
};

window.addEventListener("DOMContentLoaded", () => {
  initSplits();
  initChart();
});
