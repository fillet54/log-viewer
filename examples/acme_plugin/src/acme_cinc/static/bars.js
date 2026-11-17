// A tiny horizontal bar list; imported by owners.js to show a plugin can span several files.
export const renderBars = (container, rows, { unit = "" } = {}) => {
  const max = Math.max(1, ...rows.map((row) => row.value));
  container.innerHTML = "";
  rows.forEach(({ label, value }) => {
    const row = document.createElement("div");
    row.className = "acme-bar-row";
    const name = document.createElement("span");
    name.className = "acme-bar-label";
    name.textContent = label;
    const track = document.createElement("span");
    track.className = "acme-bar-track";
    const fill = document.createElement("span");
    fill.className = "acme-bar-fill";
    fill.style.width = `${(100 * value) / max}%`;
    track.appendChild(fill);
    const count = document.createElement("span");
    count.className = "acme-bar-value";
    count.textContent = `${value}${unit}`;
    row.append(name, track, count);
    container.appendChild(row);
  });
};
