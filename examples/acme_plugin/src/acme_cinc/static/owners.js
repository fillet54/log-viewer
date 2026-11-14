// "Faults by Owner": open faults per owning team at the selected row. Rows carry `owner` from the
// plugin's Python augmenter; `config.owners` (the team list) comes from ctx.extend(..., config=...).
import { signals } from "cinc";
import { renderBars } from "./bars.js";

export default function setup({ logTypeId, config, registerChart }) {
  const owners = Array.isArray(config.owners) ? config.owners : [];

  registerChart({
    id: "acme-owners",
    label: "Faults by Owner",
    renderPanel(panel, context) {
      const events = (context.logData?.events || []).filter((event) => event.log_type === logTypeId);
      panel.innerHTML = '<div class="acme-bars"></div>';
      const bars = panel.querySelector(".acme-bars");
      const render = () => {
        const store = context.viewerStore;
        const rowId = store?.selectedEvent?.value?.row_id ?? store?.logScroll?.value?.rowId ?? null;
        const until = events.find((event) => event.row_id === rowId)?.time;
        const open = new Map();
        events.forEach((event) => {
          if (until && event.time > until) return;
          const key = `${event.system}|${event.code}`;
          if (event.set_clear === "clear") open.delete(key);
          else open.set(key, event.owner || "Unassigned");
        });
        const counts = new Map(owners.map((owner) => [owner, 0]));
        open.forEach((owner) => counts.set(owner, (counts.get(owner) || 0) + 1));
        renderBars(bars, Array.from(counts, ([label, value]) => ({ label, value })));
      };
      render();
      const off = context.viewerStore ? signals.effect(render) : null;
      return { activate: render, destroy: () => off?.() };
    },
  });
}
