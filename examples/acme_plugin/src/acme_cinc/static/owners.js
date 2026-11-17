// "Faults by Owner": open faults per owning team at the selected row. Rows carry `owner` from the
// plugin's Python augmenter; `config.owners` (the team list) comes from ctx.extend(..., config=...).
// On served pages it also lists each team's contacts from the plugin's own endpoint
// (`/acme/api/logs/<id>/owners`, in pages.py); a standalone page has no server, so it skips that.
import { signals } from "cinc";
import { renderBars } from "./bars.js";
import { element } from "./model.js";

const loadContacts = async (server, list) => {
  try {
    const response = await fetch(server.url(`/acme/api/logs/${encodeURIComponent(server.logId)}/owners`));
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const { owners } = await response.json();
    list.replaceChildren(
      element("h4", "acme-contacts-heading", "Contacts"),
      ...Object.entries(owners).map(([team, info]) => {
        const row = element("div", "acme-contact");
        row.append(
          element("span", "acme-contact-team", team),
          element("span", "acme-contact-who", `${info.lead} · ${info.channel}`),
          element("span", "acme-contact-end", info.setAtEnd ? `${info.setAtEnd} set at end` : "")
        );
        return row;
      })
    );
  } catch (error) {
    list.replaceChildren(element("p", "acme-empty", `Contacts unavailable (${error.message}).`));
  }
};

export default function setup({ logTypeId, config, server, registerChart }) {
  const owners = Array.isArray(config.owners) ? config.owners : [];

  registerChart({
    id: "acme-owners",
    label: "Faults by Owner",
    slots: ["main", "side"],
    renderPanel(panel, context) {
      const events = (context.logData?.events || []).filter((event) => event.log_type === logTypeId);
      panel.innerHTML = '<div class="acme-bars"></div>';
      const bars = panel.querySelector(".acme-bars");
      if (server) {
        const contacts = element("div", "acme-contacts");
        panel.appendChild(contacts);
        loadContacts(server, contacts);
      }
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
