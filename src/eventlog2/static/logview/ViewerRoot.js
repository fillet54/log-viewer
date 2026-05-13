import { html } from "logview/lib";
import { LayoutShell } from "./features/layout/LayoutShell.js";

export const ViewerRoot = () => html`<${LayoutShell} />`;

window.EventLog2UI = window.EventLog2UI || {};
window.EventLog2UI.components = window.EventLog2UI.components || {};
window.EventLog2UI.components.LogViewerRoot = ViewerRoot;
