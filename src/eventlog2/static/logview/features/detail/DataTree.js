import { html } from "logview/lib";

export const buildDataTree = (value, key = "root", depth = 0, path = "root") => {
  const isArray = Array.isArray(value);
  const isObject = value && typeof value === "object" && !isArray;
  if (!isArray && !isObject) {
    return {
      key,
      depth,
      path,
      kind: "value",
      value: value == null ? "null" : String(value),
    };
  }

  const entries = isArray
    ? value.map((item, index) => [String(index), item])
    : Object.entries(value);
  return {
    key,
    depth,
    path,
    kind: isArray ? "array" : "object",
    count: entries.length,
    children: entries.map(([childKey, childValue]) =>
      buildDataTree(childValue, childKey, depth + 1, `${path}.${childKey}`)
    ),
  };
};

export const collectExpandablePaths = (node, paths = []) => {
  if (!node || node.kind === "value") return paths;
  paths.push(node.path);
  (node.children || []).forEach((child) => collectExpandablePaths(child, paths));
  return paths;
};

export const DataTreeNode = ({ node, collapsedPaths, onToggle }) => {
  if (!node) return null;

  if (node.kind === "value") {
    return html`
      <div class="data-tree-row data-tree-leaf" style=${{ "--tree-depth": node.depth }}>
        <div class="data-tree-key">${node.key}</div>
        <div class="data-tree-value">${node.value}</div>
      </div>
    `;
  }

  const collapsed = collapsedPaths.has(node.path);
  return html`
    <div class=${`data-tree-node${collapsed ? " is-collapsed" : ""}`} data-tree-kind=${node.kind}>
      <button
        type="button"
        class="data-tree-row data-tree-toggle"
        data-tree-toggle="true"
        aria-expanded=${String(!collapsed)}
        style=${{ "--tree-depth": node.depth }}
        onClick=${() => onToggle(node.path)}
      >
        <div class="data-tree-key">
          <span class="data-tree-chevron" aria-hidden="true"></span>
          <span>${node.key}</span>
        </div>
        <div class="data-tree-value"></div>
      </button>
      <div class="data-tree-children">
        ${(node.children || []).map(
          (child) => html`<${DataTreeNode} node=${child} collapsedPaths=${collapsedPaths} onToggle=${onToggle} />`
        )}
      </div>
    </div>
  `;
};

window.EventLog2UI = window.EventLog2UI || {};
window.EventLog2UI.components = window.EventLog2UI.components || {};
window.EventLog2UI.utils = window.EventLog2UI.utils || {};
window.EventLog2UI.components.DataTreeNode = DataTreeNode;
window.EventLog2UI.utils.buildDataTree = buildDataTree;
window.EventLog2UI.utils.collectExpandablePaths = collectExpandablePaths;
