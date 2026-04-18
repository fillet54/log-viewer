window.LogApp = window.LogApp || {};

/*
 * Hover query menu for log-row fields.
 *
 * Supported data attributes on a field element:
 *
 * - data-query-field
 *   Search field name used on the left-hand side of the generated clause.
 *   Example:
 *     data-query-field="name"          -> name:command_timeout
 *     data-query-field="norm_time"     -> norm_time>100
 *
 * - data-query-value-field
 *   Event-object path used to read the raw value for the generated clause.
 *   This is useful when the rendered text is formatted differently from the
 *   underlying value that should be searched.
 *   Example:
 *     data-field="offset"
 *     data-query-field="norm_time"
 *     data-query-value-field="norm_time"
 *   can render "123s" while still generating queries such as norm_time:123.
 *
 * - data-query-type
 *   Optional explicit value type. "number" enables the numeric operator switch
 *   in the menu with :, >, <, >=, and <=.
 *   Example:
 *     data-query-type="number"
 *
 * - data-query-spec-<Label>
 *   Adds a custom preset query section to the menu. The suffix becomes the
 *   section title after converting "_" / "-" to spaces and title-casing it.
 *   Example:
 *     data-query-spec-Custom="norm_time>100"
 *     data-query-spec-Specific_Command="(name:command AND $.number:2)"
 *
 * Runtime-populated attributes:
 *
 * - data-query-value
 *   Raw value copied from the current event during row render.
 *
 * - data-query-disabled
 *   Set to "true" when the field has no usable query value.
 */
LogApp.initFieldQueryMenu = () => {
  const root = document.body;
  if (!root) return null;

  const button = document.createElement("button");
  button.type = "button";
  button.className = "log-query-plus hidden";
  button.setAttribute("aria-label", "Add field to search");
  button.textContent = "+";

  const menu = document.createElement("div");
  menu.className = "log-query-menu hidden";
  menu.setAttribute("role", "menu");

  root.appendChild(button);
  root.appendChild(menu);

  let activeField = null;
  let hoverTimer = 0;
  let menuOpen = false;
  let activeOperator = ":";
  const actionModes = ["AND", "OR", "NOT", "ONLY"];
  const numericOperators = [":", ">", "<", ">=", "<="];

  const hideButton = () => {
    button.classList.add("hidden");
  };

  const hideMenu = () => {
    menu.classList.add("hidden");
    menuOpen = false;
  };

  const clearActiveField = () => {
    activeField?.classList.remove("is-query-active");
    activeField = null;
    activeOperator = ":";
  };

  const hideAll = () => {
    window.clearTimeout(hoverTimer);
    hideMenu();
    hideButton();
    clearActiveField();
  };

  const isUsableField = (field) =>
    Boolean(
      field &&
        field.dataset.queryField &&
        field.dataset.queryDisabled !== "true" &&
        field.isConnected
    );

  const positionElement = (floatingEl, field, offsetX = 0, offsetY = 0) => {
    const rect = field.getBoundingClientRect();
    floatingEl.style.left = `${Math.max(8, rect.right - floatingEl.offsetWidth + offsetX)}px`;
    floatingEl.style.top = `${Math.max(
      8,
      rect.top + rect.height / 2 - floatingEl.offsetHeight / 2 + offsetY
    )}px`;
  };

  const setActiveField = (field) => {
    if (!isUsableField(field)) {
      hideAll();
      return;
    }
    if (activeField !== field) {
      activeField?.classList.remove("is-query-active");
      activeField = field;
      activeField.classList.add("is-query-active");
    }
    button.classList.remove("hidden");
    positionElement(button, field, -6, 0);
  };

  const normalizeValue = (field) => {
    const raw = field.dataset.queryValue ?? "";
    const type = field.dataset.queryType || "string";
    if (type === "number") {
      const value = Number(raw);
      return Number.isFinite(value) ? String(value) : raw;
    }
    if (/^(true|false)$/i.test(raw)) return raw.toLowerCase();
    if (/^[A-Za-z0-9_.$*-]+$/.test(raw) && !/^(AND|OR|NOT)$/i.test(raw)) return raw;
    return `"${raw.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
  };

  const buildClause = (field, op) => `${field.dataset.queryField}${op}${normalizeValue(field)}`;

  const escapeHtml = (value) =>
    String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");

  const composeQuery = (current, clause, joinMode) => {
    const base = current.trim();
    if (joinMode === "ONLY") return clause;
    if (!base) {
      return joinMode === "NOT" ? `-${clause}` : clause;
    }
    if (joinMode === "OR") return `${base} OR ${clause}`;
    if (joinMode === "NOT") return `${base} -${clause}`;
    return `${base} AND ${clause}`;
  };

  const toTitleCase = (value) =>
    String(value || "")
      .replace(/[_-]+/g, " ")
      .replace(/\s+/g, " ")
      .trim()
      .replace(/\b\w/g, (char) => char.toUpperCase());

  const getCustomSpecs = (field) => {
    if (!field?.getAttributeNames) return [];
    return field
      .getAttributeNames()
      .filter((name) => name.startsWith("data-query-spec-"))
      .map((name) => {
        const rawLabel = name.slice("data-query-spec-".length);
        const query = field.getAttribute(name);
        if (!query) return null;
        return {
          label: toTitleCase(rawLabel),
          query: query.trim(),
        };
      })
      .filter(Boolean);
  };

  const runQueryAction = (joinMode, op) => {
    if (!activeField || !LogApp.searchPane?.setQuery) return;
    const clause = buildClause(activeField, op);
    const nextQuery = composeQuery(LogApp.searchPane.getQuery?.() || "", clause, joinMode);
    LogApp.searchPane.setQuery(nextQuery, { run: true, focus: true });
    hideAll();
  };

  const runCustomQueryAction = (query, joinMode) => {
    if (!query || !LogApp.searchPane?.setQuery) return;
    const clause = query.startsWith("(") ? query : `(${query})`;
    const nextQuery = composeQuery(LogApp.searchPane.getQuery?.() || "", clause, joinMode);
    LogApp.searchPane.setQuery(nextQuery, { run: true, focus: true });
    hideAll();
  };

  const renderActionRows = ({
    clause,
    actionData,
    actionKey,
    joinKey,
    extraAttrs = "",
    className = "log-query-menu-item",
  }) =>
    actionData
      .map((joinMode) => {
        const attrs = [`class="${className}"`, `data-${joinKey}="${joinMode}"`];
        if (actionKey != null) attrs.push(`data-${actionKey}="${escapeHtml(clause)}"`);
        if (extraAttrs) attrs.push(extraAttrs);
        return `
          <button
            type="button"
            ${attrs.join("\n            ")}
            title="${escapeHtml(clause)}"
          >
            <span class="log-query-menu-op">${escapeHtml(joinMode)}</span>
            <span class="log-query-menu-preview">${escapeHtml(clause)}</span>
          </button>
        `;
      })
      .join("");

  const renderMenu = (field) => {
    const isNumeric = field.dataset.queryType === "number";
    const display = field.dataset.queryField || field.dataset.field || "";
    const customSpecs = getCustomSpecs(field);
    const currentOperator =
      isNumeric && numericOperators.includes(activeOperator) ? activeOperator : ":";

    menu.innerHTML = `
      <div class="log-query-menu-title">${escapeHtml(display)}</div>
      ${
        isNumeric
          ? `
            <div class="log-query-operator-switch" role="group" aria-label="Numeric operator">
              ${numericOperators
                .map(
                  (op) => `
                    <button
                      type="button"
                      class="log-query-operator-button ${currentOperator === op ? "is-active" : ""}"
                      data-operator="${op}"
                      aria-pressed="${currentOperator === op ? "true" : "false"}"
                    >
                      ${escapeHtml(op)}
                    </button>
                  `
                )
                .join("")}
            </div>
          `
          : ""
      }
      <div class="log-query-menu-list">
        ${renderActionRows({
          clause: buildClause(field, currentOperator),
          actionData: actionModes,
          joinKey: "join",
          extraAttrs: `data-op="${escapeHtml(currentOperator)}"`,
        })}
        ${
          customSpecs.length
            ? `
              <div class="log-query-menu-divider"></div>
              ${customSpecs
                .map(
                  (spec) => `
                    <div class="log-query-menu-section">
                      <div class="log-query-menu-title log-query-menu-subtitle">${escapeHtml(spec.label)}</div>
                      ${renderActionRows({
                        clause: spec.query.startsWith("(") ? spec.query : `(${spec.query})`,
                        actionData: actionModes,
                        actionKey: "custom-query",
                        joinKey: "custom-join",
                      })}
                    </div>
                  `
                )
                .join("")}
            `
            : ""
        }
      </div>
    `;

    menu.querySelectorAll(".log-query-operator-button").forEach((item) => {
      item.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        activeOperator = item.dataset.operator || ":";
        if (activeField) renderMenu(activeField);
      });
    });

    menu.querySelectorAll(".log-query-menu-item").forEach((item) => {
      item.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        if (item.dataset.customQuery) {
          runCustomQueryAction(item.dataset.customQuery, item.dataset.customJoin || "AND");
          return;
        }
        runQueryAction(item.dataset.join, item.dataset.op);
      });
    });
  };

  button.addEventListener("click", (event) => {
    event.preventDefault();
    event.stopPropagation();
    if (!activeField) return;
    renderMenu(activeField);
    menu.classList.remove("hidden");
    menuOpen = true;
    positionElement(menu, activeField, 0, 28);
  });

  document.addEventListener("mouseover", (event) => {
    const field = event.target.closest("[data-query-field]");
    if (!field || !isUsableField(field)) {
      if (menuOpen) return;
      const movingIntoOverlay =
        event.target.closest(".log-query-plus") || event.target.closest(".log-query-menu");
      if (!movingIntoOverlay) {
        hoverTimer = window.setTimeout(() => {
          if (!menuOpen) hideAll();
        }, 90);
      }
      return;
    }
    window.clearTimeout(hoverTimer);
    setActiveField(field);
  });

  document.addEventListener("mouseout", (event) => {
    const leavingField = event.target.closest("[data-query-field]");
    if (!leavingField || leavingField !== activeField) return;
    const nextTarget = event.relatedTarget;
    if (
      nextTarget?.closest?.("[data-query-field]") === activeField ||
      nextTarget?.closest?.(".log-query-plus") ||
      nextTarget?.closest?.(".log-query-menu")
    ) {
      return;
    }
    hoverTimer = window.setTimeout(() => {
      if (!menuOpen) hideAll();
    }, 90);
  });

  button.addEventListener("mouseleave", () => {
    if (menuOpen) return;
    hoverTimer = window.setTimeout(hideAll, 90);
  });

  menu.addEventListener("mouseleave", () => {
    hoverTimer = window.setTimeout(hideAll, 90);
  });

  menu.addEventListener("mouseenter", () => {
    window.clearTimeout(hoverTimer);
  });

  button.addEventListener("mouseenter", () => {
    window.clearTimeout(hoverTimer);
  });

  document.addEventListener("click", (event) => {
    if (event.target.closest(".log-query-plus") || event.target.closest(".log-query-menu")) return;
    hideAll();
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") hideAll();
  });

  window.addEventListener("scroll", hideAll, true);
  window.addEventListener("resize", hideAll);

  return {
    hide: hideAll,
  };
};
