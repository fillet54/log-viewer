# Preact Migration Plan

## Summary

Migrate the UI from imperative custom-element rendering to Preact function components with hooks, without changing the Flask/backend page-data contract or breaking standalone offline exports.

This should be an incremental migration, not a rewrite. The safest path is to keep the existing service layer and plugin contracts in place first, then replace each UI surface one panel at a time.

## Current Architecture

| Area | Current file(s) | Notes |
| --- | --- | --- |
| App bootstrap | `src/eventlog2/static/app.js` | Defines `log-viewer-app`, shared helpers, page-data loading, plugin script injection, base custom-element APIs |
| Services | `src/eventlog2/static/services/app-services.js` | Event bus, bookmarks, comments, root services |
| Layout | `src/eventlog2/static/components/log-layout.js` | Split.js shell, collapse/expand state, pane wiring |
| Main view | `src/eventlog2/static/components/log-main-view.js` | Toolbar, view mode, chart/list split, virtualized log list, selection |
| Detail panel | `src/eventlog2/static/components/log-detail-panel.js` | Selected event details, data tree, comments, bookmark color editing |
| Search panel | `src/eventlog2/static/components/log-search-panel.js` | Search history, pinned filters, saved filters, results virtualization, bookmark activity |
| Plugin row rendering | `src/eventlog2/plugins/core_event/static/row.js` | Plugin row renderer returns DOM nodes |
| Plugin charts/views | `src/eventlog2/static/components/log-main-chart.js`, `src/eventlog2/plugins/core_event/static/charts.js` | Registry-based chart/timeline plugins |
| HTML templates | `src/eventlog2/templates/*.html` | Flask page shell and standalone export shell |
| Standalone export | `src/eventlog2/standalone.py` | Inlines all JS/CSS into a self-contained HTML file |

## Goals

- Move UI code to Preact function components and hooks.
- Reduce `innerHTML`/manual DOM wiring and move state into declarative components.
- Preserve the existing `window.EVENTLOG2_PAGE_DATA` contract.
- Preserve plugin registration and plugin row rendering during the migration.
- Preserve standalone HTML export with no network dependency at runtime.
- Keep the migration shippable in small PRs.

## Non-Goals

- Rewriting Flask routes or plugin parsing code.
- Changing the page-data schema.
- Replacing Chart.js or Split.js.
- Introducing a mandatory Node/Vite/Webpack build step in phase 1.

## Constraints

1. The app currently runs as plain static scripts loaded from Flask templates.
2. `standalone.html` is a self-contained artifact built by `src/eventlog2/standalone.py`.
3. Plugin code is loaded as script text from `pageData.view.scripts`, so global compatibility matters.
4. The main log list and search results both use virtualization and should not regress on large data sets.

## Recommended Technical Direction

### Runtime choice

Use vendored browser builds of:

- `preact`
- `preact/hooks`
- `htm`
- `@preact/signals`

Reasoning:

- Keeps the app build-free initially.
- Keeps the runtime served from the Flask app itself.
- Avoids any Node/npm dependency in the app runtime or migration path.
- Works with function components and hooks.
- Fits the existing script-tag loading model.
- Can be inlined into standalone exports just like the existing vendor scripts.

Do not rely on CDNs. Preact must always be loaded from local static files served by the app, and the same files must be inlined into standalone exports.

### Mount strategy

Use a single top-level Preact mount rooted at `log-viewer-app`.

During migration it was acceptable to use temporary custom-element mount shells for major panes, but the desired end state is one Preact tree that renders layout, main view, detail, and search directly.

This avoids a big-bang template rewrite and lets us migrate one panel at a time.

### State strategy

Keep these existing APIs first:

- `LogServices.createRootServices(...)`
- `window.EVENTLOG2_PAGE_DATA`
- existing event bus events
- chart/timeline registry APIs
- plugin row renderer registration APIs

Wrap them with Preact context/hooks and signals rather than replacing them immediately.

### Plugin compatibility strategy

Do not rewrite plugin row renderers in phase 1.

Instead, add a small bridge component that:

- receives an event plus plugin renderer
- asks the plugin renderer for a DOM node
- mounts that DOM node into a `ref` container in `useLayoutEffect`

That lets the main list move to Preact without forcing every plugin renderer to become JSX immediately.

## Target Architecture

### Keep stable

- Flask templates and route behavior
- page-data JSON shape
- search worker API
- plugin registration shape
- standalone export entry point

### Replace gradually

- manual `innerHTML` rendering
- most direct `addEventListener` wiring inside components
- controller classes that exist mainly to manage DOM state

### New frontend structure

Suggested directory layout:

```text
src/eventlog2/static/
  runtime.js
  context.js
  mount.js
  hooks/
    use-app-services.js
    use-bus.js
    use-local-storage.js
    use-split.js
    use-virtual-list.js
  components/
    viewer-root.js
    layout-shell.js
    detail-panel.js
    search-panel.js
    main-view-shell.js
    log-main-chart.js
```

## Migration Phases

### Phase 0: Baseline And Guardrails

Goal: lock down behavior before changing rendering.

Steps:

1. Write this migration spec.
2. Capture a manual smoke-test checklist for:
   - initial page load
   - theme toggle
   - main chart switching
   - split resize persistence
   - search query execution
   - bookmark and comment persistence
   - standalone HTML opening without network access
3. Identify current globals that must remain stable during migration:
   - `LogServices`
   - `LogSearch`
   - `LogMainViewChart`
   - `LogMainViewTimeline`
   - `window.EventLog2`

Acceptance:

- No code changes yet beyond docs and optional verification notes.

### Phase 1: Add Preact Runtime Without Changing Behavior

Goal: make Preact available everywhere with zero UI changes.

Steps:

1. Add vendored runtime files under `src/eventlog2/static/vendor/`.
2. Add script tags to `src/eventlog2/templates/base.html`.
3. Add the same files to `SCRIPT_PATHS` in `src/eventlog2/standalone.py`.
4. Add `src/eventlog2/static/runtime.js` that exposes a stable global helper, for example:
   - `window.EventLog2UI.html`
   - `window.EventLog2UI.render`
   - `window.EventLog2UI.hooks`
   - `window.EventLog2UI.signals`
5. Verify normal Flask pages and standalone pages still load.

Acceptance:

- No visual changes.
- No network dependency for standalone output.

### Phase 2: Add Shared Preact Bridge Layer

Goal: introduce reusable hooks and a root mount pattern.

Steps:

1. Add `context.js` for app services.
2. Add hooks for:
   - bus subscription
   - localStorage-backed state
   - Split.js lifecycle
   - virtualization
3. Define when to prefer signals vs hooks:
   - signals for shared UI state that crosses component boundaries frequently
   - hooks for component-local ephemeral state
4. Add a tiny mount helper so custom elements can render a Preact component into themselves.
5. Keep the existing service creation code in `app-services.js`; only wrap it.

Acceptance:

- Preact components can read existing services without changing backend data flow.

### Phase 3: Migrate The Detail Panel First

Goal: convert the lowest-risk panel before touching the main list.

Why first:

- isolated surface
- no Split.js ownership
- limited performance risk
- easy visual diff

Steps:

1. Move the detail panel rendering logic into pure helper functions where useful.
2. Replace `DetailPanelController.renderEvent()` with a Preact `DetailPanel` component.
3. Use hook state for:
   - selected event
   - expanded/collapsed data tree nodes
   - active reply target
4. Keep comment and bookmark services unchanged.
5. Keep the `event:selected` bus event unchanged.

Acceptance:

- Selecting a row still updates the panel.
- Comment posting still works.
- Bookmark color editing still works.

### Phase 4: Migrate Search Panel Chrome

Goal: move the non-virtualized search UI to Preact first.

Steps:

1. Convert tabs, query input, help dialog, history, pinned items, and saved filters.
2. Keep the existing search worker API unchanged.
3. Keep localStorage keys unchanged.
4. Keep result rendering imperative temporarily if that reduces risk.

Acceptance:

- Search history, pinned queries, and filters behave exactly as before.

### Phase 5: Migrate Search Results Virtualization

Goal: finish the search panel by moving result rendering to Preact.

Steps:

1. Introduce a reusable `useVirtualList` hook.
2. Render result rows through the plugin-row bridge instead of direct `innerHTML`.
3. Preserve click-to-select and jump-to-row behavior.
4. Preserve bookmark and comment activity rendering.

Acceptance:

- Large result sets still scroll smoothly.
- Clicking a result still selects and jumps correctly.

### Phase 6: Migrate Layout Shell

Goal: replace `LayoutController` with declarative state plus Split.js effects.

Steps:

1. Move collapse state into hooks backed by the existing localStorage keys.
2. Wrap Split.js creation/destruction in a hook.
3. Keep DOM ids and CSS class names stable where possible to reduce CSS churn.
4. Keep pane structure compatible with the existing main/detail/search children.

Acceptance:

- Top/right and bottom split behavior remains unchanged.
- Persisted split sizes still restore correctly.

### Phase 7: Migrate Main View Toolbar And Chart Host

Goal: convert the main-view shell before the virtual log list.

Steps:

1. Migrate view-mode state and toolbar buttons to Preact.
2. Preserve the chart/timeline registry APIs from `log-main-chart.js`.
3. Wrap Chart.js and plugin panel mounting in effect-driven Preact components.
4. Keep current chart type selection behavior and command bar behavior.

Acceptance:

- Chart type switching still works.
- Split/chart/list mode persistence still works.

### Phase 8: Migrate Main Log Virtual List

Goal: move the highest-risk view last, after the shared hooks are proven.

Steps:

1. Reuse the `useVirtualList` hook already proven in search results.
2. Replace manual `renderRange()` DOM writes with declarative row rendering.
3. Use the plugin-row bridge for row content.
4. Preserve:
   - row selection
   - bookmark toggles
   - match-link jumps
   - scroll-to-row behavior
   - `log:scroll`, `log:jump`, `log:filtered`, and `event:selected` bus behavior

Acceptance:

- Main list performance stays acceptable on large logs.
- Selection and jump behavior remain correct.

### Phase 9: Simplify Bootstrap And Remove Dead Code

Goal: clean up after all panels are migrated.

Steps:

1. Remove controller code and `innerHTML` rendering paths that are no longer used.
2. Reduce `app.js` to app bootstrapping, service setup, and compatibility shims.
3. Decide whether to keep custom elements as permanent mount shells or replace them with plain mount nodes.
4. Update standalone script ordering if any files moved.

Acceptance:

- No unused legacy component code remains.
- Startup path is simpler than the original.

## Small PR Sequence

Recommended PR-sized slices:

1. Add vendored Preact runtime and standalone support.
2. Add Preact bridge/helpers with no behavior changes.
3. Convert detail panel.
4. Convert search panel shell.
5. Convert search results virtualization.
6. Convert layout shell.
7. Convert main view toolbar/chart host.
8. Convert main log virtualization.
9. Remove dead legacy code and refresh docs.

## Risks And Mitigations

#### Risk: standalone export breaks

Mitigation:

- treat `src/eventlog2/standalone.py` as part of every frontend PR
- verify exported HTML with network disabled

#### Risk: plugin row renderers do not fit declarative rendering

Mitigation:

- keep the renderer contract temporarily
- bridge DOM nodes into Preact with refs
- defer plugin renderer API redesign until after the app migration

#### Risk: virtualization performance regresses

Mitigation:

- migrate detail/search shell first
- reuse one virtualization hook in both search and main view
- compare large-log scrolling before removing legacy code

#### Risk: too much churn in one PR

Mitigation:

- keep localStorage keys, bus events, and page-data shape unchanged until the end
- do not mix layout, main list, and plugin API changes in the same PR

## Definition Of Done

The migration is complete when:

- all major UI surfaces render through Preact function components
- hooks own UI state instead of controller classes
- standalone export works without network access
- plugin charts and plugin row rendering still function
- legacy imperative render paths have been removed

## Recommended First Implementation Task

Start with Phase 1 and Phase 2 only:

1. vendor `preact`, `preact/hooks`, and `htm`
2. wire them into Flask templates and standalone export
3. add a tiny Preact mount helper
4. stop there and verify behavior before converting any panel

That gives a safe foundation and keeps the first code change small and reversible.
