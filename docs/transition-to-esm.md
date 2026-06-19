# Plan: Transition to ES Modules (Buildless with Import Maps)

This plan outlines the transition from a global-based component registry to a native ES Module architecture using Import Maps.

## Objectives
- Use native `import` and `export` statements in all components.
- Use an **Import Map** for bare module resolution (e.g., `import { h } from 'preact'`).
- Support a single-file standalone export using a **Data URI Import Map** strategy.
- Maintain a "buildless" development workflow.

## Proposed Architecture

### 1. ESM Vendor Files
Replace UMD vendor files with `.mjs` versions:
- `src/eventlog2/static/vendor/preact.mjs`
- `src/eventlog2/static/vendor/hooks.mjs`
- `src/eventlog2/static/vendor/signals.mjs`
- `src/eventlog2/static/vendor/htm.mjs`

### 2. Import Map (`base.html`)
Define an Import Map to resolve bare imports:
```html
<script type="importmap">
{
  "imports": {
    "preact": "/static/vendor/preact.mjs",
    "preact/hooks": "/static/vendor/hooks.mjs",
    "preact/signals": "/static/vendor/signals.mjs",
    "htm/preact": "/static/vendor/htm.mjs"
  }
}
</script>
```

### 3. ESM Bridge (`src/eventlog2/static/logview/lib.js`)
A bridge for non-ESM libraries and shared utilities:
```javascript
export const Split = window.Split;
export const Chart = window.Chart;
// Shared Preact exports for convenience if needed
export { html } from 'htm/preact';
```

### 4. Component Refactoring
Refactor components to use ESM. Example:
```javascript
import { html } from 'htm/preact';
import { useState } from 'preact/hooks';

export const MyComponent = () => { ... };
```

### 5. Standalone Export (Data URI Strategy)
Update `standalone.py` to:
1. Encode library contents as Base64 Data URIs.
2. Generate an Import Map in the standalone HTML where bare imports point to these Data URIs.
3. Inline all components into a single `<script type="module">` block (or multiple module blocks).
4. **Key Benefit:** Component code remains identical between dev and standalone.

## Implementation Phases

### Phase 1: Vendor and Infrastructure
1. Download `.mjs` files to `src/eventlog2/static/vendor/`.
2. Update `base.html` with the Import Map.
3. Create `src/eventlog2/static/logview/lib.js`.
4. Refactor `app.js` and `mount.js` to ESM.

### Phase 2: Detail Feature Refactoring
1. Refactor all files in `src/eventlog2/static/logview/features/detail/` to use ESM.

### Phase 3: Search and Main Features
1. Refactor `search`, `main`, `layout`, and `ViewerRoot.js` to use ESM.

### Phase 4: Standalone Logic
1. Update `standalone.py` with the Data URI Import Map logic.
2. Verify standalone export.

## Verification Plan
- **Development:** Verify app loads and functional (recursive ESM loading).
- **Standalone:** Verify `standalone.py` produces a working single-file HTML with no external dependencies.
