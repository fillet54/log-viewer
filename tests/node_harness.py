"""Run browser ES modules from src/cinc/static in Node.

Bare imports from the page's import map (`preact`, `preact/signals`, ...) resolve
to the vendored copies, and `localStorage` is an in-memory stub. Tests using this
are skipped when Node isn't installed.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

STATIC = Path(__file__).resolve().parents[1] / "src/cinc/static"
VENDOR = STATIC / "vendor"
NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="needs node")

IMPORT_MAP = {
    "preact": VENDOR / "preact.mjs",
    "preact/hooks": VENDOR / "hooks.mjs",
    "preact/signals": VENDOR / "signals.mjs",
    "@preact/signals-core": VENDOR / "signals-core.mjs",
    "htm": VENDOR / "htm-core.mjs",
    "htm/preact": VENDOR / "htm-preact.mjs",
}

HOOKS = """
const map = %s;
export async function resolve(specifier, context, next) {
  if (Object.hasOwn(map, specifier)) return { url: map[specifier], shortCircuit: true };
  return next(specifier, context);
}
"""

PRELUDE = """
import { register } from "node:module";
register(%s);
const store = new Map(Object.entries(%s));
globalThis.localStorage = {
  getItem: (k) => (store.has(k) ? store.get(k) : null),
  setItem: (k, v) => { store.set(k, String(v)); },
  removeItem: (k) => { store.delete(k); },
  key: (i) => [...store.keys()][i] ?? null,
  get length() { return store.size; },
  clear: () => store.clear(),
};
globalThis.storageSnapshot = () => Object.fromEntries(store);
const STATIC = %s;
const result = await (async () => {
%s
})();
console.log(JSON.stringify(result));
"""


def static_url(path: str) -> str:
    return (STATIC / path).as_uri()


def run_js(tmp_path: Path, body: str, storage: dict | None = None):
    """Run `body` (an async function body; `STATIC` is the static dir URL) and
    return what it returns, JSON round-tripped."""
    mapping = {name: path.as_uri() for name, path in IMPORT_MAP.items()}
    hooks = tmp_path / "hooks.mjs"
    hooks.write_text(HOOKS % json.dumps(mapping))
    script = tmp_path / "run.mjs"
    script.write_text(
        PRELUDE
        % (
            json.dumps(hooks.as_uri()),
            json.dumps(storage or {}),
            json.dumps(STATIC.as_uri()),
            body,
        )
    )
    done = subprocess.run(
        [NODE, str(script)], capture_output=True, text=True, check=False
    )
    if done.returncode:
        raise AssertionError(done.stderr)
    return json.loads(done.stdout.strip().splitlines()[-1])
