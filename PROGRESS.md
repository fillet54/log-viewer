# Implementation progress

Last updated: 2026-09-13

## Completed

- Phase foundation: introduced `LogType`, `Sample`, normalized-event validation, registry discovery, and the v2 page-data assembler.
- Added initial `CoreEventLogType` and `TextLogType` implementations plus a text-log sample.
- Added the dependency policy and vendor SHA-256 manifest.
- Began frontend identity migration: `CINC_PAGE_DATA`, `Cinc`, and renamed chart/timeline registration calls.
- Added absolute-time helpers, prefix-sum row metrics, and normalized-event contract tests.

## Commits

- `1413fb2` — v2 log-type contract and discovery foundation
- `8398bd2` — text-log import stream handling
- `db45e4b` — frontend identity migration and progress checkpoint
- `21f2300` — standalone page-data global migration
- `1eed74e` — v2 time helpers and contract tests

## Verification

- Black: passing on changed Python modules.
- flake8: passing on changed Python modules.
- Existing tests: 15 passing.

## Next work

1. Finish the backend clean break: app factory, routes, CLI, storage membership table, and bundle resolution.
2. Replace generated core-event fixtures with curated 3-channel and 4-channel samples.
3. Generalize live replay/session handling and fix restart behavior.
4. Finish frontend per-type dispatch, time model, and variable-height virtualization.
5. Harden standalone output, add documentation, and replace the legacy tests with the normative suite.
