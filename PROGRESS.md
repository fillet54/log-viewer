# Implementation progress

Last updated: 2026-09-13

Status: implementation in progress; the v2 specification remains the acceptance source.

## Completed

- Phase foundation: introduced `LogType`, `Sample`, normalized-event validation, registry discovery, and the v2 page-data assembler.
- Added initial `CoreEventLogType` and `TextLogType` implementations plus a text-log sample.
- Added the dependency policy and vendor SHA-256 manifest.
- Began frontend identity migration: `CINC_PAGE_DATA`, `Cinc`, and renamed chart/timeline registration calls.
- Added absolute-time helpers, prefix-sum row metrics, and normalized-event contract tests.
- Repository-wide Black and flake8 cleanup completed.
- Added bundle resolution, explicit type precedence, sniffing, and actionable ambiguity errors.

## Commits

- `1413fb2` — v2 log-type contract and discovery foundation
- `8398bd2` — text-log import stream handling
- `db45e4b` — frontend identity migration and progress checkpoint
- `21f2300` — standalone page-data global migration
- `1eed74e` — v2 time helpers and contract tests
- `a1980e0` — updated implementation progress checkpoint
- `c9a97cb` — repository-wide formatting and lint cleanup
- `14b46e3` — self-describing bundle resolution and type sniffing

## Verification

- Black: passing on changed Python modules.
- flake8: passing on changed Python modules.
- Existing tests: 15 passing.

## Next work

1. Finish the backend clean break: app factory, routes, CLI, and storage membership table.
2. Replace generated core-event fixtures with curated 3-channel and 4-channel samples.
3. Generalize live replay/session handling and fix restart behavior.
4. Finish frontend per-type dispatch, time model, and variable-height virtualization.
5. Harden standalone output, add documentation, and replace the legacy tests with the normative suite.
