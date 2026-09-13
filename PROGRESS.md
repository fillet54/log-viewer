# Implementation progress

Last updated: 2026-09-13

Status: implementation in progress; the v2 specification remains the acceptance source.

Phase 1 status: complete. Obsolete source/docs/vendor files were removed, runtime
dependencies are constrained, and the vendor manifest is synchronized with the eight
active browser libraries. Local `cinc-data/sessions` was preserved because it contains
populated local capture data.

Phase 2 status: curated fixture replacement is complete. Core-event now has exactly two
small, declared 3-channel and 4-channel samples with 40 ordered events each. The generic
home and demo routes remain part of Phase 3 application wiring because they depend on the
registry-backed app factory.

Phase 3 status: registry-backed app and standalone paths are in place, legacy registry and
plugin-manager modules are removed, and the old v1 plugin test was retired for replacement
by the normative suite. Remaining route/CLI/live integration is being completed in the
subsequent phases.

Phase 4 status: core time consumers now use ISO `time`/absolute milliseconds, storage no
longer synthesizes epoch timestamps, and navigation/bookmark/detail paths use time values.
Remaining legacy core-event presentation fields are plugin-owned and will be finalized with
the plugin UI work.

## Completed

- Phase foundation: introduced `LogType`, `Sample`, normalized-event validation, registry discovery, and the v2 page-data assembler.
- Added initial `CoreEventLogType` and `TextLogType` implementations plus a text-log sample.
- Added the dependency policy and vendor SHA-256 manifest.
- Began frontend identity migration: `CINC_PAGE_DATA`, `Cinc`, and renamed chart/timeline registration calls.
- Added absolute-time helpers, prefix-sum row metrics, and normalized-event contract tests.
- Repository-wide Black and flake8 cleanup completed.
- Added bundle resolution, explicit type precedence, sniffing, and actionable ambiguity errors.
- Added `log_record_types` membership storage and mandatory normalized event timestamps.

## Commits

- `1413fb2` — v2 log-type contract and discovery foundation
- `8398bd2` — text-log import stream handling
- `db45e4b` — frontend identity migration and progress checkpoint
- `21f2300` — standalone page-data global migration
- `1eed74e` — v2 time helpers and contract tests
- `a1980e0` — updated implementation progress checkpoint
- `c9a97cb` — repository-wide formatting and lint cleanup
- `14b46e3` — self-describing bundle resolution and type sniffing
- `c52be9e` — storage memberships and mandatory event time
- `2117834` — complete Phase 1 hygiene and vendor manifest
- pending — Phase 2 curated samples
- `083c54c` — registry-backed app and standalone migration slice
- pending — Phase 4 absolute-time migration

## Verification

- Black: passing on changed Python modules.
- flake8: passing on changed Python modules.
- Existing tests: 15 passing.

## Next work

1. Finish the backend clean break: app factory, routes, and CLI.
2. Replace generated core-event fixtures with curated 3-channel and 4-channel samples.
3. Generalize live replay/session handling and fix restart behavior.
4. Finish frontend per-type dispatch, time model, and variable-height virtualization.
5. Harden standalone output, add documentation, and replace the legacy tests with the normative suite.
