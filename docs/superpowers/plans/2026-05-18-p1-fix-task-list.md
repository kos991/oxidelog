# OxideLog P1 Fix Task List

Date: 2026-05-18

## Current Status

Gemini read-only review found that P1 is not ready to ship.

Execution update:

- [x] Lifecycle delivery gap fixed by making scheduled lifecycle produce compact output without mutating or swapping the active DuckDB.
- [x] Frozen search format handling fixed for both `.raw.zst` line archives and `raw-import-*.tar.zst` archives.
- [x] Cold search day filtering fixed so unrelated archive/parquet files are not included for day-scoped searches.
- [x] Archive index rebuild now records actionable `source_addr`, `line_count`, `first_seen`, and `last_seen` metadata.
- [x] Archive search now preserves and filters Parquet `device_id`, and unified archive results expose `archive_path`.
- [x] Server validation passed for `fwlog-storage`, `fwlog-api`, release `fwlogd` build, service restart, and smoke endpoints.
- [x] 2026-05-19 final pass: P0-B hourly metrics API/UI and P0-C parser failure summary API/UI are implemented, built, deployed, and smoke-tested.
- [x] 2026-05-19 web date markers use hot blue dots and archive/cold gray dots from `/api/archive/days`; custom IP `归属地` UI remains present.
- [x] 2026-05-19 legacy DB/admission repair verified on server with service active.

Estimated completion after final pass:

```text
P0-A Hot DB Bound/Compaction: complete
P0-B Hourly Metrics: complete
P0-C Parser Observability: complete
P1-A Lifecycle Scheduler: complete in safe-output mode
P1-B Frozen Archive Index: complete
P1-C Device ID Binding: complete
P2-A Unified Search: complete
P2-B IP Region Cache: complete
P2-C Optional Search Adapter: complete
Admission MVP / legacy DB repair: complete
Overall: implemented and deployed; remaining risk is browser-only visual verification if a real browser session is required.
```

## P0 Fixes: Must Fix First

### 1. Fix P1-A lifecycle delivery gap

Problem:

`apps/fwlogd/src/pipeline.rs::run_lifecycle_scheduler` calls `run_lifecycle_to` and logs `output_path`, but it does not replace the active DuckDB file. The current hot DB does not shrink, so the lifecycle scheduler does not meet the P1-A outcome.

Additional risk:

`crates/fwlog-storage/src/lifecycle.rs::run_lifecycle_to` calls `prune_parsed_raw()` on the current store before the compacted DB is activated. This can clear parsed raw from the main DB even if compaction output is never used.

Required decision:

Pick one safe delivery mode:

1. Implement safe swap:
   - pause or coordinate the writer;
   - checkpoint and close old connections;
   - compact to temp path;
   - atomically rename/swap where safe;
   - handle Windows file locks and DuckDB WAL.

2. Or downgrade to manual compact:
   - do not claim automatic hot DB shrink;
   - avoid mutating the active DB before a safe activation path exists;
   - expose the generated compact DB as an operator action.

Acceptance:

- A lifecycle cycle either safely activates the compacted DB or leaves the active DB unchanged.
- No parsed raw is pruned from the active DB unless the lifecycle outcome is valid.
- Tests cover the chosen behavior.

### 2. Fix frozen archive format handling

Problem:

`fwlog_storage::write_frozen_raw` writes plain `.raw.zst` line files, but API cold search paths treat candidate frozen files as `tar.zst`. `rebuild_archive_index` can index `.raw.zst`, causing search to attempt the wrong reader.

Required fixes:

- Distinguish `.raw.zst` line files from `raw-import-*.tar.zst` archives.
- Either add separate readers for each format or restrict the index to searchable formats.
- Ensure cold search never reads `.raw.zst` through a tar reader.

Acceptance:

- `.raw.zst` search uses the raw-line reader or is excluded with an explicit reason.
- `raw-import-*.tar.zst` search uses the tar reader.
- Tests cover both formats.

### 3. Fix cold search day filtering

Problem:

`collect_cold_archive_files` / `collect_cold_parquet_files` use logic equivalent to `(day_matches || day.is_some())`, which means specifying a day can still include all archives and degenerate into full scan.

Required fixes:

- When `day` is provided, include only candidates matching that day.
- When `day` is absent, allow broader scan subject to limit.

Acceptance:

- A day-scoped cold search does not scan unrelated days.
- Test proves unrelated day files are not included.

## P1 Fixes: Next Priority

### 4. Fix archive index rebuild/import quality

Problem:

`rebuild_archive_index` uses low-quality metadata:

- `source_addr` fixed to `unknown`;
- `line_count=0`;
- `first_seen`/`last_seen` unavailable or weak;
- import flow is not wired to `upsert_frozen_archive_index`.

Required fixes:

- Populate real `day`, `source_addr`, `bytes`, `line_count`, `first_seen`, `last_seen`.
- Hook import/archive creation paths to index updates.
- Make rebuild inspect members or samples enough to derive useful metadata.

Acceptance:

- `find_frozen_archives(day, Some(source_addr))` can return indexed candidates.
- Rebuilt index contains actionable metadata.
- Tests cover indexed lookup by day and source.

### 5. Fix archived device_id support

Problem:

P1-C mostly works for hot events, but cold/archive search does not preserve or filter `device_id`.

Required fixes:

- Add `device_id` to archive/cold query types where relevant.
- SELECT and map `device_id` from Parquet rows when present.
- Ensure unified archive results expose `archive_path` and `device_id` correctly.
- Decide and implement stale backfill behavior: clear outdated `device_id`, or expose an explicit cleanup endpoint.

Acceptance:

- `/api/search?scope=archive&device_id=...` filters archived results correctly.
- Parquet row mapping preserves `device_id`.
- Backfill behavior is deterministic for deleted/disabled/changed devices.

## Test Work

Add or repair tests for:

1. lifecycle compact/swap or manual lifecycle activation behavior;
2. archive index lookup by day and source;
3. `/api/archive/index`;
4. `/api/archive/index/rebuild`;
5. `/api/devices/backfill`;
6. cold search day filtering does not full-scan;
7. `.raw.zst` and `tar.zst` frozen format handling;
8. archive/cold `device_id` filtering;
9. pipeline device binding if testable without starting listeners.

## Validation Commands

Run in an environment where Rust is available:

```bash
cargo test -p fwlog-storage
cargo test -p fwlog-api
cargo build -p fwlogd
```

If cargo is missing on Windows, run the same commands in the Rust-enabled Codex/Gemini terminal or fix PATH first.

## Coordination Rule

- Codex writes code.
- Gemini performs read-only review and test-gap checks.
- Claude coordinates, updates plans, and tracks diffs.
- Do not let two agents edit the same file set at the same time.
