# OxideLog P0/P1/P2 Operations Release Roadmap

## Purpose

This roadmap turns the existing P0/P1/P2 optimization plan into an operations-focused release sequence. The goal is to move OxideLog from a working import/demo system to a stable single-node firewall log platform that can be deployed, verified, rolled back, and operated safely.

The roadmap uses release gates instead of one large delivery. Each gate has a clear objective, deliverables, production validation, rollback point, and release criteria.

## Scope

In scope:

- Bound hot DuckDB storage and keep production query latency predictable.
- Move overview charts and operational views to metrics tables.
- Expose parser failure and source visibility for operations.
- Automate hot/cold/frozen lifecycle work.
- Add Frozen archive indexing for practical historical lookup.
- Bind events to managed device identities.
- Unify hot and frozen search results.
- Add IP attribution caching.
- Keep external full-text search optional and non-blocking.

Out of scope:

- Replacing OxideLog with Logstash, Elasticsearch, or Quickwit in P0/P1.
- Making external search a production dependency.
- UI redesign unrelated to operations visibility.
- Large architecture changes outside the current Rust, Axum, DuckDB, Frozen archive, and Ant Design Pro stack.

## Release Gate Summary

| Gate | Name | Main Outcome |
| --- | --- | --- |
| G0 | Baseline freeze | Current production state can be rebuilt, validated, backed up, and rolled back. |
| G1 | P0 stable hot store and observability | Hot storage is bounded and operations can see parser/source health. |
| G2 | P1 lifecycle and historical index | Retention, compaction, archive indexing, and device binding are operationalized. |
| G3 | P2 search and analysis enhancement | Search and attribution improve without making ingest or operations depend on optional services. |

## G0: Baseline Freeze

### Objective

Protect the current working system before P0/P1/P2 changes start. G0 should produce a known-good baseline that operations can restore if later gates fail.

### Deliverables

- Confirm current `main` builds and tests.
- Capture current production smoke results.
- Back up hot DuckDB, `data/frozen`, `data/parquet`, service config, and deployed `web/` assets.
- Record deployed binary version, service path, config path, API endpoint, TCP/UDP ingest ports, and data paths.

### Production Validation

Run the existing production smoke script against the target server. It should cover:

- `/api/health`.
- `/api/system/status`.
- Sample TCP ingest, unless intentionally skipped.
- `/api/events`.
- CSV export.
- Parquet archive/list.
- Frozen archive/list/restore.

### Rollback Point

G0 rollback means restoring:

- Previous `fwlogd` binary.
- Previous `web/` directory.
- Previous DuckDB and archive directories.
- Previous service config and environment file.

### Release Criteria

- Workspace tests pass or known failures are documented before changes begin.
- Production smoke succeeds against the current deployment.
- Backups are complete and restorable.
- Operators know the exact command sequence for rollback.

## G1: P0 Stable Hot Store and Observability

### Objective

Make OxideLog safe to run continuously by bounding hot storage growth and making parser/source health visible.

### Deliverables

1. Hot DuckDB raw pruning and compaction.
   - Parsed rows can drop large `raw` payloads.
   - Failed rows keep original `raw` for troubleshooting.
   - Hot compaction keeps the newest bounded row set.

2. Metrics-backed overview.
   - Long-range overview and trend charts read aggregated metric tables.
   - UI avoids scanning the full event table for normal dashboard views.

3. Parser and source observability.
   - Parser failure counts and reasons are available through API/UI.
   - Source device or source address distribution is visible.
   - Operations can tell whether traffic is healthy, malformed, missing, or dominated by a single source.

### Production Validation

- Run storage and API tests for pruning, compaction, metrics, and parser stats.
- Ingest representative Sangfor logs and malformed lines.
- Verify parsed events retain normalized fields after raw pruning.
- Verify failed events keep raw content and expose parse error details.
- Open the dashboard against a large dataset and confirm overview queries use metrics endpoints.
- Run production smoke after deploying backend and frontend.

### Rollback Point

G1 features should be released in independently reversible units:

- Disable or skip hot pruning/compaction if unexpected data preservation concerns appear.
- Keep event-table queries available while metrics-backed UI is being validated.
- Parser stats must not block ingest or event writes.

### Release Criteria

- Hot DuckDB size remains predictable after compaction.
- Dashboard remains usable on production-scale hot data.
- Parser failure visibility is sufficient for operations to diagnose bad input.
- No P0 change can prevent live ingest from writing events.

## G2: P1 Lifecycle and Historical Index

### Objective

Turn manual storage maintenance and blind archive scans into an automated, observable lifecycle.

### Deliverables

1. Automated lifecycle scheduler.
   - Retention, compaction, and archive maintenance run on schedule.
   - Lifecycle state is visible in system status or a dedicated endpoint.
   - Scheduler resumes correctly after service restart.

2. Frozen archive index.
   - Frozen archive metadata is indexed.
   - Historical lookup can narrow candidates by time, source, or indexed metadata instead of scanning all `.raw.zst` files.
   - Index rebuild is available for recovery.

3. Managed device ID binding.
   - Events can be associated with stable managed device IDs.
   - Device binding can be backfilled for historical data.
   - Device identity improves operations views without blocking raw event ingest.

### Production Validation

- Restart the service and confirm lifecycle scheduling continues.
- Run lifecycle job manually or wait for a scheduled run and verify status output.
- Create or use Frozen archives, rebuild the index, and verify archive lookup narrows candidate files.
- Verify device list and event source distribution remain consistent after backfill.
- Extend production smoke to check lifecycle status, Frozen index/list/restore, and device binding endpoints.

### Rollback Point

- Lifecycle scheduler must be configurable so operations can disable it quickly.
- Frozen index failure must not affect live ingest, hot queries, or raw archive restore.
- Device binding failure must not block event writes.
- Index and device backfill should be repeatable and safe to rerun.

### Release Criteria

- Operators can see when lifecycle jobs last ran and whether they succeeded.
- Frozen historical lookup no longer requires blind full-archive scanning for normal use.
- Device identity is stable enough for operations dashboards and future alerting.
- P1 automation does not create a single point of failure for ingest.

## G3: P2 Search and Analysis Enhancement

### Objective

Improve historical search and attribution after storage lifecycle is stable, while keeping native OxideLog operation independent of external search services.

### Deliverables

1. Unified search result model.
   - Hot DuckDB and Frozen results return the same shape.
   - Result metadata identifies source tier and archive location when applicable.
   - Search errors are isolated from ingest, metrics, and basic event queries.

2. IP region cache.
   - Source and destination IP attribution can be cached.
   - Cache can be rebuilt or invalidated.
   - Failed attribution does not modify original event content or block queries.

3. Optional full-text search adapter.
   - Native search remains the default backend.
   - External search is configured behind a switch such as `[search].backend = "native"`.
   - Quickwit or another external backend is considered only after native hot/frozen lifecycle is stable.

### Production Validation

- Search hot and Frozen data through the same API and verify fields match.
- Verify search still works with external search disabled.
- Rebuild IP attribution cache and compare source/destination attribution on known IPs.
- Simulate optional search backend unavailability and confirm the platform still starts and handles ingest.

### Rollback Point

- Disable unified search UI while keeping `/api/events` and archive restore available.
- Disable IP attribution cache without changing stored events.
- Keep external full-text search completely optional and safe to remove from config.

### Release Criteria

- Unified search improves operator workflow without destabilizing ingest or lifecycle jobs.
- IP attribution is additive and reversible.
- External search does not become a startup, ingest, or dashboard dependency.

## Recommended Execution Order

1. G0 baseline freeze and backups.
2. P0-A hot DuckDB raw pruning and compaction.
3. P0-B hour/device metrics and metric-backed UI views.
4. P0-C parser failure and source observability.
5. P1-B Frozen archive index.
6. P1-A lifecycle scheduler.
7. P1-C device ID binding and backfill.
8. P2-B IP region cache.
9. P2-A unified search API and result model.
10. P2-C optional external full-text search adapter.

Frozen indexing comes before lifecycle scheduling so operations can inspect and rebuild archive metadata before automation depends on it.

## Operational Risk Controls

- Keep ingest independent from parser stats, archive index, device binding, search, and IP attribution.
- Make lifecycle automation configurable and observable.
- Preserve failed raw logs until operations explicitly decide retention behavior.
- Prefer additive APIs and UI panels before removing older operational paths.
- Run production smoke after every gate, not only after G3.
- Treat external search as an enhancement, never as a platform requirement.

## Gate Review Template

Before moving from one gate to the next, confirm:

- Build and tests passed for changed crates.
- Production smoke passed.
- Rollback command sequence is known.
- New operational endpoint or UI view was checked manually.
- Logs show no repeated lifecycle, archive, parser, or ingest errors.
- Any failed validation has an owner and a decision: fix now, defer, or roll back.

## Final Success Criteria

OxideLog is ready after G3 when:

- Hot storage remains bounded under continuous ingest.
- Metrics views stay fast on production-scale data.
- Operators can diagnose parser/source issues from the UI or API.
- Lifecycle jobs are automatic, visible, and safe to disable.
- Frozen historical lookup is indexed and recoverable.
- Device identity is stable across live and historical events.
- Search returns consistent results across hot and frozen tiers.
- IP attribution is cached and reversible.
- Optional external search can be disabled with no loss of core platform function.
