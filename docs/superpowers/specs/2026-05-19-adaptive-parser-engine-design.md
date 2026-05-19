# OxideLog Adaptive Parser Engine Design

## Goal

Refactor OxideLog parsing from a single Sangfor-oriented adapter into a deterministic, multi-vendor parser framework with automatic adaptive routing, field learning, diagnostics, and rollback controls.

The first implementation should improve two things:

- Multi-vendor extensibility: adding Huawei, H3C, Topsec, or new Sangfor variants should not require rewriting the engine.
- Parser observability: operators should know which parser matched, why parsing failed, and which adaptive rules were learned or disabled.

Automatic adaptation is in scope, but it must be guarded by confidence thresholds, shadow validation, and automatic rollback. The engine should become more useful over time without turning parsed events into black-box guesses.

The hot parse path must be designed as a high-throughput data plane. Per-event parsing must not sort adapters, allocate temporary routing vectors, update DuckDB, or contend on shared counters. Adaptive learning belongs on the control path and must feed the parser through precomputed, read-only snapshots.

## Current Context

Current parsing lives in `crates/fwlog-adapter`:

- `ParserEngine` owns a list of boxed `LogAdapter` implementations.
- `SangforAdapter` is the only default adapter.
- Live ingest in `apps/fwlogd` and historical import in `apps/fwlog-import` both call `ParserEngine::new().parse(raw)`.
- `CanonicalEvent` currently has `Parsed` and `Failed` parse statuses.
- Storage and API already expose parser failure summaries and source/device visibility.

This design keeps the shared parser path for live and historical data. It does not introduce Logstash, Elasticsearch, Quickwit, or another external parser dependency.

## Architecture

The parser engine will have four layers.

### Parser Registry

The registry owns deterministic parser adapters. Each adapter exposes:

- `name`: stable parser name, such as `sangfor_nat_v1`.
- `vendor`: vendor name, such as `Sangfor`.
- `priority`: base ordering before adaptive boosts.
- `detect(raw)`: returns a detection score and match reason.
- `parse(raw)`: returns a parsed event plus diagnostics.

The existing `SangforAdapter` becomes the first registered adapter. Future adapters should be isolated by vendor or log family rather than bundled into one large parser file.

### Adaptive Router

The router records which parser succeeds for each scope:

- `device:<device_id>` when a device id is known.
- `source:<source_addr>` when only the transport source is known.

For each new log line, the router uses precomputed route groups that already include profile-derived boosts. This reduces blind parser attempts and makes multi-vendor support cheaper during live ingest and historical import.

The router must never sort adapters on the hot path. It must load an immutable route snapshot and walk fixed route groups in order. A high-confidence built-in parser should still win over an adaptive rule.

Implementation shape:

- Keep adapter registry order stable.
- Build route snapshots on a background control task.
- Represent routes as fixed priority groups, for example `[Option<StaticRouteGroup>; 16]`.
- Store adapter ids in route groups and resolve them against a static registry; do not allocate boxed candidate vectors per event.
- Publish route snapshots with `arc-swap` so workers perform an atomic pointer load and then read immutable arrays.
- Treat the hot-path route lookup as O(1) with no per-event heap allocation.

### Adaptive Field Mapper

When deterministic adapters fail or produce incomplete output, a generic extractor scans the raw line for structured fields:

- `key=value`
- `key: value`
- comma-separated fields
- Chinese and mojibake field names already seen in Sangfor logs

The mapper learns aliases from raw keys to canonical fields:

- `src_ip`
- `src_port`
- `dst_ip`
- `dst_port`
- `protocol`
- `action`
- `severity`

Learned rules are scoped by device/source and pass through `shadow` before becoming `active`.

The generic extractor must be defensive because it handles unknown or hostile input. It must not depend on complex regexes. It should use a bounded, single-pass byte scanner and SIMD-assisted delimiter search through `memchr` where useful.

Extractor output should borrow from the raw line:

```rust
pub struct GenericPair<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

pub struct GenericPairs<'a, const N: usize> {
    pub pairs: arrayvec::ArrayVec<GenericPair<'a>, N>,
}
```

This avoids `String` allocation and avoids `HashMap` allocation on the hot path. If a later stage needs keyed lookup, it should either scan the bounded pair array or build an index only on the control path.

### Confidence Gate

Automatic activation is allowed only when all guardrails pass:

- The same raw key appears repeatedly in the same scope.
- The value type is stable, for example IP, port, protocol, action, or string.
- `sample_count` meets the configured threshold.
- Wilson score lower bound meets the configured threshold.
- The rule fills an empty canonical field and does not overwrite a deterministic adapter field.
- Shadow validation shows the rule would improve parsed or partial output.

Default target thresholds:

- `min_samples = 1000`
- `activation_wilson_lower_bound = 0.98`
- `rollback_failed_ratio = 0.20`
- `max_generic_line_bytes = 8192`
- `max_generic_pairs = 64`

These values should be configurable.

The confidence gate must not use raw observed success ratio alone. Rule activation should use the lower bound of the Wilson score interval so small, temporarily homogeneous samples cannot activate brittle rules. The stored `confidence` field should represent the Wilson lower bound used for activation, with raw wins and samples available for audit.

## Hot Path Performance Contract

The live parse path is the data plane. It must obey these rules:

- No per-event adapter sorting.
- No per-event heap allocation for route candidates.
- No per-event DuckDB writes for parser profiles, adaptive rules, or diagnostics.
- No shared atomic counter increments for high-cardinality success/failure metrics.
- No complex regexes in the generic extractor.
- No owned `String` copies for generic extracted keys and values unless the event is being materialized for storage.

Adaptive state changes are the control plane. A background manager consumes batched events, updates DuckDB, recomputes static route snapshots, and publishes them atomically.

## Data Model

Parser adaptive state should live outside the hot event rows to avoid slowing normal event queries.

### `parser_profiles`

Tracks parser success and failure by scope.

Columns:

- `scope_key TEXT`
- `parser_name TEXT`
- `success_count BIGINT`
- `fail_count BIGINT`
- `last_seen TEXT`
- `priority_boost DOUBLE`

Primary key: `(scope_key, parser_name)`.

### `adaptive_field_rules`

Stores automatically learned field alias rules.

Columns:

- `rule_id TEXT PRIMARY KEY`
- `scope_key TEXT NOT NULL`
- `raw_key TEXT NOT NULL`
- `canonical_field TEXT NOT NULL`
- `value_type TEXT NOT NULL`
- `status TEXT NOT NULL`
- `confidence DOUBLE NOT NULL`
- `wins BIGINT NOT NULL`
- `sample_count BIGINT NOT NULL`
- `created_at TEXT NOT NULL`
- `activated_at TEXT`
- `disabled_at TEXT`
- `disabled_reason TEXT`

Supported status values:

- `shadow`
- `active`
- `disabled`

Rules are unique by `(scope_key, raw_key, canonical_field)`. `confidence` stores the Wilson lower bound, not the raw observed match ratio.

### `parser_diagnostics`

Groups parse failures, partial parses, and adaptive decisions.

Columns:

- `fingerprint TEXT PRIMARY KEY`
- `scope_key TEXT`
- `reason TEXT NOT NULL`
- `sample_raw TEXT`
- `count BIGINT NOT NULL`
- `suggested_rule_id TEXT`
- `last_seen TEXT NOT NULL`

Fingerprints should be based on normalized missing fields and raw key shapes, not full raw payloads.

Fingerprint generation must be cardinality-guarded. For each scope, the control path should track recent fingerprint growth with a sliding-window Bloom filter, count-min sketch, or equivalent bounded structure. If one scope creates more than the configured number of distinct fingerprints in a minute, the scope enters malformed-flood mode:

- Stop creating precise fingerprints for that scope during the window.
- Stop generating new shadow adaptive rules for that scope.
- Group subsequent failures under `FINGERPRINT_MALFORMED_FLOOD`.
- Keep parsing deterministic adapters normally.

Default guard:

- `max_fingerprints_per_scope_per_minute = 200`

## Parse Status

Add a third status to `ParseStatus`:

- `Parsed`: required fields are present with no parser warnings.
- `Partial`: useful core fields exist, but non-critical required-by-adapter fields are missing.
- `Failed`: minimum searchable fields are not present.

Minimum fields for adaptive parsed output:

- `src_ip`
- `dst_ip`
- at least one of `action` or `protocol`

If `src_ip` and `dst_ip` exist but action/protocol is missing, the event should be `Partial`.

Existing UI/API filters should treat `Partial` as searchable event data by default. Status counts must show partial separately from fully parsed and failed rows.

## Parse Flow

1. Receive `RawLog`.
2. Resolve scope from `device_id` if available, otherwise `source_addr`.
3. Load the immutable route snapshot for the scope.
4. Walk route groups in snapshot order.
5. Run bounded detection and parsing for deterministic adapters.
6. If an adapter returns `Parsed`, emit the event and append a compact success record to the local metrics batch.
7. Optionally enqueue sampled shadow-validation work to the control path; do not run unbounded adaptive validation inline.
8. If deterministic adapters fail or return `Partial`, run generic KV extraction.
9. Apply active adaptive field rules only to empty canonical fields.
10. Classify output as `Parsed`, `Partial`, or `Failed`.
11. Emit a compact parser metrics event to a local batch buffer.
12. Flush parser metrics batches to a background control task after a count or time threshold.
13. The control task updates profiles, diagnostics, and shadow rule samples in bulk.
14. Background evaluation promotes eligible shadow rules to active.
15. The control task rebuilds and publishes route snapshots with `arc-swap`.
16. Rollback evaluation disables active rules if the scope failure ratio rises after activation.

## Control Path Batching

Parser profile and diagnostics updates must be asynchronous. Workers should keep local non-atomic counters or thread-local batches keyed by scope and parser. A worker flushes a `MetricsFlushEvent` when either threshold is reached:

- `metrics_flush_count = 1000`
- `metrics_flush_interval_ms = 100`

The background control task receives flush events through an MPSC channel or bounded ring buffer. It merges batches, writes DuckDB updates in bulk, and recomputes route snapshots. Single-row DuckDB updates are forbidden in the hot path.

DuckDB persistence should prefer batch SQL transactions first. If profiling shows it is still a bottleneck, use DuckDB Appender or `COPY FROM`-style staging for larger batches.

## Rollback And Controls

Config:

```toml
[parser.adaptive]
enabled = true
auto_activate = true
min_samples = 1000
activation_wilson_lower_bound = 0.98
rollback_failed_ratio = 0.20
metrics_flush_count = 1000
metrics_flush_interval_ms = 100
max_fingerprints_per_scope_per_minute = 200
max_generic_line_bytes = 8192
max_generic_pairs = 64
```

Operational API:

- `GET /api/parser/adaptive/rules`
- `POST /api/parser/adaptive/rules/:id/enable`
- `POST /api/parser/adaptive/rules/:id/disable`
- `GET /api/parser/diagnostics`
- `GET /api/parser/profiles`

Emergency behavior:

- If `enabled = false`, only deterministic adapters run.
- If `auto_activate = false`, rules stay in `shadow`.
- If an active rule causes failure or partial rates to cross the rollback threshold, the rule becomes `disabled` and records `disabled_reason`.
- If a scope enters malformed-flood mode, adaptive rule generation pauses for that scope until the fingerprint cardinality window recovers.

## Implementation Boundaries

First implementation should stay backend-first:

- Refactor `crates/fwlog-adapter` into registry, diagnostics, adaptive mapper, and Sangfor adapter modules.
- Add `arc-swap`, `arrayvec`, and `memchr` if implementation profiling confirms they fit the route snapshot, bounded pair buffer, and delimiter scanning needs.
- Add storage tables and accessors in `crates/fwlog-storage`.
- Wire live ingest and historical import through the same engine.
- Add API endpoints for profiles, diagnostics, and adaptive rules.
- Add minimal UI only after backend behavior is tested.

Do not implement a full TOML/YAML rule language in the first pass. Leave room for a future `ConfigRuleAdapter`, but keep automatic rules in storage for now.

## Testing Strategy

Parser tests:

- Existing Sangfor tests keep passing with identical normalized fields.
- Parser registry tries adapters in precomputed route snapshot order.
- Adaptive router boosts the historically successful parser for a scope after the control task publishes a new snapshot.
- Hot route lookup does not allocate or sort per event.
- Generic extractor discovers unknown key/value fields.
- Generic extractor returns borrowed key/value slices and respects max-pair and max-line limits.
- Shadow rules collect samples without changing emitted events.
- Rules auto-activate only when sample count and Wilson lower-bound thresholds pass.
- Active rules fill only empty canonical fields.
- Active rules do not overwrite deterministic parser output.
- Bad active rules are disabled when rollback thresholds are crossed.
- Fingerprint cardinality flood groups noisy failures under `FINGERPRINT_MALFORMED_FLOOD`.
- `auto_activate = false` prevents shadow rules from becoming active.

Storage/API tests:

- Parser profile upsert and query work by scope.
- Metrics flush batches update parser profiles without single-row hot-path writes.
- Adaptive rule lifecycle supports shadow, active, and disabled.
- Diagnostics group similar failures by fingerprint.
- Diagnostics stop creating new fingerprints when per-scope cardinality exceeds the guard threshold.
- Rule enable/disable endpoints change only the selected rule.

Pipeline/import tests:

- Live ingest and historical import both use the same parser engine behavior.
- Parser adaptive state failure does not block event writes.
- Disabled adaptive mode preserves deterministic parsing.

## Migration Plan

1. Add `Partial` status and update storage/API serialization tests.
2. Refactor the adapter trait to return parse diagnostics.
3. Move Sangfor parsing behind the new registry without behavior changes.
4. Add static route snapshots and `arc-swap` publication.
5. Add batched parser metrics flush to a background control task.
6. Add profile storage and adaptive parser ordering through snapshot rebuilds.
7. Add bounded zero-copy generic KV extraction and shadow rule collection.
8. Add Wilson lower-bound activation for adaptive rules.
9. Add active adaptive rule application.
10. Add fingerprint cardinality guard and malformed-flood fallback.
11. Add rollback and manual enable/disable APIs.
12. Add UI/API visibility for profiles, rules, and diagnostics.

## Default Decisions

- Include `Partial` in normal event searches and unified search results.
- Keep `include_failed=false` scoped to failed rows only; it should not hide partial rows.
- Store adaptive rules in DuckDB first; add JSON export later only if disaster recovery needs it.
- Keep one truncated sample per fingerprint and preserve full raw only in the event row/frozen archive.
- Prefer `device:<device_id>` scope when device binding is available before parsing; otherwise use `source:<source_addr>` and let later backfill/profile repair improve scope quality.
- Keep all adaptive profile, rule, and diagnostic writes off the hot path through batched control events.
- Use Wilson lower bound as the activation confidence value.
