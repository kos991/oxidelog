# OxideLog Adaptive Parser Engine Design

## Goal

Refactor OxideLog parsing from the current three-layer engine into a deterministic, multi-vendor parser framework with automatic adaptive routing, field learning, diagnostics, and rollback controls.

The first implementation should improve two things:

- Multi-vendor extensibility: adding Huawei, H3C, Topsec, or new Sangfor variants should not require rewriting the engine.
- Parser observability: operators should know which parser matched, why parsing failed, and which adaptive rules were learned or disabled.

Automatic adaptation is in scope, but it must be guarded by confidence thresholds, shadow validation, and automatic rollback. The engine should become more useful over time without turning parsed events into black-box guesses.

The hot parse path must be designed as a high-throughput data plane. Per-event parsing must not sort adapters, allocate temporary routing vectors, update DuckDB, or contend on shared counters. Adaptive learning belongs on the control path and must feed the parser through precomputed, read-only snapshots.

## Current Context

Current parsing lives in `crates/fwlog-adapter`:

- `ParserEngine` already runs three layers: dedicated `LogAdapter`s, `GenericKvParser`, then `RuleBasedParser`.
- `SangforAdapter` is the default dedicated adapter.
- `RuleBasedParser` already supports `from_toml`, `from_rules`, and `ParserEngine::with_rules_toml`.
- Live ingest in `apps/fwlogd` and historical import in `apps/fwlog-import` both call `ParserEngine::new().parse(raw)`.
- `RawLog` has only `ingest_time`, `source_addr`, and `raw`; it does not carry `device_id`.
- `CanonicalEvent` currently has `Parsed` and `Failed` parse statuses.
- Storage and API already expose parser failure summaries and source/device visibility.

This design keeps the shared parser path for live and historical data. It does not introduce Logstash, Elasticsearch, Quickwit, or another external parser dependency. It also keeps the existing rule-based parser as a first-class layer instead of replacing it.

## Design Modes

The implementation can proceed in two modes:

- Compatibility mode: preserve the current public `ParserEngine::parse(RawLog) -> CanonicalEvent` surface and add route snapshots, diagnostics, and adaptive control-path state around it.
- Parser-kernel mode: accept breaking internal API changes and rebuild the parser data plane around a richer parse envelope, static parser tables, and zero-allocation output buffers.

Compatibility mode is the safer migration path. Parser-kernel mode is the aggressive target if the project is willing to update ingest, import, API cold-search reparsing, and tests together.

## Architecture

The parser engine will have four layers.

### Static Parser Layers

The engine preserves the existing layer order and adds metadata around it:

1. Dedicated adapters such as `SangforAdapter`.
2. `GenericKvParser` for common key/value logs.
3. `RuleBasedParser` for configured TOML/JSON rules.
4. Adaptive field mapper as a bounded control-plane-assisted fallback.

Each parser layer should eventually expose:

- `name`: stable parser name, such as `sangfor_nat_v1`.
- `vendor`: vendor name when known.
- `priority`: base ordering before adaptive boosts.
- `detect(raw)`: detection score and match reason.
- `parse(raw)`: parsed event plus diagnostics.

This is a breaking change from the current `LogAdapter` trait, which has `can_parse(&RawLog) -> bool` and `parse(RawLog) -> CanonicalEvent`. The first implementation should use a compatibility shim:

- Existing `can_parse=true` becomes a default positive `DetectOutcome`.
- Existing `can_parse=false` becomes a zero-score `DetectOutcome`.
- `SangforAdapter`, `GenericKvParser`, and `RuleBasedParser` keep their current parse behavior until each is migrated to richer diagnostics.

Future adapters should be isolated by vendor or log family rather than bundled into one large parser file.

### Parser-Kernel Mode

Parser-kernel mode replaces trait-object routing on the hot path with a static parser ABI. The goal is not dynamic plugin flexibility; it is predictable data-plane execution.

Core input type:

```rust
pub struct ParseEnvelope<'a> {
    pub ingest_time: chrono::DateTime<chrono::Utc>,
    pub source_addr: &'a str,
    pub device_hint: Option<&'a str>,
    pub raw: &'a str,
}
```

`RawLog` remains the owned boundary type used by ingress, spool, and storage. The parser converts borrowed references from `RawLog` into `ParseEnvelope` for hot-path parsing. If a future admission stage knows the device before parsing, it sets `device_hint`; otherwise `device_hint` is `None`.

Core output type:

```rust
pub struct ParseOutput<'a> {
    pub status: ParseStatus,
    pub parser_id: ParserId,
    pub fields: CanonicalFields<'a>,
    pub diagnostics: ParseDiagnostics<'a>,
    pub applied_rules: arrayvec::ArrayVec<RuleId, 8>,
}
```

`ParseOutput` borrows raw slices where possible. It is materialized into an owned `CanonicalEvent` only at the storage/API boundary. This makes "no owned `String` on the hot path" enforceable rather than aspirational.

Static parser ABI:

```rust
pub struct ParserKernel {
    pub parsers: &'static [ParserVTable],
    pub routes: arc_swap::ArcSwap<RouteSnapshot>,
}

pub struct ParserVTable {
    pub id: ParserId,
    pub name: &'static str,
    pub family: ParserFamily,
    pub detect: fn(ParseEnvelope<'_>) -> DetectOutcome,
    pub parse: fn(ParseEnvelope<'_>, &mut ParseScratch<'_>) -> ParseOutput<'_>,
}
```

`ParseScratch` is owned by a worker and reused across events. It contains bounded buffers for generic pairs, diagnostics, and temporary canonical fields. It is never shared across worker threads.

Parser-kernel mode changes the migration target:

- `LogAdapter` becomes a compatibility adapter, not the core ABI.
- `GenericKvParser` is rewritten as a scanner function that fills `ParseScratch`.
- `RuleBasedParser` remains supported, but configured regex rules are compiled into a cold/fallback parser family; they do not run before dedicated and generic static parsers unless route snapshots explicitly prioritize them for a source.
- `CanonicalEvent` creation moves to a final materialization function.

### Adaptive Router

The router records which parser succeeds for each normalized source scope:

- `source:<normalized_source>` is the only hot-path routing scope in the first implementation.

For each new log line, the router uses precomputed route groups that already include profile-derived boosts. This reduces blind parser attempts and makes multi-vendor support cheaper during live ingest and historical import.

The router must never sort adapters on the hot path. It must load an immutable route snapshot and walk fixed route groups in order. A high-confidence built-in parser should still win over an adaptive rule.

`device_id` is not available before parsing because `RawLog` does not contain it. If a parser extracts `CanonicalEvent.device_id`, or the pipeline binds a device after parsing, that identity is control-plane metadata only. It may be used for UI grouping and offline profile repair, but it must not be required for pre-parse routing unless a future ingest admission stage explicitly adds `device_id` to `RawLog`.

`source_addr` must be normalized before becoming a scope key. Transport ports are often ephemeral in UDP syslog traffic, so raw `ip:port` values would fragment profiles and prevent adaptive rules from reaching activation thresholds.

Default scope normalization:

- Parse URI-like values such as `udp://192.168.1.10:55123` and `tcp://127.0.0.1:1514`.
- Use `source_ip` mode by default: `source:udp://192.168.1.10`, dropping the peer port.
- Preserve protocol when known because TCP and UDP listeners can represent different ingest paths.
- Use `source:unknown` only when no host can be parsed.

Optional modes:

- `source_ip`: protocol plus host, no port.
- `source_ip_port`: protocol plus host plus port, mainly for TCP collectors with stable peer ports.
- `source_subnet`: protocol plus configured IPv4/IPv6 prefix, for high-cardinality NAT or relay sources.

High-entropy scope guard:

- Track raw-source cardinality per normalized scope.
- If one normalized scope receives more than `max_raw_sources_per_scope_per_minute` raw source variants, keep using the normalized scope but mark it `source_high_entropy`.
- If many normalized scopes individually have low volume and share the same subnet/vendor hints, the control path may aggregate profile learning into a `source_subnet` parent while keeping event storage untouched.
- Expire inactive source profiles after `scope_idle_ttl_seconds`.

Implementation shape:

- Keep adapter registry order stable.
- Build route snapshots on a background control task.
- Represent routes as fixed priority groups, for example `[Option<StaticRouteGroup>; 16]`.
- Store adapter ids in route groups and resolve them against a static registry; do not allocate boxed candidate vectors per event.
- Publish route snapshots with `arc-swap` so workers perform an atomic pointer load and then read immutable arrays.
- Treat the hot-path route lookup as O(1) with no per-event heap allocation.

In parser-kernel mode, route groups store `ParserId` values only. The worker resolves ids by indexing the static `ParserVTable` array, not by following boxed trait objects.

### Compatibility Diagnostics Channel

Compatibility mode keeps `ParserEngine::parse(RawLog) -> CanonicalEvent`, but diagnostics cannot disappear. The engine should add an internal method:

```rust
pub fn parse_inner(&self, raw: RawLog, scratch: &mut ParseScratch<'_>) -> ParseResult {
    // returns CanonicalEvent plus diagnostics/applied rule ids
}
```

`parse(raw)` becomes a wrapper that calls `parse_inner`, records diagnostics into the local metrics batch, and returns only the `CanonicalEvent`. This avoids adding diagnostics fields to `CanonicalEvent` while giving the control path the same metadata used by parser-kernel mode.

If `parse(raw)` is called outside a worker with no metrics batch installed, diagnostics are dropped after the returned event. Tests that need diagnostics should call `parse_inner`.

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

In the first implementation, learned rules are scoped by `source:<normalized_source>` and pass through `shadow` before becoming `active`. If later device binding is available before parsing, device-scoped rules can be added as an explicit extension.

The generic extractor must be defensive because it handles unknown or hostile input. It must not depend on complex regexes. It should use a bounded, single-pass byte scanner and SIMD-assisted delimiter search through `memchr` where useful.

Long line handling:

- If a raw line is longer than `max_generic_line_bytes`, the generic extractor scans only a safe prefix.
- The safe prefix must end at the last complete field delimiter boundary before the limit, for example whitespace, comma, semicolon, or pipe after a complete value.
- If no safe boundary exists before the limit, skip generic extraction for that line and emit a `line_too_long_no_safe_boundary` diagnostic.
- Never create shadow rules from a truncated prefix unless the extractor can prove every emitted pair ended before a safe boundary.
- Record `line_truncated=true` in diagnostics when a safe prefix was used.

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
- `suggested_rule_min_samples = 100`
- `suggested_rule_wilson_lower_bound = 0.90`

These values should be configurable.

The confidence gate must not use raw observed success ratio alone. Rule activation should use the lower bound of the Wilson score interval so small, temporarily homogeneous samples cannot activate brittle rules. The stored `confidence` field should represent the Wilson lower bound used for activation, with raw wins and samples available for audit.

Shadow validation means applying candidate rules to sampled raw lines in the control path using the same field-application function that active rules use. The emitted production event is not mutated. The simulation records:

- original status and fields from the production parse.
- simulated status and fields after candidate application.
- fields filled.
- conflicts with deterministic fields.
- conversion failures.
- candidate rule ids involved.

A shadow rule counts as a win only when the simulated event improves status or fills missing canonical fields without conflicts or conversion failures.

Suggested rule generation:

- The generic extractor reports unknown raw keys to the control path.
- The control path groups unknown keys by normalized scope, normalized raw key, inferred value type, and line shape.
- When an unknown key reaches `suggested_rule_min_samples` and Wilson lower bound for its inferred value type reaches `suggested_rule_wilson_lower_bound`, the control manager creates or updates one provisional adaptive rule id.
- `parser_diagnostics.suggested_rule_id` points to that provisional rule when a failure fingerprint is mostly explained by the unknown key.
- Suggested rules start as `shadow`; they are not operator-authored annotations.
- Duplicate suggestions are deduplicated by `(scope_key, raw_key, canonical_field, value_type)`.

## Hot Path Performance Contract

The live parse path is the data plane. It must obey these rules:

- No per-event adapter sorting.
- No per-event heap allocation for route candidates.
- No per-event DuckDB writes for parser profiles, adaptive rules, or diagnostics.
- No shared atomic counter increments for high-cardinality success/failure metrics.
- No complex regexes in the generic extractor.
- No owned `String` copies for generic extracted keys and values unless the event is being materialized for storage.
- No trait-object dispatch in parser-kernel mode; dispatch is static vtable function pointers by `ParserId`.
- No per-event `RawLog` clone in parser-kernel mode.

Adaptive state changes are the control plane. A background manager consumes batched events, updates in-memory adaptive state, periodically checkpoints state, recomputes static route snapshots, and publishes them atomically.

## Data Model

Parser adaptive state should live outside the hot event rows to avoid slowing normal event queries. Runtime state is held in the control manager's in-memory maps. DuckDB is used only for periodic checkpoint/snapshot persistence and API reads, not for high-frequency point updates.

### `parser_profiles`

Tracks parser success and failure by scope.

Columns:

- `scope_key TEXT`
- `parser_name TEXT`
- `success_count BIGINT`
- `fail_count BIGINT`
- `last_seen TIMESTAMPTZ`
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
- `created_at TIMESTAMPTZ NOT NULL`
- `activated_at TIMESTAMPTZ`
- `disabled_at TIMESTAMPTZ`
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
- `last_seen TIMESTAMPTZ NOT NULL`

Fingerprints must be deterministic and explicitly scoped. The fingerprint input is:

- `scope_key`.
- parser layer or rule name that failed.
- vendor hint when available.
- error class.
- sorted canonical missing-field set.
- sorted normalized raw-key set, capped to `max_generic_pairs`.
- coarse line shape, such as `kv`, `syslog_kv`, `arrow_rule`, or `unknown`.

Values, URLs, and raw payload substrings are excluded. This prevents random payload values from exploding cardinality, while `scope_key`, parser layer, and vendor hint avoid incorrectly merging unrelated vendors that happen to share key names.

Fingerprint generation must be cardinality-guarded. For each scope, the control path should track recent fingerprint growth with a sliding window Bloom filter, count-min sketch, or equivalent bounded structure. If one scope creates more than the configured number of distinct fingerprints in a minute, the scope enters malformed-flood mode:

- Stop creating precise fingerprints for that scope during the window.
- Stop generating new shadow adaptive rules for that scope.
- Group subsequent failures under `FINGERPRINT_MALFORMED_FLOOD`.
- Keep parsing deterministic adapters normally.

Default guard:

- `max_fingerprints_per_scope_per_minute = 200`
- `flood_recovery_seconds = 300`

Malformed-flood recovery:

- Flood mode is windowed, not permanent.
- The scope leaves flood mode after `flood_recovery_seconds` if distinct fingerprint growth stays below half the threshold during the recovery window.
- On recovery, precise fingerprints resume, but new shadow rule generation stays in cool-down for one additional recovery window.
- Entering and leaving flood mode emits parser diagnostics and should be visible through `/api/parser/diagnostics`.

## Parse Status

Add a third status to `ParseStatus`:

- `Parsed`: the event has the minimum searchable tuple for its parser family.
- `Partial`: the event has useful canonical fields, but the minimum searchable tuple is incomplete.
- `Failed`: no useful canonical event can be produced.

Default minimum searchable tuple for firewall/NAT logs:

- `src_ip`
- `dst_ip`
- at least one of `action` or `protocol`

If `src_ip` and `dst_ip` exist but both action and protocol are missing, the event is `Partial`. If only action/protocol exists without usable endpoint fields, the event is also `Partial`. This resolves the distinction: `Partial` is not "non-critical fields are missing"; it is "some useful fields exist, but the parser-family minimum tuple is incomplete."

Existing UI/API filters should treat `Partial` as searchable event data by default. Status counts must show partial separately from fully parsed and failed rows.

## Parse Flow

1. Receive `RawLog`.
2. Normalize `source_addr` and resolve hot routing scope from `normalized_source`.
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
13. The control task updates in-memory profiles, diagnostics, and shadow rule samples in bulk.
14. Background evaluation promotes eligible shadow rules to active.
15. The control task rebuilds and publishes route snapshots with `arc-swap`.
16. Rollback evaluation disables active rules if the scope failure ratio rises after activation.

## Control Path Batching

Parser profile and diagnostics updates must be asynchronous. Workers should keep local non-atomic counters or thread-local batches keyed by scope and parser. A worker flushes a `MetricsFlushEvent` when either threshold is reached:

- `metrics_flush_count = 1000`
- `metrics_flush_interval_ms = 100`

The background control task receives flush events through an MPSC channel or bounded ring buffer. It merges batches into in-memory maps and recomputes route snapshots. Single-row DuckDB updates are forbidden in both the hot path and steady-state control loop.

Checkpoint persistence should run on an interval, for example every 30 seconds or on graceful shutdown. DuckDB checkpoint writes should be snapshot-style batch transactions: replace or merge compact aggregate rows, not append one row per parser event. If this still contends with analytical queries, move adaptive state persistence to SQLite while leaving DuckDB as the event analytics store.

Parser-kernel mode may use a dedicated control-state store from the start:

- Runtime truth: in-memory control manager.
- Fast durable checkpoint: SQLite or a compact binary snapshot under the data directory.
- Analytics mirror: DuckDB tables refreshed from checkpoints for API/reporting.

This separates OLTP-like adaptive state from DuckDB's columnar event analytics role.

## Scope Repair

Because `RawLog` has no `device_id`, the first routing key is always `source:<normalized_source>`. When later parsing or pipeline binding discovers `device_id`, the control path records a source-to-device alias:

- `source_key`
- `raw_source_addr`
- `device_id`
- `first_seen TIMESTAMPTZ`
- `last_seen TIMESTAMPTZ`
- `confidence`

This alias is used for UI grouping and offline reporting. It does not automatically move hot routing to `device:<device_id>`.

If a future ingest admission stage adds `device_id` before parsing, profile repair follows these rules:

- Source profiles may be copied into a new device aggregate by summing success/fail counts.
- Adaptive rules are not blindly moved; they are revalidated in shadow mode under the device scope.
- If multiple sources map to one device with conflicting active rules, the device scope starts with deterministic routes only until shadow validation re-activates safe rules.

## Rollback And Controls

Config:

```toml
[parser]
mode = "compat" # compat | kernel

[parser.adaptive]
enabled = true
auto_activate = true
min_samples = 1000
activation_wilson_lower_bound = 0.98
rollback_failed_ratio = 0.20
metrics_flush_count = 1000
metrics_flush_interval_ms = 100
max_fingerprints_per_scope_per_minute = 200
flood_recovery_seconds = 300
quarantine_recovery_seconds = 600
max_raw_sources_per_scope_per_minute = 1000
scope_idle_ttl_seconds = 86400
scope_normalization = "source_ip" # source_ip | source_ip_port | source_subnet
scope_subnet_v4_prefix = 24
scope_subnet_v6_prefix = 64
max_generic_line_bytes = 8192
max_generic_pairs = 64
suggested_rule_min_samples = 100
suggested_rule_wilson_lower_bound = 0.90
checkpoint_interval_seconds = 30
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
- If a scope enters adaptive quarantine, all adaptive active rules stop applying for that scope unless an operator explicitly pins a rule as allowed.

Rollback attribution is per rule, not just per scope. Each emitted metrics event must include the active adaptive rule ids applied during parsing. The control manager keeps per-rule post-activation counters:

- events where the rule was applied.
- parsed, partial, and failed outcomes.
- conflicts and conversion failures.

If several rules are applied to the same event, the control manager first disables rules with direct conflicts or conversion failures. If only aggregate failure rate rises and attribution is ambiguous, the scope enters adaptive quarantine: all adaptive active rules stop applying for that scope, and individual rules are not permanently disabled until shadow replay isolates the failing rule or operator action confirms it.

Adaptive quarantine recovery:

- Quarantine lasts at least `quarantine_recovery_seconds`.
- During quarantine, deterministic parsers still run and shadow replay continues in the control path.
- The scope leaves quarantine only when shadow replay identifies safe rules whose Wilson lower bound still meets activation thresholds and the deterministic failure ratio has returned below rollback threshold.
- If no safe rule is isolated, quarantine remains active and is visible through parser diagnostics/API until an operator disables or re-enables rules manually.

## Implementation Boundaries

First implementation should stay backend-first:

- Refactor `crates/fwlog-adapter` into registry, diagnostics, adaptive mapper, and Sangfor adapter modules.
- Keep the existing `RuleBasedParser` and `with_rules_toml` API; adaptive rules are a separate runtime feature, not a replacement for configured TOML rules.
- Add `arc-swap`, `arrayvec`, and `memchr` if implementation profiling confirms they fit the route snapshot, bounded pair buffer, and delimiter scanning needs.
- Add storage tables and accessors in `crates/fwlog-storage`.
- Wire live ingest and historical import through the same engine.
- Add API endpoints for profiles, diagnostics, and adaptive rules.
- Add minimal UI only after backend behavior is tested.

Do not remove or downgrade the existing TOML rule language. Automatic adaptive rules are stored and evaluated separately from `RuleBasedParser`; configured TOML rules remain deterministic parser inputs.

If parser-kernel mode is selected, the implementation boundary changes:

- Update `fwlog-domain` with `ParseEnvelope`, `ParseOutput`, `CanonicalFields`, and materialization helpers.
- Update live ingest, historical import, and cold archive search reparsing to call the kernel API.
- Keep `ParserEngine::parse(RawLog)` as a wrapper for tests and compatibility until callers migrate.
- Add a feature flag or config gate so production can fall back to compatibility mode during rollout.

## Testing Strategy

Parser tests:

- Existing Sangfor tests keep passing with identical normalized fields.
- Existing `GenericKvParser` and `RuleBasedParser` behavior remains compatible unless explicitly changed by a migration test.
- Parser registry tries adapters in precomputed route snapshot order.
- Scope normalization drops ephemeral ports by default and prevents raw source cardinality from fragmenting profiles.
- High-entropy source scopes are marked and may aggregate learning into a subnet parent.
- Adaptive router boosts the historically successful parser for a scope after the control task publishes a new snapshot.
- Hot route lookup does not allocate or sort per event.
- Generic extractor discovers unknown key/value fields.
- Generic extractor returns borrowed key/value slices and respects max-pair and max-line limits.
- Generic extractor handles lines over `max_generic_line_bytes` only at safe pair boundaries and does not generate shadow rules from incomplete pairs.
- Shadow rules collect samples without changing emitted events.
- Shadow validation uses the same application function as active rules and records conflicts/conversion failures.
- Unknown raw keys generate deduplicated provisional `suggested_rule_id` values only after sample and Wilson thresholds pass.
- Rules auto-activate only when sample count and Wilson lower-bound thresholds pass.
- Active rules fill only empty canonical fields.
- Active rules do not overwrite deterministic parser output.
- Directly attributed bad active rules are disabled when rollback thresholds are crossed.
- Fingerprint cardinality flood groups noisy failures under `FINGERPRINT_MALFORMED_FLOOD`.
- Malformed-flood mode recovers after the configured recovery window and emits diagnostics on enter/exit.
- `auto_activate = false` prevents shadow rules from becoming active.
- Compatibility mode exposes diagnostics through `parse_inner` while `parse` still returns `CanonicalEvent`.
- Parser-kernel mode parses from borrowed `ParseEnvelope` without cloning `RawLog`.
- Parser-kernel materialization produces the same `CanonicalEvent` as compatibility mode for existing Sangfor, generic KV, and TOML rule fixtures.

Storage/API tests:

- Parser profile aggregate update and query work by scope.
- Metrics flush batches update in-memory parser profiles without single-row DuckDB writes.
- Periodic checkpoint writes durable adaptive state with `TIMESTAMPTZ` fields.
- Adaptive rule lifecycle supports shadow, active, and disabled.
- Diagnostics group similar failures by fingerprint.
- Diagnostics stop creating new fingerprints when per-scope cardinality exceeds the guard threshold.
- Rule enable/disable endpoints change only the selected rule.
- Ambiguous rollback moves a scope into adaptive quarantine instead of randomly disabling one of several active rules.
- Quarantine disables adaptive rule application for the scope and recovers only after shadow replay identifies safe rules.

Pipeline/import tests:

- Live ingest and historical import both use the same parser engine behavior.
- Parser adaptive state failure does not block event writes.
- Disabled adaptive mode preserves deterministic parsing.
- Parser-kernel mode and compatibility mode produce equivalent stored events for the same sample corpus.

## Migration Plan

Compatibility sequence:

1. Add `Partial` status and update storage/API serialization tests.
2. Add a compatibility detection shim around `can_parse` for `SangforAdapter`, `GenericKvParser`, and `RuleBasedParser`.
3. Add `parse_inner` diagnostics channel while keeping `parse` as a `CanonicalEvent` wrapper.
4. Add source normalization and high-entropy scope guards.
5. Refactor parser diagnostics without breaking existing parse behavior.
6. Add static route snapshots and `arc-swap` publication using normalized source scopes.
7. Add batched parser metrics flush to a background control task.
8. Add in-memory profile state and periodic checkpoint persistence.
9. Add bounded zero-copy generic KV extraction, safe long-line truncation, and shadow rule collection.
10. Add suggested rule generation for high-confidence unknown keys.
11. Add shadow simulation using the same rule application function as active rules.
12. Add Wilson lower-bound activation for adaptive rules.
13. Add active adaptive rule application with applied-rule attribution.
14. Add fingerprint cardinality guard, malformed-flood fallback, and recovery windows.
15. Add adaptive quarantine, recovery, rollback, and manual enable/disable APIs.
16. Add source-to-device alias reporting without using `device_id` for pre-parse hot routing.
17. Add UI/API visibility for profiles, rules, and diagnostics.

Parser-kernel sequence:

1. Add borrowed `ParseEnvelope`, `ParseOutput`, and `ParseScratch`.
2. Implement `CanonicalEvent::from_parse_output` or equivalent materialization.
3. Port `SangforAdapter` into a static parser function while keeping the old adapter wrapper.
4. Rewrite `GenericKvParser` as the bounded byte scanner.
5. Wrap `RuleBasedParser` as a cold parser family.
6. Add static `ParserVTable` and `ParserId` route snapshots.
7. Migrate `ParserEngine::parse` to call the kernel internally.
8. Migrate live ingest and historical import to borrowed kernel calls where possible.
9. Run corpus equivalence tests between compatibility and kernel modes.
10. Make kernel mode the default only after equivalence and throughput tests pass.

## Default Decisions

- Include `Partial` in normal event searches and unified search results.
- Keep `include_failed=false` scoped to failed rows only; it should not hide partial rows.
- Keep runtime adaptive state in memory and checkpoint it periodically; DuckDB is not the high-frequency adaptive-state update engine.
- Keep one truncated sample per fingerprint and preserve full raw only in the event row/frozen archive.
- Use normalized `source:<normalized_source>` as the first hot routing scope because `RawLog` has no `device_id`.
- Treat discovered `device_id` as control-plane alias/reporting metadata until an explicit ingest-stage device id exists.
- Keep all adaptive profile, rule, and diagnostic writes off the hot path through batched control events.
- Use Wilson lower bound as the activation confidence value.
- Use sliding-window recovery for malformed-flood and quarantine states; neither is permanent without continuing evidence.
- Generate `suggested_rule_id` automatically from high-confidence unknown keys, never as a manual free-text annotation.
- Keep compatibility mode available until parser-kernel mode passes corpus equivalence and production smoke tests.
