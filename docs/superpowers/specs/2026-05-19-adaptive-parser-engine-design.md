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

Bounded output behavior:

- `applied_rules` uses a fixed `ArrayVec<RuleId, 8>`.
- If more than eight active rules apply, keep the first eight in deterministic route order, set `applied_rules_truncated=true` in diagnostics, and continue parsing.
- When `applied_rules_truncated=true`, rollback attribution for that event is incomplete. The control manager may use the event for aggregate scope health, but it must not auto-disable an omitted individual rule based on that event.
- Overflow must never panic or allocate on the hot path.

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
- If no host can be parsed, use `source:unknown:<hash_prefix>` where `hash_prefix` is a short SHA-256 prefix of the raw `source_addr`.
- `source:unknown:<hash_prefix>` scopes run deterministic parsers but do not generate adaptive shadow rules until a stable host/device alias is discovered.

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
- Apply operator-pinned parsers before adaptive boosts. Pinned parser ids and rule names go into the highest route group for their configured source scope or globally.

In parser-kernel mode, route groups store `ParserId` values only. The worker resolves ids by indexing the static `ParserVTable` array, not by following boxed trait objects.

### Compatibility Diagnostics Channel

Compatibility mode keeps `ParserEngine::parse(RawLog) -> CanonicalEvent`, but diagnostics cannot disappear. The engine should add an internal method:

```rust
pub fn parse_inner(&self, raw: RawLog, diagnostics: &mut ParseDiagnosticsBuffer) -> ParseResult {
    // returns CanonicalEvent plus diagnostics/applied rule ids
}
```

`ParseDiagnosticsBuffer` is a lightweight compatibility-mode structure, not `ParseScratch`. It stores parser name, detect outcome, parse status, errors, inferred fields, and applied adaptive rule ids. `ParseScratch` belongs to parser-kernel mode and is introduced later with the zero-copy scanner.

`parse(raw)` becomes a wrapper that calls `parse_inner`, records diagnostics into the local metrics batch, and returns only the `CanonicalEvent`. This avoids adding diagnostics fields to `CanonicalEvent` while giving the control path metadata equivalent to parser-kernel mode. `parse_inner` consumes `RawLog` in compatibility mode so the wrapper does not need an extra clone, but compatibility mode still materializes owned `CanonicalEvent` values; the strict no-clone/no-owned-field contract applies to parser-kernel mode.

If `parse(raw)` is called outside a worker with no metrics batch installed, diagnostics are dropped after the returned event. Tests that need diagnostics should call `parse_inner`.

### Adaptive Field Mapper

When static parser families fail or produce incomplete output, the adaptive field mapper's generic extractor scans the raw line for structured fields:

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

In the first implementation, learned rules are scoped by `source:<normalized_source>` and pass through `shadow` before becoming `active`. `source:unknown:<hash_prefix>` buckets are the exception: they can collect deterministic parser profile counters and diagnostics, but they cannot create shadow rules or activate adaptive rules until the control path discovers a stable host/device alias. If later device binding is available before parsing, device-scoped rules can be added as an explicit extension.

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

Pair overflow behavior:

- `GenericPairs` must use `try_push` or equivalent checked insertion.
- When `max_generic_pairs` is reached, stop adding pairs, set `pairs_truncated=true` in diagnostics, and continue parsing with the complete pairs already collected.
- The extractor may continue scanning only for safe boundary accounting, but it must not emit more pairs or create shadow rules from omitted pairs.
- Pair overflow must never panic or allocate on the hot path.

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

Tracks parser parsed, partial, and failed outcomes by scope.

Columns:

- `scope_key TEXT`
- `parser_id TEXT`
- `parser_name TEXT`
- `success_count BIGINT`
- `partial_count BIGINT`
- `fail_count BIGINT`
- `last_seen TIMESTAMPTZ`
- `priority_boost DOUBLE`

Primary key: `(scope_key, parser_id)`. `parser_name` is display metadata and should remain stable, but route snapshots and pinned parser references use `parser_id`.

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
- `recovery_sample_rate DOUBLE`
- `recovery_attempts BIGINT`
- `last_recovery_at TIMESTAMPTZ`

Supported status values:

- `shadow`
- `shadow_recovering`
- `active`
- `disabled`

Rules are unique by `(scope_key, raw_key, canonical_field)`. `confidence` stores the Wilson lower bound, not the raw observed match ratio. `shadow_recovering` is a persisted lifecycle state used only during staged quarantine recovery; it is distinct from normal `shadow` learning. `recovery_sample_rate` is used only for `shadow_recovering` rules and is `NULL` or `0` for ordinary `shadow`, `active`, and `disabled` rules.

### `parser_scope_state`

Stores scope-level adaptive controls that cannot be represented by parser profiles or individual rules.

Columns:

- `scope_key TEXT PRIMARY KEY`
- `source_high_entropy BOOLEAN NOT NULL`
- `adaptive_learning_enabled BOOLEAN NOT NULL`
- `unknown_source_bucket BOOLEAN NOT NULL`
- `metrics_gap BOOLEAN NOT NULL`
- `metrics_gap_since TIMESTAMPTZ`
- `malformed_flood_until TIMESTAMPTZ`
- `shadow_rule_cooldown_until TIMESTAMPTZ`
- `adaptive_quarantine_until TIMESTAMPTZ`
- `quarantine_backoff_seconds BIGINT NOT NULL`
- `quarantine_attempts BIGINT NOT NULL`
- `last_state_change TIMESTAMPTZ NOT NULL`
- `last_seen TIMESTAMPTZ NOT NULL`

`parser_scope_state` is the API-visible home for high-entropy scopes, unknown buckets, malformed-flood mode, metrics gaps, adaptive quarantine, and quarantine backoff. `source:unknown:<hash_prefix>` scopes set `unknown_source_bucket=true` and `adaptive_learning_enabled=false` until alias repair enables learning for a more specific scope.

### `parser_diagnostics`

Groups parse failures, partial parses, and adaptive decisions.

Columns:

- `fingerprint TEXT PRIMARY KEY`
- `scope_key TEXT`
- `reason TEXT NOT NULL`
- `sample_raw TEXT`
- `sample_raw_truncated BOOLEAN NOT NULL`
- `count BIGINT NOT NULL`
- `suggested_rule_id TEXT`
- `last_seen TIMESTAMPTZ NOT NULL`

`sample_raw` is a bounded diagnostic sample, not a full archival copy of the log line. It should be truncated to a configured diagnostic sample limit before checkpointing, with `sample_raw_truncated=true` when truncation occurs.

### `source_device_aliases`

Records source-to-device relationships discovered after parsing or pipeline binding.

Columns:

- `source_key TEXT`
- `raw_source_addr TEXT`
- `device_id TEXT`
- `first_seen TIMESTAMPTZ`
- `last_seen TIMESTAMPTZ`
- `confidence DOUBLE`

Primary key: `(source_key, raw_source_addr, device_id)`.

### `parser_checkpoint_version`

Publishes complete adaptive-state snapshots to API readers.

Columns:

- `snapshot_version BIGINT PRIMARY KEY`
- `created_at TIMESTAMPTZ NOT NULL`
- `published_at TIMESTAMPTZ`
- `status TEXT NOT NULL`
- `profiles_count BIGINT NOT NULL`
- `rules_count BIGINT NOT NULL`
- `diagnostics_count BIGINT NOT NULL`
- `scope_state_count BIGINT NOT NULL`
- `aliases_count BIGINT NOT NULL`

Supported status values are `pending`, `published`, and `failed`. When versioned checkpoint tables are used, each checkpointed adaptive table includes `snapshot_version` and its primary key is extended with `snapshot_version`. API readers first load the latest published version from `parser_checkpoint_version`, then read all adaptive tables using that version. If the first implementation uses stable tables with `MERGE` instead, `parser_checkpoint_version` still records the last durable checkpoint and row counts for observability.

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
- `source:unknown:<hash_prefix>` scopes can enter flood mode, but flood mode for one unknown bucket must not affect other unknown buckets.

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
5. Run bounded detection and parsing for static parser families: dedicated adapters, `GenericKvParser`, and `RuleBasedParser`.
6. If a static parser family returns `Parsed`, emit the event and append a compact success record to the local metrics batch. Active adaptive rules do not mutate already parsed deterministic output in the first implementation.
7. Optionally enqueue sampled shadow-validation work to the control path; do not run unbounded adaptive validation inline.
8. If the static parser families fail or return `Partial`, run the adaptive field mapper's bounded extractor. This extractor is distinct from the deterministic `GenericKvParser` layer: it produces borrowed `GenericPair` observations for diagnostics and adaptive-rule learning, and it does not replace configured TOML rules.
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

Backpressure policy:

- The metrics channel is bounded.
- Workers must use non-blocking send.
- If the channel is full, the worker keeps a compact overflow aggregate in local memory by `(scope_key, parser_id, status)` and increments `parser_metrics_dropped_batches`.
- If the local overflow aggregate exceeds `metrics_overflow_max_entries`, the worker drops the lowest-priority entries in this order: shadow-validation samples, detailed diagnostics, then aggregate profile deltas.
- Aggregate success/partial/fail counters are preserved longer than diagnostic samples because they keep route profiles roughly correct.
- Every drop path increments visible runtime counters and emits a rate-limited warning so adaptive blind spots are observable.
- The control manager records `metrics_gap=true` for scopes affected by drops; rules cannot auto-activate from windows that include metrics gaps.

Checkpoint persistence should run on an interval, for example every 30 seconds or on graceful shutdown. DuckDB checkpoint writes should use staging tables plus an atomic publish marker:

1. Write the next snapshot into `parser_profiles_staging`, `adaptive_field_rules_staging`, `parser_scope_state_staging`, `parser_diagnostics_staging`, and `source_device_aliases_staging` inside one transaction.
2. Validate row counts and snapshot metadata.
3. Commit by updating a single `parser_checkpoint_version` table to point API readers at the new snapshot version.
4. Keep the previous snapshot readable until the new version is committed.
5. Garbage-collect old snapshot rows after readers no longer need them.

API queries must either see the previous complete snapshot or the new complete snapshot, never an empty or partial replacement. If staging/versioned reads are too much for the first implementation, use `MERGE`/UPSERT into stable tables and avoid `DELETE + INSERT` windows. If this still contends with analytical queries, move adaptive state persistence to SQLite while leaving DuckDB as the event analytics store.

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

- Source profiles may be copied into a new device aggregate by summing parsed, partial, and failed counts.
- Adaptive rules are not blindly moved; they are revalidated in shadow mode under the device scope.
- If multiple sources map to one device with conflicting active rules, the device scope starts with deterministic routes only until shadow validation re-activates safe rules.

## Rollback And Controls

Config:

```toml
[parser]
mode = "compat" # compat | kernel
pinned_parsers = [] # optional parser ids/rule names, highest priority globally

[[parser.pinned_scope]]
scope = "source:udp://192.168.1.10"
parsers = ["rule:CriticalCustomRule", "sangfor_nat_v1"]

[parser.adaptive]
enabled = true
auto_activate = true
min_samples = 1000
activation_wilson_lower_bound = 0.98
rollback_failed_ratio = 0.20
metrics_flush_count = 1000
metrics_flush_interval_ms = 100
metrics_channel_capacity = 1024
metrics_overflow_max_entries = 4096
max_fingerprints_per_scope_per_minute = 200
flood_recovery_seconds = 300
quarantine_recovery_seconds = 600
quarantine_max_seconds = 3600
quarantine_max_seconds_cap = 86400
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
- `GET /api/parser/scopes`

Parser and rule identifiers:

- Built-in and kernel parsers use stable `parser_id` values such as `parser:sangfor_nat_v1`.
- Configured TOML rules use `rule:<ruleset_id>:<rule_name>` when a ruleset id exists, or `rule:default:<rule_name>` for single-file compatibility.
- Rule names must be unique within a ruleset. If duplicate names are loaded, the engine must reject the ruleset or require explicit generated ids before the rules can be pinned.
- `pinned_parsers` and `parser.pinned_scope.parsers` accept only these fully qualified ids.

Emergency behavior:

- If `enabled = false`, only deterministic adapters run.
- If `auto_activate = false`, rules stay in `shadow`.
- If an active rule causes failure or partial rates to cross the rollback threshold, the rule becomes `disabled` and records `disabled_reason`.
- If a scope enters malformed-flood mode, adaptive rule generation pauses for that scope until the fingerprint cardinality window recovers.
- If a scope enters adaptive quarantine, all adaptive active rules stop applying for that scope unless an operator explicitly pins a rule as allowed.

Rollback attribution is per rule, not just per scope. Each emitted metrics event must include the active adaptive rule ids applied during parsing. If `applied_rules_truncated=true`, per-rule attribution for omitted rules is treated as unknown and the affected window cannot auto-disable individual omitted rules. The control manager keeps per-rule post-activation counters:

- events where the rule was applied.
- parsed, partial, and failed outcomes.
- conflicts and conversion failures.

If several rules are applied to the same event, the control manager first disables rules with direct conflicts or conversion failures. If only aggregate failure rate rises and attribution is ambiguous, the scope enters adaptive quarantine: all adaptive active rules stop applying for that scope, and individual rules are not permanently disabled until shadow replay isolates the failing rule or operator action confirms it.

Adaptive quarantine recovery:

- Quarantine lasts at least `quarantine_recovery_seconds`.
- During quarantine, deterministic parsers still run and shadow replay continues in the control path.
- Recovery is staged by rule, not all-or-nothing.
- A safe rule may re-enter `shadow_recovering` when shadow replay shows it improves parse output and has no direct conflicts, even if the deterministic failure ratio remains high because the device is producing bad logs.
- `shadow_recovering` rules apply to a small sampled fraction of events first; if post-application failures do not rise, the control manager gradually increases the fraction and then restores `active`.
- Deterministic failure ratio is a health signal, not a hard gate for every rule's recovery.
- If quarantine lasts longer than `quarantine_max_seconds`, the control manager performs one cautious staged recovery attempt for safe rules and keeps enhanced monitoring enabled.
- If no safe rule is isolated after the max window, quarantine remains active and is visible through parser diagnostics/API until an operator disables or re-enables rules manually.
- If a staged recovery attempt causes failure or partial rates to rise again, the scope re-enters quarantine immediately.
- Repeated quarantine uses exponential backoff for `quarantine_max_seconds`, doubling after each failed recovery attempt up to `quarantine_max_seconds_cap = 86400`.
- A successful recovery resets the backoff counter.

## Implementation Boundaries

First implementation should stay backend-first:

- Refactor `crates/fwlog-adapter` into registry, diagnostics, adaptive mapper, and Sangfor adapter modules.
- Keep the existing `RuleBasedParser` and `with_rules_toml` API; adaptive rules are a separate runtime feature, not a replacement for configured TOML rules.
- Preserve configured rule priority semantics by default. A TOML rule with a lower `priority` value must outrank higher-numbered TOML rules within the rule-based parser family, and operators can promote selected rules above generic/dedicated families through `pinned_parsers` or `parser.pinned_scope`.
- Document rule priority clearly: default family order is dedicated adapters, generic KV, configured rules, then adaptive fallback. `ParseRule.priority` orders rules only inside `RuleBasedParser`; operators must use `pinned_parsers` or `parser.pinned_scope` to run a configured rule before generic or dedicated parsers.
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
- `source:unknown:<hash_prefix>` buckets collect deterministic diagnostics but do not create shadow rules or apply adaptive rules.
- High-entropy source scopes are marked and may aggregate learning into a subnet parent.
- Pinned parsers and fully qualified pinned TOML rule ids appear in the highest route group and preserve operator intent.
- Adaptive router boosts the historically successful parser for a scope after the control task publishes a new snapshot.
- Hot route lookup does not allocate or sort per event.
- Generic extractor discovers unknown key/value fields.
- Generic extractor returns borrowed key/value slices and respects max-pair and max-line limits.
- Generic extractor handles lines over `max_generic_line_bytes` only at safe pair boundaries and does not generate shadow rules from incomplete pairs.
- Generic extractor stops safely at `max_generic_pairs`, sets `pairs_truncated=true`, and never panics when the bounded pair buffer is full.
- `ParseOutput.applied_rules` truncates safely with `applied_rules_truncated=true` when more than eight rules apply.
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
- Full metrics channels drop lower-priority control data first, increment visible drop counters, and prevent auto-activation from affected windows.
- Periodic checkpoint writes durable adaptive state with `TIMESTAMPTZ` fields.
- Checkpoint publication gives API readers either the old complete snapshot or the new complete snapshot, never a partial replacement.
- Adaptive rule lifecycle supports shadow, shadow_recovering, active, and disabled.
- Adaptive rule lifecycle persists `shadow_recovering` separately from ordinary `shadow`.
- Scope state persists `metrics_gap`, high-entropy flags, malformed-flood windows, quarantine windows, and quarantine backoff.
- Checkpoint publication records `parser_checkpoint_version` row counts and API reads use one complete published snapshot.
- Source-to-device aliases are checkpointed and queryable without changing hot routing.
- Diagnostics group similar failures by fingerprint.
- Diagnostics stop creating new fingerprints when per-scope cardinality exceeds the guard threshold.
- Rule enable/disable endpoints change only the selected rule.
- Ambiguous rollback moves a scope into adaptive quarantine instead of randomly disabling one of several active rules.
- Quarantine disables adaptive rule application for the scope, then recovers safe rules through staged `shadow_recovering` even when deterministic failure ratio remains high.
- Failed staged recovery re-enters quarantine with exponential backoff up to the configured cap.

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
5. Add pinned parser/rule support and route snapshot ordering tests.
6. Refactor parser diagnostics without breaking existing parse behavior.
7. Add static route snapshots and `arc-swap` publication using normalized source scopes.
8. Add batched parser metrics flush to a background control task with bounded-channel backpressure/drop accounting.
9. Add in-memory profile state, scope state, source-device aliases, and consistent checkpoint publication.
10. Add bounded zero-copy generic KV extraction, safe long-line/pair truncation, and shadow rule collection.
11. Add suggested rule generation for high-confidence unknown keys.
12. Add shadow simulation using the same rule application function as active rules.
13. Add Wilson lower-bound activation for adaptive rules.
14. Add active adaptive rule application with applied-rule attribution.
15. Add fingerprint cardinality guard, malformed-flood fallback, and recovery windows.
16. Add adaptive quarantine, persisted `shadow_recovering`, staged recovery, rollback backoff, and manual enable/disable APIs.
17. Add source-to-device alias reporting without using `device_id` for pre-parse hot routing.
18. Add UI/API visibility for profiles, rules, and diagnostics.

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
- Publish checkpoints with staging/version markers or MERGE semantics so parser APIs never observe partial checkpoint state.
- Keep one truncated sample per fingerprint and preserve full raw only in the event row/frozen archive.
- Use normalized `source:<normalized_source>` as the first hot routing scope because `RawLog` has no `device_id`.
- Bucket unparseable source addresses by hash and disable adaptive learning for unknown buckets until a stable alias is available.
- Persist scope-level adaptive controls in `parser_scope_state`, not in diagnostics side effects.
- Treat discovered `device_id` as control-plane alias/reporting metadata until an explicit ingest-stage device id exists.
- Keep all adaptive profile, rule, and diagnostic writes off the hot path through batched control events.
- Use Wilson lower bound as the activation confidence value.
- Use sliding-window recovery for malformed-flood and quarantine states; neither is permanent without continuing evidence.
- Prefer staged quarantine recovery for safe rules over permanent scope lockout.
- Persist `shadow_recovering` as its own rule status.
- Use checked insertion for all bounded hot-path buffers; overflow sets diagnostics and never panics.
- Preserve TOML rule priority within `RuleBasedParser`, and require pinning to promote configured rules across parser families.
- Generate `suggested_rule_id` automatically from high-confidence unknown keys, never as a manual free-text annotation.
- Allow operators to pin parsers and configured TOML rules into high-priority route groups.
- Treat control-channel drops as observable degraded adaptive learning, not silent data loss.
- Keep compatibility mode available until parser-kernel mode passes corpus equivalence and production smoke tests.
