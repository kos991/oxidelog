# OxideLog Adaptive Parser Engine Design

## Goal

Refactor OxideLog parsing from a single Sangfor-oriented adapter into a deterministic, multi-vendor parser framework with automatic adaptive routing, field learning, diagnostics, and rollback controls.

The first implementation should improve two things:

- Multi-vendor extensibility: adding Huawei, H3C, Topsec, or new Sangfor variants should not require rewriting the engine.
- Parser observability: operators should know which parser matched, why parsing failed, and which adaptive rules were learned or disabled.

Automatic adaptation is in scope, but it must be guarded by confidence thresholds, shadow validation, and automatic rollback. The engine should become more useful over time without turning parsed events into black-box guesses.

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

For each new log line, the router boosts parsers that recently succeeded for that scope. This reduces blind parser attempts and makes multi-vendor support cheaper during live ingest and historical import.

The router must never skip deterministic parser detection entirely. It can reorder candidates, but a high-confidence built-in parser should still win over an adaptive rule.

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

### Confidence Gate

Automatic activation is allowed only when all guardrails pass:

- The same raw key appears repeatedly in the same scope.
- The value type is stable, for example IP, port, protocol, action, or string.
- `sample_count` meets the configured threshold.
- `confidence` meets the configured threshold.
- The rule fills an empty canonical field and does not overwrite a deterministic adapter field.
- Shadow validation shows the rule would improve parsed or partial output.

Default target thresholds:

- `min_samples = 100`
- `activation_confidence = 0.98`
- `rollback_failed_ratio = 0.20`

These values should be configurable.

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
- `sample_count BIGINT NOT NULL`
- `created_at TEXT NOT NULL`
- `activated_at TEXT`
- `disabled_at TEXT`
- `disabled_reason TEXT`

Supported status values:

- `shadow`
- `active`
- `disabled`

Rules are unique by `(scope_key, raw_key, canonical_field)`.

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
3. Read parser profile for the scope.
4. Detect registered deterministic adapters.
5. Order adapters by detection score, base priority, and adaptive boost.
6. Try deterministic adapters.
7. If an adapter returns `Parsed`, emit the event, update parser profile, and run shadow adaptive validation without changing the event.
8. If deterministic adapters fail or return `Partial`, run generic KV extraction.
9. Apply active adaptive field rules only to empty canonical fields.
10. Classify output as `Parsed`, `Partial`, or `Failed`.
11. Update parser profiles and diagnostics.
12. Update shadow rule samples.
13. Background evaluation promotes eligible shadow rules to active.
14. Rollback evaluation disables active rules if the scope failure ratio rises after activation.

## Rollback And Controls

Config:

```toml
[parser.adaptive]
enabled = true
auto_activate = true
min_samples = 100
activation_confidence = 0.98
rollback_failed_ratio = 0.20
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

## Implementation Boundaries

First implementation should stay backend-first:

- Refactor `crates/fwlog-adapter` into registry, diagnostics, adaptive mapper, and Sangfor adapter modules.
- Add storage tables and accessors in `crates/fwlog-storage`.
- Wire live ingest and historical import through the same engine.
- Add API endpoints for profiles, diagnostics, and adaptive rules.
- Add minimal UI only after backend behavior is tested.

Do not implement a full TOML/YAML rule language in the first pass. Leave room for a future `ConfigRuleAdapter`, but keep automatic rules in storage for now.

## Testing Strategy

Parser tests:

- Existing Sangfor tests keep passing with identical normalized fields.
- Parser registry tries higher-scoring deterministic adapters first.
- Adaptive router boosts the historically successful parser for a scope.
- Generic extractor discovers unknown key/value fields.
- Shadow rules collect samples without changing emitted events.
- Rules auto-activate when sample and confidence thresholds pass.
- Active rules fill only empty canonical fields.
- Active rules do not overwrite deterministic parser output.
- Bad active rules are disabled when rollback thresholds are crossed.
- `auto_activate = false` prevents shadow rules from becoming active.

Storage/API tests:

- Parser profile upsert and query work by scope.
- Adaptive rule lifecycle supports shadow, active, and disabled.
- Diagnostics group similar failures by fingerprint.
- Rule enable/disable endpoints change only the selected rule.

Pipeline/import tests:

- Live ingest and historical import both use the same parser engine behavior.
- Parser adaptive state failure does not block event writes.
- Disabled adaptive mode preserves deterministic parsing.

## Migration Plan

1. Add `Partial` status and update storage/API serialization tests.
2. Refactor the adapter trait to return parse diagnostics.
3. Move Sangfor parsing behind the new registry without behavior changes.
4. Add profile storage and adaptive parser ordering.
5. Add generic KV extraction and shadow rule collection.
6. Add active adaptive rule application.
7. Add rollback and manual enable/disable APIs.
8. Add UI/API visibility for profiles, rules, and diagnostics.

## Default Decisions

- Include `Partial` in normal event searches and unified search results.
- Keep `include_failed=false` scoped to failed rows only; it should not hide partial rows.
- Store adaptive rules in DuckDB first; add JSON export later only if disaster recovery needs it.
- Keep one truncated sample per fingerprint and preserve full raw only in the event row/frozen archive.
- Prefer `device:<device_id>` scope when device binding is available before parsing; otherwise use `source:<source_addr>` and let later backfill/profile repair improve scope quality.
