# Adaptive Parser Learning Loop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the compatibility-mode adaptive learning loop without parser-kernel refactoring.

**Architecture:** Keep `ParserEngine::parse(RawLog) -> CanonicalEvent` and add a bounded adaptive rule application layer after deterministic parsers return `Partial` or `Failed`. A control manager consumes batched observations, promotes safe shadow rules using Wilson lower bound, disables bad active rules, and checkpoints complete adaptive state to the existing DuckDB tables.

**Tech Stack:** Rust workspace, `fwlog-adapter`, `fwlog-storage`, `fwlog-api`, `fwlogd`, DuckDB, `arrayvec`, `chrono`, existing bounded extractor and diagnostics modules.

---

## Scope

Included:

- Adaptive field rule model independent of configured TOML `RuleBasedParser`.
- Active adaptive rule application that fills only empty canonical fields.
- Applied rule attribution through `ParseDiagnosticsBuffer`.
- Observation collection from bounded generic pairs for partial/failed parses.
- Wilson lower-bound promotion from `shadow` to `active`.
- Rollback/quarantine decisions from post-activation counters.
- Durable checkpoint writes to `parser_profiles`, `adaptive_field_rules`, `parser_scope_state`, `parser_diagnostics`, `source_device_aliases`, and `parser_checkpoint_version`.
- `ParserEngine` configuration hooks for immutable adaptive snapshots.
- `fwlogd` worker integration using batch flushes.

Excluded:

- Parser-kernel ABI and zero-copy `ParseOutput`.
- Per-event DuckDB adaptive writes.
- Frontend changes.
- Sophisticated Bloom/count-min malformed-flood implementation; V1 uses bounded per-scope counters and visible scope flags.

## File Structure

Create:

- `crates/fwlog-adapter/src/learn.rs`  
  Adaptive rule status, value type inference, Wilson score, rule application, shadow observations, rollback counters, and the in-memory `AdaptiveControlState`.

Modify:

- `crates/fwlog-adapter/src/adaptive.rs`  
  Keep bounded pair extraction and expose helpers needed by learning.

- `crates/fwlog-adapter/src/diagnostics.rs`  
  Record adaptive observations, rule application conflicts, and attribution.

- `crates/fwlog-adapter/src/lib.rs`  
  Add adaptive snapshot configuration, apply active rules after deterministic output, and emit observations for control-path flush.

- `crates/fwlog-adapter/src/control.rs`  
  Extend flush events with adaptive observations, applied-rule status counters, and scope gap semantics.

- `crates/fwlog-storage/src/duckdb.rs`  
  Add transactional checkpoint writer methods for adaptive state tables.

- `crates/fwlog-storage/src/lib.rs`  
  Export checkpoint DTOs.

- `apps/fwlogd/src/pipeline.rs`  
  Create a control state beside the parser, flush parser observations in batches, publish new parser snapshots, and checkpoint state periodically.

## Task 1: Adaptive Rule Application

**Files:**

- Create: `crates/fwlog-adapter/src/learn.rs`
- Modify: `crates/fwlog-adapter/src/lib.rs`
- Modify: `crates/fwlog-adapter/src/diagnostics.rs`

- [ ] **Step 1: Write failing tests for active rules filling empty fields**

Add tests in `learn.rs`:

```rust
#[test]
fn active_rule_fills_empty_destination_and_reclassifies_partial() {
    let raw = raw("dstAddr=10.0.0.2 src=10.0.0.1 proto=TCP");
    let mut event = CanonicalEvent::failed(raw.clone(), "partial");
    event.src_ip = Some("10.0.0.1".to_string());
    event.protocol = Some("TCP".to_string());
    event.classify_firewall_tuple();

    let rules = AdaptiveRuleSnapshot::from_rules(vec![AdaptiveFieldRule::active(
        "rule:source:tcp:dstAddr:dst_ip",
        "source:tcp://127.0.0.1",
        "dstAddr",
        CanonicalField::DstIp,
        AdaptiveValueType::Ip,
    )]);
    let mut diagnostics = ParseDiagnosticsBuffer::default();

    let result = apply_active_rules(
        &rules,
        "source:tcp://127.0.0.1",
        &raw.raw,
        &mut event,
        &mut diagnostics,
    );

    assert_eq!(result.applied, 1);
    assert_eq!(event.dst_ip.as_deref(), Some("10.0.0.2"));
    assert_eq!(event.parse_status, ParseStatus::Parsed);
    assert_eq!(diagnostics.applied_rules.len(), 1);
}
```

- [ ] **Step 2: Verify the test fails**

Run:

```powershell
& "$env:USERPROFILE\.cargo\bin\cargo.exe" test -p fwlog-adapter active_rule_fills_empty_destination_and_reclassifies_partial
```

Expected: fail because `learn.rs` and adaptive rule application do not exist.

- [ ] **Step 3: Implement adaptive field rule model and application**

Create:

- `AdaptiveRuleStatus`: `Shadow`, `ShadowRecovering`, `Active`, `Disabled`
- `CanonicalField`: `SrcIp`, `SrcPort`, `DstIp`, `DstPort`, `Protocol`, `Action`, `Severity`
- `AdaptiveValueType`: `Ip`, `Port`, `Protocol`, `Action`, `String`
- `AdaptiveFieldRule`
- `AdaptiveRuleSnapshot`
- `apply_active_rules()`

Rules must:

- Apply only when status is `Active`.
- Match exact `scope_key`.
- Fill only empty fields.
- Validate value type before setting.
- Push applied rule id into diagnostics.
- Re-run `CanonicalEvent::classify_firewall_tuple()`.
- Never overwrite deterministic parser output.

## Task 2: Shadow Observations And Wilson Activation

**Files:**

- Modify: `crates/fwlog-adapter/src/learn.rs`
- Modify: `crates/fwlog-adapter/src/control.rs`

- [ ] **Step 1: Write failing tests for Wilson-gated activation**

Tests:

```rust
#[test]
fn shadow_rule_activates_only_after_sample_and_wilson_thresholds() {
    let mut state = AdaptiveControlState::new(AdaptiveLearningConfig {
        suggested_rule_min_samples: 20,
        activation_wilson_lower_bound: 0.80,
        auto_activate: true,
        ..AdaptiveLearningConfig::test_defaults()
    });

    for _ in 0..19 {
        state.observe_pair("source:tcp://127.0.0.1", "dstAddr", "10.0.0.2");
    }
    state.evaluate_rules(Utc::now());
    assert!(state.active_rules().is_empty());

    for _ in 0..20 {
        state.record_shadow_result("source:tcp://127.0.0.1", "dstAddr", CanonicalField::DstIp, true);
    }
    state.evaluate_rules(Utc::now());
    assert_eq!(state.active_rules().len(), 1);
    assert_eq!(state.active_rules()[0].confidence >= 0.80, true);
}
```

- [ ] **Step 2: Implement control-state learning**

Implement:

- `AdaptiveLearningConfig`
- `AdaptiveObservation`
- `AdaptiveControlState::observe_pair`
- `AdaptiveControlState::record_shadow_result`
- `AdaptiveControlState::evaluate_rules`
- `wilson_lower_bound(wins, samples, z)`

V1 inference:

- Known raw keys such as `src`, `src_ip`, `dst`, `dst_ip`, `proto`, `action`, `sport`, `dport` map directly.
- Unknown keys infer target by value type only when the current event is missing exactly one canonical field of that type.
- Unknown source buckets do not generate rules.
- Metrics-gap scopes do not auto-activate.

## Task 3: ParserEngine Adaptive Snapshot Integration

**Files:**

- Modify: `crates/fwlog-adapter/src/lib.rs`
- Modify: `crates/fwlog-adapter/src/diagnostics.rs`
- Modify: `crates/fwlog-adapter/src/control.rs`

- [ ] **Step 1: Write failing parser integration tests**

Tests:

```rust
#[test]
fn parser_applies_active_adaptive_rule_after_partial_static_parse() {
    let snapshot = AdaptiveRuleSnapshot::from_rules(vec![AdaptiveFieldRule::active(
        "rule:dstAddr",
        "source:tcp://127.0.0.1",
        "dstAddr",
        CanonicalField::DstIp,
        AdaptiveValueType::Ip,
    )]);
    let engine = ParserEngine::new().with_adaptive_snapshot(snapshot);
    let mut diagnostics = ParseDiagnosticsBuffer::default();

    let result = engine.parse_inner(raw("src=10.0.0.1 dstAddr=10.0.0.2 proto=TCP"), &mut diagnostics);

    assert_eq!(result.event.parse_status, ParseStatus::Parsed);
    assert_eq!(result.event.dst_ip.as_deref(), Some("10.0.0.2"));
    assert_eq!(diagnostics.applied_rules.len(), 1);
}
```

- [ ] **Step 2: Implement parser integration**

Add `adaptive_snapshot: AdaptiveRuleSnapshot` to `ParserEngine`.

After deterministic routes finish:

- If result is `Parsed`, return without adaptive mutation.
- If result is `Partial` or `Failed`, extract bounded pairs and apply active rules.
- If rules improve the event, return improved event.
- If active rules fail validation, record diagnostics and leave deterministic result intact.

## Task 4: Rollback And Quarantine

**Files:**

- Modify: `crates/fwlog-adapter/src/learn.rs`
- Modify: `crates/fwlog-adapter/src/control.rs`

- [ ] **Step 1: Write failing rollback tests**

Tests:

```rust
#[test]
fn active_rule_is_disabled_after_attributed_failure_threshold() {
    let mut state = AdaptiveControlState::with_active_rule(test_dst_rule());
    for _ in 0..10 {
        state.record_applied_rule_result("rule:dstAddr", ParseStatus::Failed);
    }
    state.evaluate_rollback(Utc::now());

    let rule = state.rule("rule:dstAddr").unwrap();
    assert_eq!(rule.status, AdaptiveRuleStatus::Disabled);
    assert_eq!(rule.disabled_reason.as_deref(), Some("rollback: attributed failure threshold exceeded"));
}
```

- [ ] **Step 2: Implement rollback**

Rules:

- Direct conversion conflicts disable the rule.
- Attributed failure/partial ratio above threshold disables the rule.
- Ambiguous multi-rule failure marks the scope as quarantined.
- Quarantine prevents active rules from applying for that scope.
- Repeated quarantine increases backoff up to the configured cap.

## Task 5: Durable Checkpoint Writer

**Files:**

- Modify: `crates/fwlog-storage/src/duckdb.rs`
- Modify: `crates/fwlog-storage/src/lib.rs`
- Modify: `crates/fwlog-adapter/src/learn.rs`

- [ ] **Step 1: Write failing storage checkpoint tests**

Tests:

```rust
#[test]
fn checkpoints_complete_adaptive_state_transactionally() {
    let dir = tempfile::tempdir().unwrap();
    let store = DuckDbStore::open(dir.path().join("oxidelog.duckdb")).unwrap();
    let checkpoint = ParserAdaptiveCheckpoint::single_active_rule_for_test();

    store.checkpoint_parser_adaptive_state(&checkpoint).unwrap();

    assert_eq!(store.list_adaptive_field_rules().unwrap().len(), 1);
    assert_eq!(store.list_parser_checkpoint_versions().unwrap()[0].status, "published");
}
```

- [ ] **Step 2: Implement checkpoint DTO and writer**

Add:

- `ParserAdaptiveCheckpoint`
- `ParserAdaptiveRuleCheckpointRow`
- `ParserProfileCheckpointRow`
- `ParserDiagnosticCheckpointRow`
- `ParserScopeCheckpointRow`
- `SourceDeviceAliasCheckpointRow`
- `DuckDbStore::checkpoint_parser_adaptive_state`

Use one transaction:

- Delete stable adaptive tables.
- Insert complete new rows.
- Insert `parser_checkpoint_version` with `status='published'`.
- Do not touch event rows.

## Task 6: fwlogd Control Loop Integration

**Files:**

- Modify: `apps/fwlogd/src/pipeline.rs`
- Modify: `crates/fwlog-adapter/src/control.rs`
- Modify: `crates/fwlog-adapter/src/lib.rs`

- [ ] **Step 1: Write focused non-network tests for batch learning**

Add unit tests around a small helper:

```rust
#[test]
fn worker_batch_flush_updates_control_state_and_parser_snapshot() {
    let mut control = AdaptiveControlState::new(AdaptiveLearningConfig::test_defaults());
    let mut parser = ParserEngine::new();
    let events = vec![raw("src=10.0.0.1 dstAddr=10.0.0.2 proto=TCP"); 32];

    for raw in events {
        let mut diagnostics = ParseDiagnosticsBuffer::default();
        let result = parser.parse_inner(raw, &mut diagnostics);
        control.observe_parse_result(&result.event, &diagnostics);
    }
    control.evaluate_rules(Utc::now());
    parser = parser.with_adaptive_snapshot(control.rule_snapshot());

    assert!(!control.rule_snapshot().is_empty());
}
```

- [ ] **Step 2: Integrate in worker**

In `run_worker`:

- Keep `AdaptiveControlState` in the worker thread.
- Parse with `parse_inner` to collect diagnostics.
- After each storage batch flush, merge observations into control state.
- Periodically evaluate rules and update parser adaptive snapshot.
- Periodically checkpoint adaptive state.
- If checkpoint fails, log and increment worker errors, but continue event ingestion.

## Task 7: Verification

Run:

```powershell
& "$env:USERPROFILE\.cargo\bin\cargo.exe" fmt
& "$env:USERPROFILE\.cargo\bin\cargo.exe" test -p fwlog-adapter
$env:RUSTFLAGS='-l Rstrtmgr'
& "$env:USERPROFILE\.cargo\bin\cargo.exe" test -p fwlog-storage
& "$env:USERPROFILE\.cargo\bin\cargo.exe" test -p fwlog-api -- --test-threads=1
& "$env:USERPROFILE\.cargo\bin\cargo.exe" check -p fwlog-storage -p fwlog-api -p fwlog-import -p fwlogd
```

Expected:

- Adapter/domain/storage tests pass.
- API may still have the unrelated frontend `/umi.` assertion failure if frontend remains out of scope.
- Check passes with only existing storage dead-code warnings unless those DTOs are consumed by the checkpoint writer.
