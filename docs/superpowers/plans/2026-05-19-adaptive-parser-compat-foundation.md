# Adaptive Parser Compatibility Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the compatibility-mode foundation for adaptive parsing: `Partial` status, parser diagnostics, source scopes, route snapshots, bounded extraction, and durable read-only adaptive state.

**Architecture:** Keep `ParserEngine::parse(RawLog) -> CanonicalEvent` as the public compatibility API while adding focused modules inside `crates/fwlog-adapter`. The first phase records diagnostics and adaptive observations, but it does not auto-activate learned rules or implement parser-kernel mode. Storage/API expose parser profiles, diagnostics, rules, and scope state from durable checkpoint tables.

**Tech Stack:** Rust workspace, `fwlog-domain`, `fwlog-adapter`, `fwlog-storage`, `fwlog-api`, DuckDB, `arrayvec`, `memchr`, `sha2`, existing `regex`/`serde`/`toml`.

---

## Scope

This plan implements the first compatibility foundation slice from `docs/superpowers/specs/2026-05-19-adaptive-parser-engine-design.md`.

Included:

- `ParseStatus::Partial` across domain, storage, API, import, and cold search.
- Compatibility diagnostics via `ParserEngine::parse_inner`.
- Stable parser ids and route snapshot ordering with pinned parser ids.
- Source normalization and unknown-source bucket behavior.
- Bounded adaptive field extraction with checked `ArrayVec` insertion.
- In-memory metrics batch types and explicit drop/gap accounting data structures.
- DuckDB schemas and read accessors for parser profiles, adaptive rules, diagnostics, scope state, source aliases, and checkpoint version.
- Read-only API endpoints for parser adaptive state.

Excluded from this plan:

- Wilson-score activation.
- Active adaptive field rule application.
- Rollback/quarantine enforcement.
- Background async control manager.
- Parser-kernel mode and zero-copy `ParseOutput`.
- UI beyond the API responses.

Those excluded items belong in the next implementation plan after this foundation is merged and tested.

## File Structure

Create:

- `crates/fwlog-adapter/src/diagnostics.rs`  
  Owns compatibility diagnostics buffers, detect outcomes, parse results, parser ids, and bounded applied-rule recording.

- `crates/fwlog-adapter/src/scope.rs`  
  Owns `SourceScope`, `ScopeNormalizationMode`, source normalization, unknown hash buckets, and high-entropy marker inputs.

- `crates/fwlog-adapter/src/route.rs`  
  Owns static route groups, route snapshots, parser registry ids, pinned parser config normalization, and route ordering.

- `crates/fwlog-adapter/src/adaptive.rs`  
  Owns `GenericPair`, `GenericPairs`, bounded extractor diagnostics, safe long-line truncation, and max-pair overflow behavior.

- `crates/fwlog-adapter/src/control.rs`  
  Owns local parser metrics batch structs, drop counters, scope-state DTOs, and flush-event shapes. This task does not spawn a background manager.

Modify:

- `crates/fwlog-domain/src/event.rs`  
  Add `ParseStatus::Partial`, minimum tuple classification helper, and serialization tests.

- `crates/fwlog-adapter/src/lib.rs`  
  Wire modules, parser ids, compatibility `parse_inner`, diagnostics recording, route snapshot traversal, and `parse` wrapper.

- `crates/fwlog-adapter/src/generic.rs`  
  Preserve current deterministic generic parser behavior but classify partial outputs consistently.

- `crates/fwlog-adapter/src/rule.rs`  
  Preserve TOML rule priority inside `RuleBasedParser`; expose stable rule ids and reject duplicate rule names.

- `crates/fwlog-adapter/src/sangfor.rs`  
  Add stable `parser_id` metadata through a compatibility shim while preserving current parse fields.

- `crates/fwlog-adapter/Cargo.toml`  
  Add direct `arrayvec`, `memchr`, and `sha2.workspace = true` dependencies for the adapter crate.

- `crates/fwlog-storage/src/duckdb.rs`  
  Update event status mapping/query semantics and add adaptive parser tables plus read accessors.

- `crates/fwlog-storage/src/lib.rs`  
  Export parser adaptive DTOs and query methods.

- `crates/fwlog-api/src/lib.rs`  
  Register read-only parser adaptive endpoints.

- `crates/fwlog-api/src/handlers.rs`  
  Add handler DTOs and update `include_failed=false` logic so it excludes only `Failed`, not `Partial`.

- `apps/fwlog-import/src/main.rs`  
  Ensure historical import and reparse preserve `Partial` status and use the compatibility parser path.

- `apps/fwlogd/src/pipeline.rs`  
  Ensure live ingest still uses the shared parser and treats `Partial` as event data.

---

### Task 1: Add `Partial` Status End-To-End For Stored Events

**Files:**

- Modify: `crates/fwlog-domain/src/event.rs`
- Modify: `crates/fwlog-storage/src/duckdb.rs`
- Modify: `crates/fwlog-api/src/handlers.rs`
- Modify: `apps/fwlog-import/src/main.rs`

- [ ] **Step 1: Write failing domain tests for `Partial` serialization and classification**

Add these tests to `crates/fwlog-domain/src/event.rs`:

```rust
#[test]
fn partial_events_serialize_status_as_lowercase() {
    let event = CanonicalEvent {
        event_id: "id".to_string(),
        ingest_time: Utc.timestamp_opt(1_778_808_000, 0).unwrap(),
        source_addr: "udp://192.168.1.1:514".to_string(),
        device_id: None,
        event_time: None,
        vendor: None,
        product: None,
        src_ip: Some("192.168.1.1".to_string()),
        src_port: None,
        dst_ip: None,
        dst_port: None,
        protocol: Some("TCP".to_string()),
        action: None,
        severity: None,
        raw: "raw".to_string(),
        parse_status: ParseStatus::Partial,
        parse_error: Some("missing destination endpoint".to_string()),
    };

    let json = serde_json::to_string(&event).unwrap();
    assert!(json.contains(r#""parse_status":"partial""#));
}

#[test]
fn classify_firewall_tuple_distinguishes_parsed_partial_failed() {
    let mut event = CanonicalEvent::failed(
        crate::RawLog {
            ingest_time: Utc.timestamp_opt(1_778_808_000, 0).unwrap(),
            source_addr: "tcp://127.0.0.1:1514".to_string(),
            raw: "raw".to_string(),
        },
        "bad",
    );

    event.src_ip = Some("192.168.1.1".to_string());
    event.dst_ip = Some("10.0.0.1".to_string());
    event.protocol = Some("UDP".to_string());
    event.parse_error = Some("old error".to_string());
    event.classify_firewall_tuple();
    assert_eq!(event.parse_status, ParseStatus::Parsed);
    assert_eq!(event.parse_error, None);

    event.protocol = None;
    event.action = None;
    event.classify_firewall_tuple();
    assert_eq!(event.parse_status, ParseStatus::Partial);
    assert!(event.parse_error.as_deref().unwrap().contains("minimum searchable tuple"));

    event.src_ip = None;
    event.dst_ip = None;
    event.classify_firewall_tuple();
    assert_eq!(event.parse_status, ParseStatus::Failed);
}
```

- [ ] **Step 2: Run domain test and verify it fails**

Run:

```powershell
cargo test -p fwlog-domain partial_events_serialize_status_as_lowercase classify_firewall_tuple_distinguishes_parsed_partial_failed
```

Expected: fail because `ParseStatus::Partial` and `CanonicalEvent::classify_firewall_tuple` do not exist.

- [ ] **Step 3: Implement `Partial` and classification helper**

In `crates/fwlog-domain/src/event.rs`, update `ParseStatus`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ParseStatus {
    Parsed,
    Partial,
    Failed,
}
```

Add this method inside `impl CanonicalEvent`:

```rust
pub fn classify_firewall_tuple(&mut self) {
    let has_src = self.src_ip.as_deref().is_some_and(|v| !v.is_empty());
    let has_dst = self.dst_ip.as_deref().is_some_and(|v| !v.is_empty());
    let has_action_or_protocol = self
        .action
        .as_deref()
        .is_some_and(|v| !v.is_empty())
        || self
            .protocol
            .as_deref()
            .is_some_and(|v| !v.is_empty());

    if has_src && has_dst && has_action_or_protocol {
        self.parse_status = ParseStatus::Parsed;
        self.parse_error = None;
    } else if has_src || has_dst || has_action_or_protocol {
        self.parse_status = ParseStatus::Partial;
        self.parse_error = Some("partial parse: minimum searchable tuple incomplete".to_string());
    } else {
        self.parse_status = ParseStatus::Failed;
        if self.parse_error.is_none() {
            self.parse_error = Some("failed parse: no useful canonical fields".to_string());
        }
    }
}
```

- [ ] **Step 4: Update storage status mapping and query semantics**

In `crates/fwlog-storage/src/duckdb.rs`, update private status helpers. Replace the existing `status_str` and row mapping branches with:

```rust
fn status_str(status: ParseStatus) -> &'static str {
    match status {
        ParseStatus::Parsed => "parsed",
        ParseStatus::Partial => "partial",
        ParseStatus::Failed => "failed",
    }
}

fn parse_status_from_str(value: &str) -> ParseStatus {
    match value {
        "parsed" => ParseStatus::Parsed,
        "partial" => ParseStatus::Partial,
        _ => ParseStatus::Failed,
    }
}
```

Use `parse_status_from_str(&parse_status)` in row-to-event conversion.

Change event filtering where `include_failed=false` currently forces parsed-only:

```rust
if !query.include_failed {
    clauses.push("parse_status <> 'failed'");
}
```

Change raw-pruning logic so partial rows keep raw like failed rows:

```sql
UPDATE events SET raw = '' WHERE parse_status = 'parsed' AND raw <> ''
```

Keep this as-is if it already only prunes parsed rows.

- [ ] **Step 5: Write storage tests for partial query and metrics**

Add to `crates/fwlog-storage/src/duckdb.rs` tests:

```rust
#[test]
fn include_failed_false_keeps_partial_rows() {
    let store = temp_store();
    let parsed = event("parsed", ParseStatus::Parsed);
    let partial = event("partial", ParseStatus::Partial);
    let failed = event("failed", ParseStatus::Failed);

    store.insert_batch(&[parsed, partial, failed]).unwrap();

    let rows = store
        .query_events(
            &EventQuery {
                include_failed: false,
                ..EventQuery::default()
            },
            10,
        )
        .unwrap();

    let ids: Vec<_> = rows.iter().map(|row| row.event_id.as_str()).collect();
    assert!(ids.contains(&"parsed"));
    assert!(ids.contains(&"partial"));
    assert!(!ids.contains(&"failed"));
}
```

If metric DTOs currently expose only `parsed` and `failed`, add `partial: u64` to the relevant structs in `duckdb.rs` and update SQL:

```sql
SUM(CASE WHEN parse_status = 'partial' THEN 1 ELSE 0 END) AS partial
```

- [ ] **Step 6: Update API cold-search filtering**

In `crates/fwlog-api/src/handlers.rs`, replace parsed-only checks like:

```rust
if !query.include_failed && event.parse_status != fwlog_domain::ParseStatus::Parsed {
    continue;
}
```

with:

```rust
if !query.include_failed && event.parse_status == fwlog_domain::ParseStatus::Failed {
    continue;
}
```

Update `parquet_row_to_event` status parsing:

```rust
parse_status: match parse_status.as_str() {
    "parsed" => ParseStatus::Parsed,
    "partial" => ParseStatus::Partial,
    _ => ParseStatus::Failed,
},
```

- [ ] **Step 7: Run focused tests**

Run:

```powershell
cargo test -p fwlog-domain
cargo test -p fwlog-storage include_failed_false_keeps_partial_rows
cargo test -p fwlog-api include_failed
```

Expected: all pass.

- [ ] **Step 8: Commit**

```powershell
git add crates\fwlog-domain\src\event.rs crates\fwlog-storage\src\duckdb.rs crates\fwlog-api\src\handlers.rs apps\fwlog-import\src\main.rs
git commit -m "feat: add partial parse status"
```

---

### Task 2: Add Compatibility Parser Diagnostics

**Files:**

- Create: `crates/fwlog-adapter/src/diagnostics.rs`
- Modify: `crates/fwlog-adapter/src/lib.rs`
- Modify: `crates/fwlog-adapter/src/sangfor.rs`
- Modify: `crates/fwlog-adapter/src/generic.rs`
- Modify: `crates/fwlog-adapter/src/rule.rs`

- [ ] **Step 1: Add failing diagnostics tests**

Add to `crates/fwlog-adapter/src/lib.rs` tests:

```rust
#[test]
fn parse_inner_records_successful_parser() {
    let engine = ParserEngine::new();
    let mut diagnostics = ParseDiagnosticsBuffer::default();
    let result = engine.parse_inner(
        raw("Sangfor: src=192.168.1.10 dst=8.8.8.8 proto=UDP action=allow"),
        &mut diagnostics,
    );

    assert_eq!(result.event.parse_status, ParseStatus::Parsed);
    assert_eq!(diagnostics.matched_parser_id.as_deref(), Some("parser:sangfor_nat_v1"));
    assert!(diagnostics.attempts.iter().any(|attempt| attempt.parser_id == "parser:sangfor_nat_v1"));
}

#[test]
fn parse_inner_records_all_failed_attempts() {
    let engine = ParserEngine::new();
    let mut diagnostics = ParseDiagnosticsBuffer::default();
    let result = engine.parse_inner(raw("totally unstructured garbage line"), &mut diagnostics);

    assert_eq!(result.event.parse_status, ParseStatus::Failed);
    assert!(diagnostics.attempts.iter().any(|attempt| attempt.parser_id == "parser:sangfor_nat_v1"));
    assert!(diagnostics.attempts.iter().any(|attempt| attempt.parser_id == "parser:generic_kv_v1"));
    assert!(diagnostics.failure_reason.as_deref().unwrap().contains("no parser matched"));
}
```

- [ ] **Step 2: Run adapter tests and verify failure**

Run:

```powershell
cargo test -p fwlog-adapter parse_inner_records_successful_parser parse_inner_records_all_failed_attempts
```

Expected: fail because diagnostics types and `parse_inner` are missing.

- [ ] **Step 3: Create diagnostics module**

Create `crates/fwlog-adapter/src/diagnostics.rs`:

```rust
use arrayvec::ArrayVec;
use fwlog_domain::CanonicalEvent;

pub const MAX_APPLIED_RULES: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParserId(pub &'static str);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleId(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetectScore {
    NoMatch,
    CompatibilityMatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectOutcome {
    pub score: DetectScore,
    pub reason: &'static str,
}

impl DetectOutcome {
    pub fn matched(reason: &'static str) -> Self {
        Self {
            score: DetectScore::CompatibilityMatch,
            reason,
        }
    }

    pub fn no_match(reason: &'static str) -> Self {
        Self {
            score: DetectScore::NoMatch,
            reason,
        }
    }

    pub fn is_match(&self) -> bool {
        matches!(self.score, DetectScore::CompatibilityMatch)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParserAttemptDiagnostic {
    pub parser_id: String,
    pub parser_name: String,
    pub detect_reason: String,
    pub status: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ParseDiagnosticsBuffer {
    pub matched_parser_id: Option<String>,
    pub attempts: Vec<ParserAttemptDiagnostic>,
    pub failure_reason: Option<String>,
    pub pairs_truncated: bool,
    pub line_truncated: bool,
    pub applied_rules: ArrayVec<RuleId, MAX_APPLIED_RULES>,
    pub applied_rules_truncated: bool,
}

impl Default for ParseDiagnosticsBuffer {
    fn default() -> Self {
        Self {
            matched_parser_id: None,
            attempts: Vec::new(),
            failure_reason: None,
            pairs_truncated: false,
            line_truncated: false,
            applied_rules: ArrayVec::new(),
            applied_rules_truncated: false,
        }
    }
}

impl ParseDiagnosticsBuffer {
    pub fn clear(&mut self) {
        *self = Self::default();
    }

    pub fn push_attempt(
        &mut self,
        parser_id: impl Into<String>,
        parser_name: impl Into<String>,
        detect_reason: impl Into<String>,
        event: &CanonicalEvent,
    ) {
        self.attempts.push(ParserAttemptDiagnostic {
            parser_id: parser_id.into(),
            parser_name: parser_name.into(),
            detect_reason: detect_reason.into(),
            status: serde_json::to_string(&event.parse_status)
                .unwrap_or_else(|_| "\"failed\"".to_string())
                .trim_matches('"')
                .to_string(),
            error: event.parse_error.clone(),
        });
    }

    pub fn push_applied_rule(&mut self, rule_id: RuleId) {
        if self.applied_rules.try_push(rule_id).is_err() {
            self.applied_rules_truncated = true;
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParseResult {
    pub event: CanonicalEvent,
}
```

- [ ] **Step 4: Export diagnostics module and add dependency**

In `crates/fwlog-adapter/Cargo.toml`, add:

```toml
arrayvec = "0.7"
```

In `crates/fwlog-adapter/src/lib.rs`, add:

```rust
mod diagnostics;

pub use diagnostics::{
    DetectOutcome, DetectScore, ParseDiagnosticsBuffer, ParseResult, ParserAttemptDiagnostic,
    ParserId, RuleId,
};
```

- [ ] **Step 5: Add compatibility metadata methods**

In `crates/fwlog-adapter/src/sangfor.rs`, extend the `LogAdapter` trait with default methods:

```rust
fn parser_id(&self) -> &'static str {
    "parser:legacy_adapter"
}

fn detect(&self, raw: &RawLog) -> crate::DetectOutcome {
    if self.can_parse(raw) {
        crate::DetectOutcome::matched("compatibility can_parse matched")
    } else {
        crate::DetectOutcome::no_match("compatibility can_parse did not match")
    }
}
```

Override for `SangforAdapter`:

```rust
fn parser_id(&self) -> &'static str {
    "parser:sangfor_nat_v1"
}
```

In `GenericKvParser`, add:

```rust
pub const PARSER_ID: &'static str = "parser:generic_kv_v1";
pub const PARSER_NAME: &'static str = "GenericKv";
```

In `RuleBasedParser`, add:

```rust
pub const PARSER_ID: &'static str = "parser:rule_based_v1";
pub const PARSER_NAME: &'static str = "RuleBased";
```

- [ ] **Step 6: Implement `parse_inner` wrapper**

In `crates/fwlog-adapter/src/lib.rs`, keep `parse` public and add:

```rust
pub fn parse_inner(&self, raw: RawLog, diagnostics: &mut ParseDiagnosticsBuffer) -> ParseResult {
    diagnostics.clear();
    let mut tried: Vec<String> = Vec::new();

    let mut matched_any_adapter = false;
    for adapter in &self.known_adapters {
        let detect = adapter.detect(&raw);
        if !detect.is_match() {
            continue;
        }
        matched_any_adapter = true;
        let event = adapter.parse(raw.clone());
        diagnostics.push_attempt(adapter.parser_id(), adapter.name(), detect.reason, &event);
        if event.parse_status == ParseStatus::Parsed {
            diagnostics.matched_parser_id = Some(adapter.parser_id().to_string());
            return ParseResult { event };
        }
        tried.push(format!(
            "{}: {}",
            adapter.name(),
            event.parse_error.as_deref().unwrap_or("failed to parse")
        ));
    }

    if !matched_any_adapter {
        for adapter in &self.known_adapters {
            let event = adapter.parse(raw.clone());
            diagnostics.push_attempt(
                adapter.parser_id(),
                adapter.name(),
                "compatibility fallback after no adapter detected",
                &event,
            );
            if event.parse_status == ParseStatus::Parsed {
                diagnostics.matched_parser_id = Some(adapter.parser_id().to_string());
                return ParseResult { event };
            }
            tried.push(format!(
                "{}: {}",
                adapter.name(),
                event.parse_error.as_deref().unwrap_or("failed to parse")
            ));
        }
    }

    if self.generic.can_parse(&raw) {
        let event = self.generic.parse(raw.clone());
        diagnostics.push_attempt(
            GenericKvParser::PARSER_ID,
            GenericKvParser::PARSER_NAME,
            "generic key/value detector matched",
            &event,
        );
        if event.parse_status == ParseStatus::Parsed {
            diagnostics.matched_parser_id = Some(GenericKvParser::PARSER_ID.to_string());
            return ParseResult { event };
        }
        tried.push(format!(
            "GenericKv: {}",
            event.parse_error.as_deref().unwrap_or("failed")
        ));
    }

    if self.rules.can_parse(&raw) {
        let event = self.rules.parse(raw.clone());
        diagnostics.push_attempt(
            RuleBasedParser::PARSER_ID,
            RuleBasedParser::PARSER_NAME,
            "configured rules available",
            &event,
        );
        if event.parse_status == ParseStatus::Parsed {
            diagnostics.matched_parser_id = Some(RuleBasedParser::PARSER_ID.to_string());
            return ParseResult { event };
        }
        tried.push(format!(
            "RuleBased: {}",
            event.parse_error.as_deref().unwrap_or("failed")
        ));
    }

    let reason = format!("no parser matched; attempted [{}]", tried.join(", "));
    diagnostics.failure_reason = Some(reason.clone());
    ParseResult {
        event: CanonicalEvent::failed(raw, reason),
    }
}
```

Change `parse` to:

```rust
pub fn parse(&self, raw: RawLog) -> CanonicalEvent {
    let mut diagnostics = ParseDiagnosticsBuffer::default();
    self.parse_inner(raw, &mut diagnostics).event
}
```

- [ ] **Step 7: Run focused tests**

```powershell
cargo test -p fwlog-adapter parse_inner_records_successful_parser parse_inner_records_all_failed_attempts
cargo test -p fwlog-adapter
```

Expected: pass.

- [ ] **Step 8: Commit**

```powershell
git add crates\fwlog-adapter\Cargo.toml crates\fwlog-adapter\src\diagnostics.rs crates\fwlog-adapter\src\lib.rs crates\fwlog-adapter\src\sangfor.rs crates\fwlog-adapter\src\generic.rs crates\fwlog-adapter\src\rule.rs Cargo.toml Cargo.lock
git commit -m "feat: add parser diagnostics shim"
```

---

### Task 3: Add Source Scope Normalization

**Files:**

- Create: `crates/fwlog-adapter/src/scope.rs`
- Modify: `crates/fwlog-adapter/src/lib.rs`
- Modify: `crates/fwlog-adapter/Cargo.toml`

- [ ] **Step 1: Add failing source normalization tests**

Create `crates/fwlog-adapter/src/scope.rs` with tests first:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_ip_mode_drops_ephemeral_ports() {
        let scope = normalize_source_scope(
            "udp://192.168.1.10:55123",
            ScopeNormalizationMode::SourceIp,
        );
        assert_eq!(scope.scope_key, "source:udp://192.168.1.10");
        assert_eq!(scope.normalized_source, "udp://192.168.1.10");
        assert!(!scope.unknown_source_bucket);
    }

    #[test]
    fn source_ip_port_mode_preserves_port() {
        let scope = normalize_source_scope(
            "tcp://127.0.0.1:1514",
            ScopeNormalizationMode::SourceIpPort,
        );
        assert_eq!(scope.scope_key, "source:tcp://127.0.0.1:1514");
    }

    #[test]
    fn malformed_sources_use_stable_unknown_hash_bucket() {
        let first = normalize_source_scope("not a uri", ScopeNormalizationMode::SourceIp);
        let second = normalize_source_scope("not a uri", ScopeNormalizationMode::SourceIp);
        let other = normalize_source_scope("also not a uri", ScopeNormalizationMode::SourceIp);

        assert!(first.scope_key.starts_with("source:unknown:"));
        assert_eq!(first.scope_key, second.scope_key);
        assert_ne!(first.scope_key, other.scope_key);
        assert!(first.unknown_source_bucket);
        assert!(!first.adaptive_learning_enabled);
    }
}
```

- [ ] **Step 2: Run tests and verify failure**

```powershell
cargo test -p fwlog-adapter source_ip_mode_drops_ephemeral_ports malformed_sources_use_stable_unknown_hash_bucket
```

Expected: fail because the module is not wired and functions do not exist.

- [ ] **Step 3: Implement scope normalization**

Replace `crates/fwlog-adapter/src/scope.rs` with:

```rust
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeNormalizationMode {
    SourceIp,
    SourceIpPort,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceScope {
    pub scope_key: String,
    pub normalized_source: String,
    pub unknown_source_bucket: bool,
    pub adaptive_learning_enabled: bool,
}

pub fn normalize_source_scope(raw_source: &str, mode: ScopeNormalizationMode) -> SourceScope {
    if let Some((scheme, host, port)) = parse_scheme_host_port(raw_source) {
        let normalized = match mode {
            ScopeNormalizationMode::SourceIp => format!("{scheme}://{host}"),
            ScopeNormalizationMode::SourceIpPort => match port {
                Some(port) => format!("{scheme}://{host}:{port}"),
                None => format!("{scheme}://{host}"),
            },
        };
        return SourceScope {
            scope_key: format!("source:{normalized}"),
            normalized_source: normalized,
            unknown_source_bucket: false,
            adaptive_learning_enabled: true,
        };
    }

    let hash_prefix = unknown_hash_prefix(raw_source);
    let normalized = format!("unknown:{hash_prefix}");
    SourceScope {
        scope_key: format!("source:{normalized}"),
        normalized_source: normalized,
        unknown_source_bucket: true,
        adaptive_learning_enabled: false,
    }
}

fn parse_scheme_host_port(raw_source: &str) -> Option<(&str, String, Option<u16>)> {
    let (scheme, rest) = raw_source.split_once("://")?;
    if scheme.is_empty() || rest.is_empty() {
        return None;
    }

    let host_port = rest.split('/').next().unwrap_or(rest);
    let (host, port) = if host_port.starts_with('[') {
        let end = host_port.find(']')?;
        let host = host_port[1..end].to_string();
        let port = host_port[end + 1..]
            .strip_prefix(':')
            .and_then(|value| value.parse::<u16>().ok());
        (host, port)
    } else {
        match host_port.rsplit_once(':') {
            Some((host, port)) if !host.contains(':') => {
                (host.to_string(), port.parse::<u16>().ok())
            }
            _ => (host_port.to_string(), None),
        }
    };

    if host.is_empty() {
        None
    } else {
        Some((scheme, host, port))
    }
}

fn unknown_hash_prefix(raw_source: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_source.as_bytes());
    let digest = hasher.finalize();
    hex_prefix(&digest, 8)
}

fn hex_prefix(bytes: &[u8], nibbles: usize) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(nibbles);
    for byte in bytes {
        if out.len() == nibbles {
            break;
        }
        out.push(HEX[(byte >> 4) as usize] as char);
        if out.len() == nibbles {
            break;
        }
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
```

- [ ] **Step 4: Wire module and dependency**

In `crates/fwlog-adapter/Cargo.toml`, add:

```toml
sha2.workspace = true
```

In `crates/fwlog-adapter/src/lib.rs`, add:

```rust
mod scope;

pub use scope::{normalize_source_scope, ScopeNormalizationMode, SourceScope};
```

- [ ] **Step 5: Run tests**

```powershell
cargo test -p fwlog-adapter source_ip_mode_drops_ephemeral_ports source_ip_port_mode_preserves_port malformed_sources_use_stable_unknown_hash_bucket
```

Expected: pass.

- [ ] **Step 6: Commit**

```powershell
git add crates\fwlog-adapter\Cargo.toml crates\fwlog-adapter\src\scope.rs crates\fwlog-adapter\src\lib.rs Cargo.lock
git commit -m "feat: normalize parser source scopes"
```

---

### Task 4: Add Static Route Snapshots And Pinned Parser Ids

**Files:**

- Create: `crates/fwlog-adapter/src/route.rs`
- Modify: `crates/fwlog-adapter/src/lib.rs`
- Modify: `crates/fwlog-adapter/src/rule.rs`

- [ ] **Step 1: Write route ordering tests**

Create `crates/fwlog-adapter/src/route.rs` with tests first:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_route_snapshot_preserves_static_family_order() {
        let snapshot = RouteSnapshot::default_static();
        let mut ids = Vec::new();
        snapshot.for_each_parser_id("source:udp://192.168.1.10", |id| ids.push(id.to_string()));

        assert_eq!(
            ids,
            vec![
                "parser:sangfor_nat_v1",
                "parser:generic_kv_v1",
                "parser:rule_based_v1"
            ]
        );
    }

    #[test]
    fn pinned_scope_ids_are_first_without_sorting() {
        let snapshot = RouteSnapshot::with_pins(vec![PinnedScopeParsers {
            scope_key: "source:udp://192.168.1.10".to_string(),
            parser_ids: vec![
                "rule:default:CriticalCustomRule".to_string(),
                "parser:sangfor_nat_v1".to_string(),
            ],
        }]);

        let mut ids = Vec::new();
        snapshot.for_each_parser_id("source:udp://192.168.1.10", |id| ids.push(id.to_string()));
        assert_eq!(ids[0], "rule:default:CriticalCustomRule");
        assert_eq!(ids[1], "parser:sangfor_nat_v1");
        assert!(ids.iter().any(|id| id == "parser:generic_kv_v1"));
    }
}
```

- [ ] **Step 2: Run route tests and verify failure**

```powershell
cargo test -p fwlog-adapter default_route_snapshot_preserves_static_family_order pinned_scope_ids_are_first_without_sorting
```

Expected: fail because route types are missing.

- [ ] **Step 3: Implement route snapshot**

Replace `crates/fwlog-adapter/src/route.rs` with:

```rust
use std::collections::BTreeMap;

pub const SANGFOR_PARSER_ID: &str = "parser:sangfor_nat_v1";
pub const GENERIC_KV_PARSER_ID: &str = "parser:generic_kv_v1";
pub const RULE_BASED_PARSER_ID: &str = "parser:rule_based_v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticRouteGroup {
    parser_ids: Vec<String>,
}

impl StaticRouteGroup {
    pub fn new(parser_ids: Vec<String>) -> Self {
        Self { parser_ids }
    }

    pub fn parser_ids(&self) -> &[String] {
        &self.parser_ids
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinnedScopeParsers {
    pub scope_key: String,
    pub parser_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RouteSnapshot {
    default_groups: Vec<StaticRouteGroup>,
    scoped_pins: BTreeMap<String, StaticRouteGroup>,
}

impl RouteSnapshot {
    pub fn default_static() -> Self {
        Self {
            default_groups: vec![StaticRouteGroup::new(vec![
                SANGFOR_PARSER_ID.to_string(),
                GENERIC_KV_PARSER_ID.to_string(),
                RULE_BASED_PARSER_ID.to_string(),
            ])],
            scoped_pins: BTreeMap::new(),
        }
    }

    pub fn with_pins(pins: Vec<PinnedScopeParsers>) -> Self {
        let mut snapshot = Self::default_static();
        for pin in pins {
            snapshot.scoped_pins.insert(
                pin.scope_key,
                StaticRouteGroup::new(dedupe_preserving_order(pin.parser_ids)),
            );
        }
        snapshot
    }

    pub fn for_each_parser_id(&self, scope_key: &str, mut visit: impl FnMut(&str)) {
        let pinned = self.scoped_pins.get(scope_key);
        if let Some(group) = pinned {
            for id in group.parser_ids() {
                visit(id);
            }
        }
        for group in &self.default_groups {
            for id in group.parser_ids() {
                if pinned.is_some_and(|pinned| pinned.parser_ids().iter().any(|pinned_id| pinned_id == id)) {
                    continue;
                }
                visit(id);
            }
        }
    }
}

impl Default for RouteSnapshot {
    fn default() -> Self {
        Self::default_static()
    }
}

fn dedupe_preserving_order(ids: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    for id in ids {
        if !out.iter().any(|existing| existing == &id) {
            out.push(id);
        }
    }
    out
}

```

- [ ] **Step 4: Wire route module**

In `crates/fwlog-adapter/src/lib.rs`, add:

```rust
mod route;

pub use route::{
    PinnedScopeParsers, RouteSnapshot, StaticRouteGroup, GENERIC_KV_PARSER_ID,
    RULE_BASED_PARSER_ID, SANGFOR_PARSER_ID,
};
```

- [ ] **Step 5: Reject duplicate TOML rule names**

In `crates/fwlog-adapter/src/rule.rs`, update `RuleBasedParser::from_rules` before compiling:

```rust
let mut seen = std::collections::BTreeSet::new();
for rule in &rules {
    if !seen.insert(rule.name.clone()) {
        return Err(format!("duplicate rule name '{}' in ruleset", rule.name));
    }
}
```

Add test:

```rust
#[test]
fn duplicate_rule_names_are_rejected() {
    let rule = ParseRule {
        name: "Duplicate".to_string(),
        priority: 1,
        match_pattern: r"action=(?P<action>\S+)".to_string(),
        field_mappings: vec![FieldMapping {
            target: "action".to_string(),
            source: "action".to_string(),
            transform: None,
        }],
    };

    let err = RuleBasedParser::from_rules(vec![rule.clone(), rule]).unwrap_err();
    assert!(err.contains("duplicate rule name"));
}
```

- [ ] **Step 6: Run tests**

```powershell
cargo test -p fwlog-adapter default_route_snapshot_preserves_static_family_order pinned_scope_ids_are_first_without_sorting duplicate_rule_names_are_rejected
```

Expected: pass.

- [ ] **Step 7: Commit**

```powershell
git add crates\fwlog-adapter\src\route.rs crates\fwlog-adapter\src\lib.rs crates\fwlog-adapter\src\rule.rs
git commit -m "feat: add parser route snapshots"
```

---

### Task 5: Add Bounded Adaptive Field Extractor

**Files:**

- Create: `crates/fwlog-adapter/src/adaptive.rs`
- Modify: `crates/fwlog-adapter/src/lib.rs`
- Modify: `crates/fwlog-adapter/Cargo.toml`

- [ ] **Step 1: Add failing extractor tests**

Create `crates/fwlog-adapter/src/adaptive.rs` with tests first:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extractor_returns_borrowed_pairs() {
        let raw = "src=192.168.1.1 dst:10.0.0.1 action=allow";
        let mut diagnostics = ExtractDiagnostics::default();
        let pairs = extract_generic_pairs::<8>(raw, 8192, &mut diagnostics);

        assert_eq!(pairs.pairs.len(), 3);
        assert_eq!(pairs.pairs[0].key, "src");
        assert_eq!(pairs.pairs[0].value, "192.168.1.1");
        assert_eq!(pairs.pairs[1].key, "dst");
        assert_eq!(pairs.pairs[1].value, "10.0.0.1");
        assert!(!diagnostics.pairs_truncated);
    }

    #[test]
    fn extractor_truncates_pairs_without_panic() {
        let raw = "a=1 b=2 c=3";
        let mut diagnostics = ExtractDiagnostics::default();
        let pairs = extract_generic_pairs::<2>(raw, 8192, &mut diagnostics);

        assert_eq!(pairs.pairs.len(), 2);
        assert!(diagnostics.pairs_truncated);
    }

    #[test]
    fn long_line_without_safe_boundary_is_skipped() {
        let raw = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa=1";
        let mut diagnostics = ExtractDiagnostics::default();
        let pairs = extract_generic_pairs::<8>(raw, 8, &mut diagnostics);

        assert_eq!(pairs.pairs.len(), 0);
        assert_eq!(
            diagnostics.reason.as_deref(),
            Some("line_too_long_no_safe_boundary")
        );
    }

    #[test]
    fn extractor_handles_utf8_keys_without_splitting_boundaries() {
        let raw = "源IP:192.168.1.1 目的IP:10.0.0.1";
        let mut diagnostics = ExtractDiagnostics::default();
        let pairs = extract_generic_pairs::<8>(raw, 8192, &mut diagnostics);

        assert_eq!(pairs.pairs.len(), 2);
        assert_eq!(pairs.pairs[0].key, "源IP");
        assert_eq!(pairs.pairs[1].key, "目的IP");
    }
}
```

- [ ] **Step 2: Run extractor tests and verify failure**

```powershell
cargo test -p fwlog-adapter extractor_returns_borrowed_pairs extractor_truncates_pairs_without_panic long_line_without_safe_boundary_is_skipped
```

Expected: fail because extractor API is missing.

- [ ] **Step 3: Implement bounded extractor**

Replace `crates/fwlog-adapter/src/adaptive.rs` with:

```rust
use arrayvec::ArrayVec;
use memchr::memchr3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenericPair<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenericPairs<'a, const N: usize> {
    pub pairs: ArrayVec<GenericPair<'a>, N>,
}

impl<'a, const N: usize> Default for GenericPairs<'a, N> {
    fn default() -> Self {
        Self {
            pairs: ArrayVec::new(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExtractDiagnostics {
    pub pairs_truncated: bool,
    pub line_truncated: bool,
    pub reason: Option<String>,
}

pub fn extract_generic_pairs<'a, const N: usize>(
    raw: &'a str,
    max_line_bytes: usize,
    diagnostics: &mut ExtractDiagnostics,
) -> GenericPairs<'a, N> {
    *diagnostics = ExtractDiagnostics::default();
    let Some(input) = safe_prefix(raw, max_line_bytes, diagnostics) else {
        return GenericPairs::default();
    };

    let mut out = GenericPairs::default();
    let mut cursor = 0;

    while cursor < input.len() {
        cursor = skip_delimiters(input, cursor);
        let key_start = cursor;
        cursor = take_while(input, cursor, is_key_char);
        if key_start == cursor {
            cursor = advance_one(input, cursor);
            continue;
        }
        let key_end = cursor;
        cursor = skip_spaces(input, cursor);
        let Some((separator_idx, separator)) = char_at(input, cursor) else {
            break;
        };
        if separator != '=' && separator != ':' {
            cursor = advance_one(input, separator_idx);
            continue;
        }
        cursor = separator_idx + separator.len_utf8();
        cursor = skip_spaces(input, cursor);
        let value_start = cursor;
        cursor = take_while(input, cursor, |ch| !is_delimiter(ch));
        let value_end = cursor;
        if value_start == value_end {
            continue;
        }

        let pair = GenericPair {
            key: &input[key_start..key_end],
            value: trim_quotes(&input[value_start..value_end]),
        };
        if out.pairs.try_push(pair).is_err() {
            diagnostics.pairs_truncated = true;
            break;
        }
    }

    out
}

fn safe_prefix<'a>(
    raw: &'a str,
    max_line_bytes: usize,
    diagnostics: &mut ExtractDiagnostics,
) -> Option<&'a str> {
    if raw.len() <= max_line_bytes {
        return Some(raw);
    }
    let limit = max_line_bytes.min(raw.len());
    let mut last_boundary = None;
    let mut offset = 0;
    while offset < limit {
        if let Some(found) = memchr3(b' ', b',', b';', &raw.as_bytes()[offset..limit]) {
            last_boundary = Some(offset + found);
            offset += found + 1;
        } else {
            break;
        }
    }
    let Some(boundary) = last_boundary else {
        diagnostics.reason = Some("line_too_long_no_safe_boundary".to_string());
        return None;
    };
    diagnostics.line_truncated = true;
    Some(&raw[..boundary])
}

fn char_at(input: &str, cursor: usize) -> Option<(usize, char)> {
    input[cursor..].char_indices().next().map(|(offset, ch)| (cursor + offset, ch))
}

fn advance_one(input: &str, cursor: usize) -> usize {
    char_at(input, cursor)
        .map(|(idx, ch)| idx + ch.len_utf8())
        .unwrap_or(input.len())
}

fn take_while(input: &str, mut cursor: usize, predicate: impl Fn(char) -> bool) -> usize {
    while let Some((idx, ch)) = char_at(input, cursor) {
        if !predicate(ch) {
            return idx;
        }
        cursor = idx + ch.len_utf8();
    }
    input.len()
}

fn skip_spaces(input: &str, cursor: usize) -> usize {
    take_while(input, cursor, |ch| ch.is_whitespace())
}

fn skip_delimiters(input: &str, cursor: usize) -> usize {
    take_while(input, cursor, is_delimiter)
}

fn is_key_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_' || ch as u32 >= 0x80
}

fn is_delimiter(ch: char) -> bool {
    ch.is_whitespace() || matches!(ch, ',' | ';' | '|')
}

fn trim_quotes(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .or_else(|| value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
        .unwrap_or(value)
}
```

- [ ] **Step 4: Wire module and dependencies**

In `crates/fwlog-adapter/Cargo.toml`, add:

```toml
memchr = "2"
```

In `crates/fwlog-adapter/src/lib.rs`, add:

```rust
mod adaptive;

pub use adaptive::{extract_generic_pairs, ExtractDiagnostics, GenericPair, GenericPairs};
```

- [ ] **Step 5: Run tests**

```powershell
cargo test -p fwlog-adapter extractor_returns_borrowed_pairs extractor_truncates_pairs_without_panic long_line_without_safe_boundary_is_skipped
cargo test -p fwlog-adapter
```

Expected: pass.

- [ ] **Step 6: Commit**

```powershell
git add crates\fwlog-adapter\Cargo.toml crates\fwlog-adapter\src\adaptive.rs crates\fwlog-adapter\src\lib.rs Cargo.lock
git commit -m "feat: add bounded adaptive extractor"
```

---

### Task 6: Add Parser Metrics Batch And Scope State DTOs

**Files:**

- Create: `crates/fwlog-adapter/src/control.rs`
- Modify: `crates/fwlog-adapter/src/lib.rs`

- [ ] **Step 1: Add failing control tests**

Create `crates/fwlog-adapter/src/control.rs` with tests first:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use fwlog_domain::ParseStatus;

    #[test]
    fn local_batch_aggregates_by_scope_parser_and_status() {
        let mut batch = LocalParserMetricsBatch::default();
        batch.record("source:udp://192.168.1.10", "parser:sangfor_nat_v1", ParseStatus::Parsed);
        batch.record("source:udp://192.168.1.10", "parser:sangfor_nat_v1", ParseStatus::Parsed);
        batch.record("source:udp://192.168.1.10", "parser:sangfor_nat_v1", ParseStatus::Partial);

        let flush = batch.flush();
        assert_eq!(flush.profile_deltas.len(), 2);
        assert!(flush.profile_deltas.iter().any(|delta| {
            delta.scope_key == "source:udp://192.168.1.10"
                && delta.parser_id == "parser:sangfor_nat_v1"
                && delta.status == ParseStatus::Parsed
                && delta.count == 2
        }));
    }

    #[test]
    fn overflow_marks_metrics_gap() {
        let mut batch = LocalParserMetricsBatch::with_max_entries(1);
        batch.record("source:a", "parser:a", ParseStatus::Parsed);
        batch.record("source:b", "parser:b", ParseStatus::Failed);

        assert_eq!(batch.dropped_batches, 1);
        assert!(batch.scope_gaps.contains(&"source:b".to_string()));
    }
}
```

- [ ] **Step 2: Run control tests and verify failure**

```powershell
cargo test -p fwlog-adapter local_batch_aggregates_by_scope_parser_and_status overflow_marks_metrics_gap
```

Expected: fail because control types are missing.

- [ ] **Step 3: Implement local metrics batch**

Replace `crates/fwlog-adapter/src/control.rs` with:

```rust
use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use fwlog_domain::ParseStatus;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MetricsKey {
    scope_key: String,
    parser_id: String,
    status: ParseStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParserProfileDelta {
    pub scope_key: String,
    pub parser_id: String,
    pub status: ParseStatus,
    pub count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetricsFlushEvent {
    pub profile_deltas: Vec<ParserProfileDelta>,
    pub metrics_gap_scopes: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct LocalParserMetricsBatch {
    max_entries: usize,
    counts: BTreeMap<MetricsKey, u64>,
    pub dropped_batches: u64,
    pub scope_gaps: BTreeSet<String>,
}

impl Default for LocalParserMetricsBatch {
    fn default() -> Self {
        Self::with_max_entries(4096)
    }
}

impl LocalParserMetricsBatch {
    pub fn with_max_entries(max_entries: usize) -> Self {
        Self {
            max_entries,
            counts: BTreeMap::new(),
            dropped_batches: 0,
            scope_gaps: BTreeSet::new(),
        }
    }

    pub fn record(&mut self, scope_key: &str, parser_id: &str, status: ParseStatus) {
        let key = MetricsKey {
            scope_key: scope_key.to_string(),
            parser_id: parser_id.to_string(),
            status,
        };
        if !self.counts.contains_key(&key) && self.counts.len() >= self.max_entries {
            self.dropped_batches += 1;
            self.scope_gaps.insert(scope_key.to_string());
            return;
        }
        *self.counts.entry(key).or_insert(0) += 1;
    }

    pub fn flush(&mut self) -> MetricsFlushEvent {
        let profile_deltas = std::mem::take(&mut self.counts)
            .into_iter()
            .map(|(key, count)| ParserProfileDelta {
                scope_key: key.scope_key,
                parser_id: key.parser_id,
                status: key.status,
                count,
            })
            .collect();
        let metrics_gap_scopes = std::mem::take(&mut self.scope_gaps).into_iter().collect();
        MetricsFlushEvent {
            profile_deltas,
            metrics_gap_scopes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParserScopeState {
    pub scope_key: String,
    pub source_high_entropy: bool,
    pub adaptive_learning_enabled: bool,
    pub unknown_source_bucket: bool,
    pub metrics_gap: bool,
    pub metrics_gap_since: Option<DateTime<Utc>>,
    pub malformed_flood_until: Option<DateTime<Utc>>,
    pub shadow_rule_cooldown_until: Option<DateTime<Utc>>,
    pub adaptive_quarantine_until: Option<DateTime<Utc>>,
    pub quarantine_backoff_seconds: i64,
    pub quarantine_attempts: i64,
    pub last_state_change: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
}
```

- [ ] **Step 4: Wire module**

In `crates/fwlog-adapter/src/lib.rs`, add:

```rust
mod control;

pub use control::{
    LocalParserMetricsBatch, MetricsFlushEvent, ParserProfileDelta, ParserScopeState,
};
```

- [ ] **Step 5: Run tests**

```powershell
cargo test -p fwlog-adapter local_batch_aggregates_by_scope_parser_and_status overflow_marks_metrics_gap
```

Expected: pass.

- [ ] **Step 6: Commit**

```powershell
git add crates\fwlog-adapter\src\control.rs crates\fwlog-adapter\src\lib.rs
git commit -m "feat: add parser metrics batch types"
```

---

### Task 7: Add Adaptive Parser Storage Tables And Read Accessors

**Files:**

- Modify: `crates/fwlog-storage/src/duckdb.rs`
- Modify: `crates/fwlog-storage/src/lib.rs`

- [ ] **Step 1: Add failing storage schema/read tests**

Add to `crates/fwlog-storage/src/duckdb.rs` tests:

```rust
#[test]
fn initializes_parser_adaptive_tables() {
    let store = temp_store();
    let profiles = store.list_parser_profiles().unwrap();
    let rules = store.list_adaptive_field_rules().unwrap();
    let diagnostics = store.list_parser_diagnostics().unwrap();
    let scopes = store.list_parser_scopes().unwrap();

    assert!(profiles.is_empty());
    assert!(rules.is_empty());
    assert!(diagnostics.is_empty());
    assert!(scopes.is_empty());
}
```

- [ ] **Step 2: Run storage test and verify failure**

```powershell
cargo test -p fwlog-storage initializes_parser_adaptive_tables
```

Expected: fail because accessor methods and tables are missing.

- [ ] **Step 3: Add DTOs**

In `crates/fwlog-storage/src/duckdb.rs`, add public structs near existing query DTOs:

```rust
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ParserProfileRow {
    pub scope_key: String,
    pub parser_id: String,
    pub parser_name: String,
    pub success_count: i64,
    pub partial_count: i64,
    pub fail_count: i64,
    pub last_seen: String,
    pub priority_boost: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AdaptiveFieldRuleRow {
    pub rule_id: String,
    pub scope_key: String,
    pub raw_key: String,
    pub canonical_field: String,
    pub value_type: String,
    pub status: String,
    pub confidence: f64,
    pub wins: i64,
    pub sample_count: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ParserDiagnosticRow {
    pub fingerprint: String,
    pub scope_key: Option<String>,
    pub reason: String,
    pub sample_raw: Option<String>,
    pub sample_raw_truncated: bool,
    pub count: i64,
    pub suggested_rule_id: Option<String>,
    pub last_seen: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ParserScopeRow {
    pub scope_key: String,
    pub source_high_entropy: bool,
    pub adaptive_learning_enabled: bool,
    pub unknown_source_bucket: bool,
    pub metrics_gap: bool,
    pub quarantine_backoff_seconds: i64,
    pub quarantine_attempts: i64,
    pub last_seen: String,
}
```

- [ ] **Step 4: Add schema creation**

In the function that initializes DuckDB tables, add:

```rust
conn.execute_batch(
    r#"
    CREATE TABLE IF NOT EXISTS parser_profiles (
      scope_key TEXT NOT NULL,
      parser_id TEXT NOT NULL,
      parser_name TEXT NOT NULL,
      success_count BIGINT NOT NULL,
      partial_count BIGINT NOT NULL,
      fail_count BIGINT NOT NULL,
      last_seen TIMESTAMPTZ NOT NULL,
      priority_boost DOUBLE NOT NULL,
      PRIMARY KEY (scope_key, parser_id)
    );

    CREATE TABLE IF NOT EXISTS adaptive_field_rules (
      rule_id TEXT PRIMARY KEY,
      scope_key TEXT NOT NULL,
      raw_key TEXT NOT NULL,
      canonical_field TEXT NOT NULL,
      value_type TEXT NOT NULL,
      status TEXT NOT NULL,
      confidence DOUBLE NOT NULL,
      wins BIGINT NOT NULL,
      sample_count BIGINT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      activated_at TIMESTAMPTZ,
      disabled_at TIMESTAMPTZ,
      disabled_reason TEXT,
      recovery_sample_rate DOUBLE,
      recovery_attempts BIGINT,
      last_recovery_at TIMESTAMPTZ
    );

    CREATE TABLE IF NOT EXISTS parser_scope_state (
      scope_key TEXT PRIMARY KEY,
      source_high_entropy BOOLEAN NOT NULL,
      adaptive_learning_enabled BOOLEAN NOT NULL,
      unknown_source_bucket BOOLEAN NOT NULL,
      metrics_gap BOOLEAN NOT NULL,
      metrics_gap_since TIMESTAMPTZ,
      malformed_flood_until TIMESTAMPTZ,
      shadow_rule_cooldown_until TIMESTAMPTZ,
      adaptive_quarantine_until TIMESTAMPTZ,
      quarantine_backoff_seconds BIGINT NOT NULL,
      quarantine_attempts BIGINT NOT NULL,
      last_state_change TIMESTAMPTZ NOT NULL,
      last_seen TIMESTAMPTZ NOT NULL
    );

    CREATE TABLE IF NOT EXISTS parser_diagnostics (
      fingerprint TEXT PRIMARY KEY,
      scope_key TEXT,
      reason TEXT NOT NULL,
      sample_raw TEXT,
      sample_raw_truncated BOOLEAN NOT NULL,
      count BIGINT NOT NULL,
      suggested_rule_id TEXT,
      last_seen TIMESTAMPTZ NOT NULL
    );

    CREATE TABLE IF NOT EXISTS source_device_aliases (
      source_key TEXT NOT NULL,
      raw_source_addr TEXT NOT NULL,
      device_id TEXT NOT NULL,
      first_seen TIMESTAMPTZ NOT NULL,
      last_seen TIMESTAMPTZ NOT NULL,
      confidence DOUBLE NOT NULL,
      PRIMARY KEY (source_key, raw_source_addr, device_id)
    );

    CREATE TABLE IF NOT EXISTS parser_checkpoint_version (
      snapshot_version BIGINT PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL,
      published_at TIMESTAMPTZ,
      status TEXT NOT NULL,
      profiles_count BIGINT NOT NULL,
      rules_count BIGINT NOT NULL,
      diagnostics_count BIGINT NOT NULL,
      scope_state_count BIGINT NOT NULL,
      aliases_count BIGINT NOT NULL
    );
    "#,
)?;
```

- [ ] **Step 5: Add read accessors**

Add methods on `DuckDbStore`:

```rust
pub fn list_parser_profiles(&self) -> Result<Vec<ParserProfileRow>> {
    let conn = self.connection();
    let mut stmt = conn.prepare(
        "SELECT scope_key, parser_id, parser_name, success_count, partial_count, fail_count,
                CAST(last_seen AS TEXT), priority_boost
         FROM parser_profiles
         ORDER BY last_seen DESC, scope_key, parser_id",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(ParserProfileRow {
            scope_key: row.get(0)?,
            parser_id: row.get(1)?,
            parser_name: row.get(2)?,
            success_count: row.get(3)?,
            partial_count: row.get(4)?,
            fail_count: row.get(5)?,
            last_seen: row.get(6)?,
            priority_boost: row.get(7)?,
        })
    })?;
    rows.collect::<duckdb::Result<Vec<_>>>().map_err(Into::into)
}
```

Add the remaining read methods on `DuckDbStore`:

```rust
pub fn list_adaptive_field_rules(&self) -> Result<Vec<AdaptiveFieldRuleRow>> {
    let conn = self.connection();
    let mut stmt = conn.prepare(
        "SELECT rule_id, scope_key, raw_key, canonical_field, value_type, status,
                confidence, wins, sample_count
         FROM adaptive_field_rules
         ORDER BY scope_key, raw_key, canonical_field",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(AdaptiveFieldRuleRow {
            rule_id: row.get(0)?,
            scope_key: row.get(1)?,
            raw_key: row.get(2)?,
            canonical_field: row.get(3)?,
            value_type: row.get(4)?,
            status: row.get(5)?,
            confidence: row.get(6)?,
            wins: row.get(7)?,
            sample_count: row.get(8)?,
        })
    })?;
    rows.collect::<duckdb::Result<Vec<_>>>().map_err(Into::into)
}

pub fn list_parser_diagnostics(&self) -> Result<Vec<ParserDiagnosticRow>> {
    let conn = self.connection();
    let mut stmt = conn.prepare(
        "SELECT fingerprint, scope_key, reason, sample_raw, sample_raw_truncated,
                count, suggested_rule_id, CAST(last_seen AS TEXT)
         FROM parser_diagnostics
         ORDER BY last_seen DESC, fingerprint",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(ParserDiagnosticRow {
            fingerprint: row.get(0)?,
            scope_key: row.get(1)?,
            reason: row.get(2)?,
            sample_raw: row.get(3)?,
            sample_raw_truncated: row.get(4)?,
            count: row.get(5)?,
            suggested_rule_id: row.get(6)?,
            last_seen: row.get(7)?,
        })
    })?;
    rows.collect::<duckdb::Result<Vec<_>>>().map_err(Into::into)
}

pub fn list_parser_scopes(&self) -> Result<Vec<ParserScopeRow>> {
    let conn = self.connection();
    let mut stmt = conn.prepare(
        "SELECT scope_key, source_high_entropy, adaptive_learning_enabled,
                unknown_source_bucket, metrics_gap, quarantine_backoff_seconds,
                quarantine_attempts, CAST(last_seen AS TEXT)
         FROM parser_scope_state
         ORDER BY last_seen DESC, scope_key",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(ParserScopeRow {
            scope_key: row.get(0)?,
            source_high_entropy: row.get(1)?,
            adaptive_learning_enabled: row.get(2)?,
            unknown_source_bucket: row.get(3)?,
            metrics_gap: row.get(4)?,
            quarantine_backoff_seconds: row.get(5)?,
            quarantine_attempts: row.get(6)?,
            last_seen: row.get(7)?,
        })
    })?;
    rows.collect::<duckdb::Result<Vec<_>>>().map_err(Into::into)
}
```

- [ ] **Step 6: Export DTOs**

In `crates/fwlog-storage/src/lib.rs`, extend the existing `pub use duckdb::{ DeviceBinding, DuckDbStore, EventQuery, FrozenArchiveIndex, IpRegionCacheEntry, MinuteMetricPoint, MinuteMetricQuery, SourceMetricBucket, SourceMetricQuery }` list with:

```rust
AdaptiveFieldRuleRow, ParserDiagnosticRow, ParserProfileRow, ParserScopeRow,
```

- [ ] **Step 7: Run storage tests**

```powershell
cargo test -p fwlog-storage initializes_parser_adaptive_tables
cargo test -p fwlog-storage
```

Expected: pass.

- [ ] **Step 8: Commit**

```powershell
git add crates\fwlog-storage\src\duckdb.rs crates\fwlog-storage\src\lib.rs
git commit -m "feat: add parser adaptive storage tables"
```

---

### Task 8: Add Read-Only Parser Adaptive API Endpoints

**Files:**

- Modify: `crates/fwlog-api/src/lib.rs`
- Modify: `crates/fwlog-api/src/handlers.rs`

- [ ] **Step 1: Add failing route tests**

Add to `crates/fwlog-api/src/handlers.rs` tests:

```rust
#[tokio::test]
async fn parser_scopes_endpoint_returns_empty_list() {
    let dir = tempfile::tempdir().unwrap();
    let app = crate::router(
        dir.path().join("oxidelog.duckdb"),
        dir.path().join("parquet"),
        dir.path().join("frozen"),
    );
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/parser/scopes")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}
```

- [ ] **Step 2: Run API route test and verify failure**

```powershell
cargo test -p fwlog-api parser_scopes_endpoint_returns_empty_list
```

Expected: fail because the route does not exist.

- [ ] **Step 3: Add handlers**

In `crates/fwlog-api/src/handlers.rs`, add:

```rust
pub async fn parser_profiles(Extension(state): Extension<ApiState>) -> Response {
    match state.store.list_parser_profiles() {
        Ok(rows) => Json(rows).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": err.to_string() })),
        )
            .into_response(),
    }
}

pub async fn parser_adaptive_rules(Extension(state): Extension<ApiState>) -> Response {
    match state.store.list_adaptive_field_rules() {
        Ok(rows) => Json(rows).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": err.to_string() })),
        )
            .into_response(),
    }
}

pub async fn parser_diagnostics(Extension(state): Extension<ApiState>) -> Response {
    match state.store.list_parser_diagnostics() {
        Ok(rows) => Json(rows).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": err.to_string() })),
        )
            .into_response(),
    }
}

pub async fn parser_scopes(Extension(state): Extension<ApiState>) -> Response {
    match state.store.list_parser_scopes() {
        Ok(rows) => Json(rows).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": err.to_string() })),
        )
            .into_response(),
    }
}
```

The storage DTOs derive `Serialize` in Task 7, so `Json(rows)` can serialize them directly.

- [ ] **Step 4: Register routes**

In `crates/fwlog-api/src/lib.rs`, add routes near the existing parser summary route:

```rust
.route("/api/parser/profiles", get(handlers::parser_profiles))
.route("/api/parser/adaptive/rules", get(handlers::parser_adaptive_rules))
.route("/api/parser/diagnostics", get(handlers::parser_diagnostics))
.route("/api/parser/scopes", get(handlers::parser_scopes))
```

- [ ] **Step 5: Run API tests**

```powershell
cargo test -p fwlog-api parser_scopes_endpoint_returns_empty_list
cargo test -p fwlog-api parser
```

Expected: pass.

- [ ] **Step 6: Commit**

```powershell
git add crates\fwlog-api\src\lib.rs crates\fwlog-api\src\handlers.rs crates\fwlog-storage\src\duckdb.rs
git commit -m "feat: expose parser adaptive state api"
```

---

### Task 9: Wire Compatibility Parser Path Through Live And Import Tests

**Files:**

- Modify: `apps/fwlogd/src/pipeline.rs`
- Modify: `apps/fwlog-import/src/main.rs`
- Modify: `crates/fwlog-adapter/src/lib.rs`

- [ ] **Step 1: Add shared-engine behavior tests**

In `crates/fwlog-adapter/src/lib.rs`, add:

```rust
#[test]
fn parse_wrapper_matches_parse_inner_event() {
    let engine = ParserEngine::new();
    let line = "src=10.0.0.1 dst=10.0.0.2 sport=12345 dport=443 proto=TCP action=deny";
    let raw_for_parse = raw(line);
    let raw_for_inner = raw(line);

    let wrapped = engine.parse(raw_for_parse);
    let mut diagnostics = ParseDiagnosticsBuffer::default();
    let inner = engine.parse_inner(raw_for_inner, &mut diagnostics).event;

    assert_eq!(wrapped, inner);
    assert_eq!(diagnostics.matched_parser_id.as_deref(), Some("parser:generic_kv_v1"));
}
```

- [ ] **Step 2: Run focused tests**

```powershell
cargo test -p fwlog-adapter parse_wrapper_matches_parse_inner_event
```

Expected: pass after earlier tasks.

- [ ] **Step 3: Confirm live ingest still calls shared parser**

In `apps/fwlogd/src/pipeline.rs`, keep parser creation as:

```rust
let parser = ParserEngine::new();
```

Keep event creation as:

```rust
batch.push(parser.parse(raw));
```

Do not add a separate live-only parser.

- [ ] **Step 4: Confirm historical import still calls shared parser**

In `apps/fwlog-import/src/main.rs`, keep line parsing as:

```rust
parser.parse(raw)
```

For reparse mode, keep the source address and raw payload:

```rust
let mut reparsed = ParserEngine::new().parse(RawLog {
    ingest_time: event.ingest_time,
    source_addr: event.source_addr.clone(),
    raw: event.raw.clone(),
});
```

- [ ] **Step 5: Run pipeline/import tests or build checks**

```powershell
cargo test -p fwlog-adapter parse_wrapper_matches_parse_inner_event
cargo test -p fwlog-import
cargo test -p fwlogd
```

If `fwlog-import` or `fwlogd` have no tests, run:

```powershell
cargo check -p fwlog-import
cargo check -p fwlogd
```

Expected: tests pass or checks complete successfully.

- [ ] **Step 6: Run foundation regression suite**

```powershell
cargo test -p fwlog-domain
cargo test -p fwlog-adapter
cargo test -p fwlog-storage
cargo test -p fwlog-api
cargo check -p fwlog-import
cargo check -p fwlogd
```

Expected: all pass.

- [ ] **Step 7: Commit**

```powershell
git add crates\fwlog-adapter\src\lib.rs apps\fwlogd\src\pipeline.rs apps\fwlog-import\src\main.rs
git commit -m "test: verify shared parser compatibility path"
```

---

## Plan Self-Review

Spec coverage in this plan:

- `Partial` status: Task 1.
- Compatibility `parse_inner`: Task 2.
- Source normalization and unknown buckets: Task 3.
- Pinned ids and route snapshots: Task 4.
- Bounded extractor and checked `ArrayVec` insertion: Task 5.
- Metrics batching data shapes and metrics gaps: Task 6.
- Adaptive storage tables and checkpoint metadata: Task 7.
- Read-only operational API: Task 8.
- Live/import shared parser path: Task 9.

Spec items intentionally deferred:

- Wilson score activation and suggested-rule promotion.
- Active adaptive rule application.
- Rollback attribution enforcement, malformed-flood recovery, and quarantine staged recovery.
- Background control task, checkpoint writer loop, and MPSC integration.
- Parser-kernel mode.
- UI.

Implementation notes:

- Keep each commit focused and run the listed tests before committing.
- Do not refactor unrelated API/export/archive code while touching `handlers.rs` and `duckdb.rs`.
- Preserve existing TOML rule behavior except duplicate rule-name rejection.
- The existing source files contain mojibake comments. Do not churn comments unless a touched block needs a short English clarification.
