use std::collections::BTreeMap;
use std::net::IpAddr;

use chrono::{DateTime, Utc};
use fwlog_domain::{CanonicalEvent, ParseStatus};

use crate::{normalize_source_scope, ParseDiagnosticsBuffer, RuleId, ScopeNormalizationMode};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveRuleStatus {
    Shadow,
    ShadowRecovering,
    Active,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CanonicalField {
    SrcIp,
    SrcPort,
    DstIp,
    DstPort,
    Protocol,
    Action,
    Severity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveValueType {
    Ip,
    Port,
    Protocol,
    Action,
    String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AdaptiveFieldRule {
    pub rule_id: String,
    pub scope_key: String,
    pub raw_key: String,
    pub canonical_field: CanonicalField,
    pub value_type: AdaptiveValueType,
    pub status: AdaptiveRuleStatus,
    pub confidence: f64,
    pub wins: u64,
    pub sample_count: u64,
    pub disabled_reason: Option<String>,
}

impl AdaptiveFieldRule {
    pub fn active(
        rule_id: impl Into<String>,
        scope_key: impl Into<String>,
        raw_key: impl Into<String>,
        canonical_field: CanonicalField,
        value_type: AdaptiveValueType,
    ) -> Self {
        Self {
            rule_id: rule_id.into(),
            scope_key: scope_key.into(),
            raw_key: raw_key.into(),
            canonical_field,
            value_type,
            status: AdaptiveRuleStatus::Active,
            confidence: 1.0,
            wins: 0,
            sample_count: 0,
            disabled_reason: None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct AdaptiveRuleSnapshot {
    rules: Vec<AdaptiveFieldRule>,
}

impl AdaptiveRuleSnapshot {
    pub fn from_rules(rules: Vec<AdaptiveFieldRule>) -> Self {
        Self { rules }
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn rules(&self) -> &[AdaptiveFieldRule] {
        &self.rules
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ActiveRuleApplyResult {
    pub applied: usize,
    pub conflicts: usize,
}

#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
#[serde(default)]
pub struct AdaptiveLearningConfig {
    pub suggested_rule_min_samples: u64,
    pub activation_wilson_lower_bound: f64,
    pub wilson_z: f64,
    pub auto_activate: bool,
    pub rollback_min_samples: u64,
    pub rollback_failure_ratio: f64,
    pub rollback_conflict_ratio: f64,
}

impl Default for AdaptiveLearningConfig {
    fn default() -> Self {
        Self {
            suggested_rule_min_samples: 100,
            activation_wilson_lower_bound: 0.95,
            wilson_z: 1.96,
            auto_activate: true,
            rollback_min_samples: 50,
            rollback_failure_ratio: 0.25,
            rollback_conflict_ratio: 0.10,
        }
    }
}

impl AdaptiveLearningConfig {
    pub fn test_defaults() -> Self {
        Self {
            suggested_rule_min_samples: 4,
            activation_wilson_lower_bound: 0.70,
            wilson_z: 1.96,
            auto_activate: true,
            rollback_min_samples: 4,
            rollback_failure_ratio: 0.50,
            rollback_conflict_ratio: 0.50,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct RuleCounters {
    post_activation_failed: u64,
    post_activation_conflicts: u64,
    post_activation_total: u64,
}

#[derive(Debug, Clone)]
pub struct AdaptiveControlState {
    config: AdaptiveLearningConfig,
    rules: BTreeMap<String, AdaptiveFieldRule>,
    counters: BTreeMap<String, RuleCounters>,
}

impl AdaptiveControlState {
    pub fn new(config: AdaptiveLearningConfig) -> Self {
        Self {
            config,
            rules: BTreeMap::new(),
            counters: BTreeMap::new(),
        }
    }

    pub fn with_active_rule(rule: AdaptiveFieldRule) -> Self {
        let mut state = Self::new(AdaptiveLearningConfig::test_defaults());
        state.rules.insert(rule.rule_id.clone(), rule);
        state
    }

    pub fn from_rules(config: AdaptiveLearningConfig, rules: Vec<AdaptiveFieldRule>) -> Self {
        let mut state = Self::new(config);
        for rule in rules {
            state.rules.insert(rule.rule_id.clone(), rule);
        }
        state
    }

    pub fn record_shadow_result(
        &mut self,
        scope_key: &str,
        raw_key: &str,
        canonical_field: CanonicalField,
        value_type: AdaptiveValueType,
        won: bool,
    ) {
        let rule_id = adaptive_rule_id(scope_key, raw_key, canonical_field);
        let rule = self
            .rules
            .entry(rule_id.clone())
            .or_insert_with(|| AdaptiveFieldRule {
                rule_id,
                scope_key: scope_key.to_string(),
                raw_key: raw_key.to_string(),
                canonical_field,
                value_type,
                status: AdaptiveRuleStatus::Shadow,
                confidence: 0.0,
                wins: 0,
                sample_count: 0,
                disabled_reason: None,
            });

        if matches!(
            rule.status,
            AdaptiveRuleStatus::Shadow | AdaptiveRuleStatus::ShadowRecovering
        ) {
            rule.sample_count = rule.sample_count.saturating_add(1);
            if won {
                rule.wins = rule.wins.saturating_add(1);
            }
            rule.confidence =
                wilson_lower_bound(rule.wins, rule.sample_count, self.config.wilson_z);
        }
    }

    pub fn evaluate_rules(&mut self, _now: DateTime<Utc>) {
        if !self.config.auto_activate {
            return;
        }

        for rule in self.rules.values_mut() {
            if rule.status != AdaptiveRuleStatus::Shadow {
                continue;
            }
            if rule.sample_count < self.config.suggested_rule_min_samples {
                continue;
            }
            rule.confidence =
                wilson_lower_bound(rule.wins, rule.sample_count, self.config.wilson_z);
            if rule.confidence >= self.config.activation_wilson_lower_bound {
                rule.status = AdaptiveRuleStatus::Active;
            }
        }
    }

    pub fn active_rules(&self) -> Vec<AdaptiveFieldRule> {
        self.rules
            .values()
            .filter(|rule| rule.status == AdaptiveRuleStatus::Active)
            .cloned()
            .collect()
    }

    pub fn rules(&self) -> Vec<AdaptiveFieldRule> {
        self.rules.values().cloned().collect()
    }

    pub fn rule(&self, rule_id: &str) -> Option<&AdaptiveFieldRule> {
        self.rules.get(rule_id)
    }

    pub fn rule_snapshot(&self) -> AdaptiveRuleSnapshot {
        AdaptiveRuleSnapshot::from_rules(self.active_rules())
    }

    pub fn observe_parse_result(
        &mut self,
        event: &CanonicalEvent,
        _diagnostics: &ParseDiagnosticsBuffer,
    ) {
        if event.parse_status == ParseStatus::Parsed {
            return;
        }
        let scope = normalize_source_scope(&event.source_addr, ScopeNormalizationMode::SourceIp);
        if scope.unknown_source_bucket || !scope.adaptive_learning_enabled {
            return;
        }

        let mut extract_diagnostics = crate::ExtractDiagnostics::default();
        let pairs =
            crate::extract_generic_pairs::<64>(&event.raw, 16 * 1024, &mut extract_diagnostics);
        if extract_diagnostics.pairs_truncated || extract_diagnostics.reason.is_some() {
            return;
        }

        for pair in pairs.pairs {
            let Some((field, value_type)) = candidate_from_pair(event, pair.key, pair.value) else {
                continue;
            };
            if let Some(won) =
                shadow_candidate_result(event, &scope.scope_key, pair.key, field, value_type)
            {
                self.record_shadow_result(&scope.scope_key, pair.key, field, value_type, won);
            }
        }
    }

    pub fn record_applied_rule_result(&mut self, rule_id: &str, status: ParseStatus) {
        let counters = self.counters.entry(rule_id.to_string()).or_default();
        counters.post_activation_total = counters.post_activation_total.saturating_add(1);
        if status != ParseStatus::Parsed {
            counters.post_activation_failed = counters.post_activation_failed.saturating_add(1);
        }
    }

    pub fn record_failed_rule_result(&mut self, rule_id: &str) {
        let counters = self.counters.entry(rule_id.to_string()).or_default();
        counters.post_activation_total = counters.post_activation_total.saturating_add(1);
        counters.post_activation_failed = counters.post_activation_failed.saturating_add(1);
        counters.post_activation_conflicts = counters.post_activation_conflicts.saturating_add(1);
    }

    pub fn evaluate_rollback(&mut self, _now: DateTime<Utc>) {
        for (rule_id, counters) in &self.counters {
            if counters.post_activation_total < self.config.rollback_min_samples {
                continue;
            }
            let bad_ratio =
                counters.post_activation_failed as f64 / counters.post_activation_total as f64;
            let conflict_ratio =
                counters.post_activation_conflicts as f64 / counters.post_activation_total as f64;
            if bad_ratio < self.config.rollback_failure_ratio
                && conflict_ratio < self.config.rollback_conflict_ratio
            {
                continue;
            }
            if let Some(rule) = self.rules.get_mut(rule_id) {
                if rule.status == AdaptiveRuleStatus::Active {
                    rule.status = AdaptiveRuleStatus::Disabled;
                    rule.disabled_reason =
                        Some(if conflict_ratio >= self.config.rollback_conflict_ratio {
                            "rollback: active rule value conversion conflict threshold exceeded"
                                .to_string()
                        } else {
                            "rollback: attributed failure threshold exceeded".to_string()
                        });
                }
            }
        }
    }
}

pub fn wilson_lower_bound(wins: u64, samples: u64, z: f64) -> f64 {
    if samples == 0 {
        return 0.0;
    }
    let n = samples as f64;
    let phat = wins as f64 / n;
    let z2 = z * z;
    let denominator = 1.0 + z2 / n;
    let center = phat + z2 / (2.0 * n);
    let margin = z * ((phat * (1.0 - phat) + z2 / (4.0 * n)) / n).sqrt();
    ((center - margin) / denominator).clamp(0.0, 1.0)
}

fn adaptive_rule_id(scope_key: &str, raw_key: &str, canonical_field: CanonicalField) -> String {
    format!(
        "rule:{}:{}:{}",
        scope_key,
        raw_key,
        canonical_field.as_str()
    )
}

impl CanonicalField {
    pub fn as_str(self) -> &'static str {
        match self {
            CanonicalField::SrcIp => "src_ip",
            CanonicalField::SrcPort => "src_port",
            CanonicalField::DstIp => "dst_ip",
            CanonicalField::DstPort => "dst_port",
            CanonicalField::Protocol => "protocol",
            CanonicalField::Action => "action",
            CanonicalField::Severity => "severity",
        }
    }
}

impl AdaptiveRuleStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            AdaptiveRuleStatus::Shadow => "shadow",
            AdaptiveRuleStatus::ShadowRecovering => "shadow_recovering",
            AdaptiveRuleStatus::Active => "active",
            AdaptiveRuleStatus::Disabled => "disabled",
        }
    }
}

impl AdaptiveValueType {
    pub fn as_str(self) -> &'static str {
        match self {
            AdaptiveValueType::Ip => "ip",
            AdaptiveValueType::Port => "port",
            AdaptiveValueType::Protocol => "protocol",
            AdaptiveValueType::Action => "action",
            AdaptiveValueType::String => "string",
        }
    }
}

pub fn apply_active_rules(
    snapshot: &AdaptiveRuleSnapshot,
    scope_key: &str,
    raw: &str,
    event: &mut CanonicalEvent,
    diagnostics: &mut ParseDiagnosticsBuffer,
) -> ActiveRuleApplyResult {
    let mut result = ActiveRuleApplyResult::default();

    for rule in snapshot.rules() {
        if rule.status != AdaptiveRuleStatus::Active || rule.scope_key != scope_key {
            continue;
        }
        if !field_is_empty(event, rule.canonical_field) {
            continue;
        }
        let Some(value) = find_raw_value(raw, &rule.raw_key) else {
            continue;
        };
        let Some(normalized) = normalize_value(value, rule.value_type) else {
            result.conflicts += 1;
            diagnostics.push_failed_rule(RuleId(rule.rule_id.clone()));
            continue;
        };
        if set_field_if_empty(event, rule.canonical_field, normalized) {
            diagnostics.push_applied_rule(RuleId(rule.rule_id.clone()));
            result.applied += 1;
        }
    }

    if result.applied > 0 {
        event.classify_firewall_tuple();
    }

    result
}

fn find_raw_value<'a>(raw: &'a str, raw_key: &str) -> Option<&'a str> {
    let mut diagnostics = crate::ExtractDiagnostics::default();
    let pairs = crate::extract_generic_pairs::<64>(raw, 16 * 1024, &mut diagnostics);
    pairs
        .pairs
        .iter()
        .find(|pair| pair.key == raw_key)
        .map(|pair| unquote(pair.value))
}

fn unquote(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            value
                .strip_prefix('\'')
                .and_then(|value| value.strip_suffix('\''))
        })
        .unwrap_or(value)
}

fn field_is_empty(event: &CanonicalEvent, field: CanonicalField) -> bool {
    match field {
        CanonicalField::SrcIp => event.src_ip.as_deref().is_none_or(str::is_empty),
        CanonicalField::SrcPort => event.src_port.is_none(),
        CanonicalField::DstIp => event.dst_ip.as_deref().is_none_or(str::is_empty),
        CanonicalField::DstPort => event.dst_port.is_none(),
        CanonicalField::Protocol => event.protocol.as_deref().is_none_or(str::is_empty),
        CanonicalField::Action => event.action.as_deref().is_none_or(str::is_empty),
        CanonicalField::Severity => event.severity.as_deref().is_none_or(str::is_empty),
    }
}

fn set_field_if_empty(event: &mut CanonicalEvent, field: CanonicalField, value: String) -> bool {
    if !field_is_empty(event, field) {
        return false;
    }

    match field {
        CanonicalField::SrcIp => event.src_ip = Some(value),
        CanonicalField::SrcPort => event.src_port = value.parse().ok(),
        CanonicalField::DstIp => event.dst_ip = Some(value),
        CanonicalField::DstPort => event.dst_port = value.parse().ok(),
        CanonicalField::Protocol => event.protocol = Some(value),
        CanonicalField::Action => event.action = Some(value),
        CanonicalField::Severity => event.severity = Some(value),
    }

    true
}

fn normalize_value(value: &str, value_type: AdaptiveValueType) -> Option<String> {
    match value_type {
        AdaptiveValueType::Ip => value.parse::<IpAddr>().ok().map(|_| value.to_string()),
        AdaptiveValueType::Port => value.parse::<u16>().ok().map(|port| port.to_string()),
        AdaptiveValueType::Protocol => Some(match value.to_ascii_lowercase().as_str() {
            "1" | "icmp" => "ICMP".to_string(),
            "6" | "tcp" => "TCP".to_string(),
            "17" | "udp" => "UDP".to_string(),
            _ => value.to_ascii_uppercase(),
        }),
        AdaptiveValueType::Action | AdaptiveValueType::String => Some(value.to_string()),
    }
}

fn candidate_from_pair(
    event: &CanonicalEvent,
    raw_key: &str,
    value: &str,
) -> Option<(CanonicalField, AdaptiveValueType)> {
    let key = raw_key.to_ascii_lowercase();
    if is_src_ip_key(&key)
        && event.src_ip.is_none()
        && normalize_value(value, AdaptiveValueType::Ip).is_some()
    {
        return Some((CanonicalField::SrcIp, AdaptiveValueType::Ip));
    }
    if is_dst_ip_key(&key)
        && event.dst_ip.is_none()
        && normalize_value(value, AdaptiveValueType::Ip).is_some()
    {
        return Some((CanonicalField::DstIp, AdaptiveValueType::Ip));
    }
    if is_src_port_key(&key)
        && event.src_port.is_none()
        && normalize_value(value, AdaptiveValueType::Port).is_some()
    {
        return Some((CanonicalField::SrcPort, AdaptiveValueType::Port));
    }
    if is_dst_port_key(&key)
        && event.dst_port.is_none()
        && normalize_value(value, AdaptiveValueType::Port).is_some()
    {
        return Some((CanonicalField::DstPort, AdaptiveValueType::Port));
    }
    if is_protocol_key(&key)
        && event.protocol.is_none()
        && normalize_value(value, AdaptiveValueType::Protocol).is_some()
    {
        return Some((CanonicalField::Protocol, AdaptiveValueType::Protocol));
    }
    if is_action_key(&key) && event.action.is_none() {
        return Some((CanonicalField::Action, AdaptiveValueType::Action));
    }
    if is_severity_key(&key) && event.severity.is_none() {
        return Some((CanonicalField::Severity, AdaptiveValueType::String));
    }
    None
}

fn shadow_candidate_result(
    event: &CanonicalEvent,
    scope_key: &str,
    raw_key: &str,
    canonical_field: CanonicalField,
    value_type: AdaptiveValueType,
) -> Option<bool> {
    let rule_id = adaptive_rule_id(scope_key, raw_key, canonical_field);
    let snapshot = AdaptiveRuleSnapshot::from_rules(vec![AdaptiveFieldRule::active(
        rule_id,
        scope_key,
        raw_key,
        canonical_field,
        value_type,
    )]);
    let mut simulated = event.clone();
    let before_status = simulated.parse_status;
    let before_empty = field_is_empty(&simulated, canonical_field);
    let mut diagnostics = ParseDiagnosticsBuffer::default();
    let result = apply_active_rules(
        &snapshot,
        scope_key,
        &event.raw,
        &mut simulated,
        &mut diagnostics,
    );

    if result.conflicts > 0 {
        return Some(false);
    }
    if result.applied == 0 {
        return None;
    }

    let field_filled = before_empty && !field_is_empty(&simulated, canonical_field);
    let status_improved =
        before_status != ParseStatus::Parsed && simulated.parse_status == ParseStatus::Parsed;
    Some(field_filled || status_improved)
}

fn is_src_ip_key(key: &str) -> bool {
    matches!(
        key,
        "src" | "source" | "source_ip" | "saddr" | "srcaddr" | "src_ip" | "sip" | "srcip"
    )
}

fn is_dst_ip_key(key: &str) -> bool {
    matches!(
        key,
        "dst"
            | "dest"
            | "destination"
            | "destination_ip"
            | "daddr"
            | "dstaddr"
            | "dst_ip"
            | "dip"
            | "dstip"
            | "target_ip"
            | "targetaddr"
            | "target_addr"
    )
}

fn is_src_port_key(key: &str) -> bool {
    matches!(
        key,
        "sport" | "source_port" | "srcport" | "spt" | "src_port"
    )
}

fn is_dst_port_key(key: &str) -> bool {
    matches!(
        key,
        "dport" | "destination_port" | "dstport" | "dpt" | "dst_port"
    )
}

fn is_protocol_key(key: &str) -> bool {
    matches!(key, "proto" | "protocol" | "prot")
}

fn is_action_key(key: &str) -> bool {
    matches!(
        key,
        "action" | "act" | "disposition" | "actname" | "nat_type"
    )
}

fn is_severity_key(key: &str) -> bool {
    matches!(key, "severity" | "level" | "priority" | "sev")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use fwlog_domain::{CanonicalEvent, ParseStatus, RawLog};

    fn raw(line: &str) -> RawLog {
        RawLog {
            ingest_time: Utc.timestamp_opt(1_778_808_000, 0).unwrap(),
            source_addr: "tcp://127.0.0.1:1514".to_string(),
            raw: line.to_string(),
        }
    }

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

    #[test]
    fn active_rule_does_not_overwrite_existing_deterministic_field() {
        let raw = raw("dstAddr=10.0.0.2 dst=10.0.0.9 src=10.0.0.1 proto=TCP");
        let mut event = CanonicalEvent::failed(raw.clone(), "partial");
        event.src_ip = Some("10.0.0.1".to_string());
        event.dst_ip = Some("10.0.0.9".to_string());
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

        assert_eq!(result.applied, 0);
        assert_eq!(event.dst_ip.as_deref(), Some("10.0.0.9"));
        assert!(diagnostics.applied_rules.is_empty());
    }

    #[test]
    fn shadow_rule_activates_only_after_sample_and_wilson_thresholds() {
        let mut state = AdaptiveControlState::new(AdaptiveLearningConfig {
            suggested_rule_min_samples: 20,
            activation_wilson_lower_bound: 0.80,
            auto_activate: true,
            ..AdaptiveLearningConfig::test_defaults()
        });

        for _ in 0..19 {
            state.record_shadow_result(
                "source:tcp://127.0.0.1",
                "dstAddr",
                CanonicalField::DstIp,
                AdaptiveValueType::Ip,
                true,
            );
        }
        state.evaluate_rules(Utc.timestamp_opt(1_778_808_000, 0).unwrap());
        assert!(state.active_rules().is_empty());

        for _ in 0..181 {
            state.record_shadow_result(
                "source:tcp://127.0.0.1",
                "dstAddr",
                CanonicalField::DstIp,
                AdaptiveValueType::Ip,
                true,
            );
        }
        state.evaluate_rules(Utc.timestamp_opt(1_778_808_060, 0).unwrap());

        assert_eq!(state.active_rules().len(), 1);
        assert!(state.active_rules()[0].confidence >= 0.80);
    }

    #[test]
    fn auto_activate_false_keeps_eligible_rules_in_shadow() {
        let mut state = AdaptiveControlState::new(AdaptiveLearningConfig {
            suggested_rule_min_samples: 20,
            activation_wilson_lower_bound: 0.80,
            auto_activate: false,
            ..AdaptiveLearningConfig::test_defaults()
        });

        for _ in 0..200 {
            state.record_shadow_result(
                "source:tcp://127.0.0.1",
                "dstAddr",
                CanonicalField::DstIp,
                AdaptiveValueType::Ip,
                true,
            );
        }
        state.evaluate_rules(Utc.timestamp_opt(1_778_808_060, 0).unwrap());

        assert!(state.active_rules().is_empty());
        assert_eq!(
            state
                .rule("rule:source:tcp://127.0.0.1:dstAddr:dst_ip")
                .unwrap()
                .status,
            AdaptiveRuleStatus::Shadow
        );
    }

    #[test]
    fn active_rule_is_disabled_after_attributed_failure_threshold() {
        let mut state = AdaptiveControlState::with_active_rule(AdaptiveFieldRule::active(
            "rule:dstAddr",
            "source:tcp://127.0.0.1",
            "dstAddr",
            CanonicalField::DstIp,
            AdaptiveValueType::Ip,
        ));

        for _ in 0..10 {
            state.record_applied_rule_result("rule:dstAddr", ParseStatus::Failed);
        }
        state.evaluate_rollback(Utc.timestamp_opt(1_778_808_000, 0).unwrap());

        let rule = state.rule("rule:dstAddr").unwrap();
        assert_eq!(rule.status, AdaptiveRuleStatus::Disabled);
        assert_eq!(
            rule.disabled_reason.as_deref(),
            Some("rollback: attributed failure threshold exceeded")
        );
    }

    #[test]
    fn active_rule_records_failed_rule_on_value_conversion_conflict() {
        let raw = raw("dstAddr=not-an-ip src=10.0.0.1 proto=TCP");
        let mut event = CanonicalEvent::failed(raw.clone(), "partial");
        event.src_ip = Some("10.0.0.1".to_string());
        event.protocol = Some("TCP".to_string());
        event.classify_firewall_tuple();
        let rules = AdaptiveRuleSnapshot::from_rules(vec![AdaptiveFieldRule::active(
            "rule:bad-dst",
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

        assert_eq!(result.conflicts, 1);
        assert!(diagnostics.applied_rules.is_empty());
        assert_eq!(diagnostics.failed_rules[0].0, "rule:bad-dst");
        assert_eq!(event.dst_ip, None);
    }

    #[test]
    fn failed_rule_conflicts_disable_active_rule_after_threshold() {
        let mut state = AdaptiveControlState::with_active_rule(AdaptiveFieldRule::active(
            "rule:bad-dst",
            "source:tcp://127.0.0.1",
            "dstAddr",
            CanonicalField::DstIp,
            AdaptiveValueType::Ip,
        ));

        for _ in 0..4 {
            state.record_failed_rule_result("rule:bad-dst");
        }
        state.evaluate_rollback(Utc.timestamp_opt(1_778_808_000, 0).unwrap());

        let rule = state.rule("rule:bad-dst").unwrap();
        assert_eq!(rule.status, AdaptiveRuleStatus::Disabled);
        assert_eq!(
            rule.disabled_reason.as_deref(),
            Some("rollback: active rule value conversion conflict threshold exceeded")
        );
    }

    #[test]
    fn observe_parse_result_learns_shadow_rule_from_partial_raw_pairs() {
        let mut state = AdaptiveControlState::new(AdaptiveLearningConfig {
            suggested_rule_min_samples: 2,
            activation_wilson_lower_bound: 0.20,
            auto_activate: true,
            ..AdaptiveLearningConfig::test_defaults()
        });
        let mut diagnostics = ParseDiagnosticsBuffer::default();

        for _ in 0..4 {
            let raw = raw("src=10.0.0.1 dst=10.0.0.2 actName=allow");
            let mut event = CanonicalEvent::failed(raw, "partial");
            event.src_ip = Some("10.0.0.1".to_string());
            event.dst_ip = Some("10.0.0.2".to_string());
            event.classify_firewall_tuple();
            state.observe_parse_result(&event, &diagnostics);
            diagnostics.clear();
        }
        state.evaluate_rules(Utc.timestamp_opt(1_778_808_000, 0).unwrap());

        let snapshot = state.rule_snapshot();
        assert_eq!(snapshot.rules().len(), 1);
        assert_eq!(snapshot.rules()[0].raw_key, "actName");
        assert_eq!(snapshot.rules()[0].canonical_field, CanonicalField::Action);
    }

    #[test]
    fn candidate_from_pair_does_not_match_action_substrings() {
        let raw = raw("src=10.0.0.1 dst=10.0.0.2 activation=true transaction=abc");
        let mut event = CanonicalEvent::failed(raw, "partial");
        event.src_ip = Some("10.0.0.1".to_string());
        event.dst_ip = Some("10.0.0.2".to_string());
        event.classify_firewall_tuple();

        assert_eq!(candidate_from_pair(&event, "activation", "true"), None);
        assert_eq!(candidate_from_pair(&event, "transaction", "abc"), None);
    }

    #[test]
    fn candidate_from_pair_does_not_treat_bare_target_as_dst_ip() {
        let raw = raw("src=10.0.0.1 target=10.0.0.2");
        let mut event = CanonicalEvent::failed(raw, "partial");
        event.src_ip = Some("10.0.0.1".to_string());
        event.classify_firewall_tuple();

        assert_eq!(candidate_from_pair(&event, "target", "10.0.0.2"), None);
        assert_eq!(
            candidate_from_pair(&event, "target_ip", "10.0.0.2"),
            Some((CanonicalField::DstIp, AdaptiveValueType::Ip))
        );
    }

    #[test]
    fn candidate_from_pair_does_not_match_level_substrings() {
        let raw = raw("src=10.0.0.1 dst=10.0.0.2 deviceLevelName=gold");
        let mut event = CanonicalEvent::failed(raw, "partial");
        event.src_ip = Some("10.0.0.1".to_string());
        event.dst_ip = Some("10.0.0.2".to_string());
        event.classify_firewall_tuple();

        assert_eq!(candidate_from_pair(&event, "deviceLevelName", "gold"), None);
    }
}
