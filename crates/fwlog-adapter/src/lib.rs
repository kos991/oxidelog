mod adaptive;
mod control;
mod diagnostics;
mod generic;
mod learn;
mod route;
mod rule;
mod sangfor;
mod scope;

pub use adaptive::{extract_generic_pairs, ExtractDiagnostics, GenericPair, GenericPairs};
pub use control::{
    LocalParserMetricsBatch, MetricsFlushEvent, ParserProfileDelta, ParserScopeState,
};
pub use diagnostics::{
    DetectOutcome, DetectScore, ParseDiagnosticsBuffer, ParseResult, ParserAttemptDiagnostic,
    ParserId, RuleId,
};
pub use generic::GenericKvParser;
pub use learn::{
    apply_active_rules, AdaptiveControlState, AdaptiveFieldRule, AdaptiveLearningConfig,
    AdaptiveRuleSnapshot, AdaptiveRuleStatus, AdaptiveValueType, CanonicalField,
};
pub use route::{
    PinnedScopeParsers, RouteSnapshot, StaticRouteGroup, GENERIC_KV_PARSER_ID,
    RULE_BASED_PARSER_ID, SANGFOR_PARSER_ID,
};
pub use rule::{FieldMapping, ParseRule, RuleBasedParser};
pub use sangfor::{LogAdapter, SangforAdapter};
pub use scope::{normalize_source_scope, ScopeNormalizationMode, SourceScope};

use fwlog_domain::{CanonicalEvent, ParseStatus, RawLog};

pub struct ParserEngine {
    known_adapters: Vec<Box<dyn LogAdapter + Send + Sync>>,
    generic: GenericKvParser,
    rules: RuleBasedParser,
    route_snapshot: RouteSnapshot,
    adaptive_snapshot: AdaptiveRuleSnapshot,
}

impl ParserEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_adapter<A>(mut self, adapter: A) -> Self
    where
        A: LogAdapter + Send + Sync + 'static,
    {
        self.route_snapshot
            .register_default_adapter(adapter.parser_id());
        self.known_adapters.push(Box::new(adapter));
        self
    }

    pub fn with_rules(mut self, rules: Vec<ParseRule>) -> Result<Self, String> {
        self.rules = RuleBasedParser::from_rules(rules)?;
        Ok(self)
    }

    pub fn with_rules_toml(mut self, toml_str: &str) -> Result<Self, String> {
        self.rules = RuleBasedParser::from_toml(toml_str)?;
        Ok(self)
    }

    pub fn with_route_snapshot(mut self, route_snapshot: RouteSnapshot) -> Self {
        self.route_snapshot = route_snapshot;
        for adapter in &self.known_adapters {
            self.route_snapshot
                .register_default_adapter(adapter.parser_id());
        }
        self
    }

    pub fn with_adaptive_snapshot(mut self, adaptive_snapshot: AdaptiveRuleSnapshot) -> Self {
        self.adaptive_snapshot = adaptive_snapshot;
        self
    }

    pub fn set_adaptive_snapshot(&mut self, adaptive_snapshot: AdaptiveRuleSnapshot) {
        self.adaptive_snapshot = adaptive_snapshot;
    }

    pub fn parse(&self, raw: RawLog) -> CanonicalEvent {
        let mut diagnostics = ParseDiagnosticsBuffer::default();
        self.parse_inner(raw, &mut diagnostics).event
    }

    pub fn parse_inner(
        &self,
        raw: RawLog,
        diagnostics: &mut ParseDiagnosticsBuffer,
    ) -> ParseResult {
        diagnostics.clear();
        let mut tried: Vec<String> = Vec::new();
        let mut best_partial: Option<(CanonicalEvent, &'static str)> = None;
        let mut matched_any_adapter = false;
        let raw_line = raw.raw.clone();

        let scope = normalize_source_scope(&raw.source_addr, ScopeNormalizationMode::SourceIp);
        let mut route_ids = Vec::new();
        self.route_snapshot
            .for_each_parser_id(&scope.scope_key, |parser_id| {
                route_ids.push(parser_id.to_string())
            });

        for parser_id in route_ids {
            if parser_id == GenericKvParser::PARSER_ID {
                if self.generic.can_parse(&raw) {
                    let event = self.generic.parse(raw.clone());
                    diagnostics.push_attempt(
                        GenericKvParser::PARSER_ID,
                        GenericKvParser::PARSER_NAME,
                        "generic key/value detector matched",
                        &event,
                    );
                    if event.parse_status == ParseStatus::Parsed {
                        diagnostics.matched_parser_id =
                            Some(GenericKvParser::PARSER_ID.to_string());
                        return ParseResult { event };
                    }
                    if event.parse_status == ParseStatus::Partial && best_partial.is_none() {
                        best_partial = Some((event.clone(), GenericKvParser::PARSER_ID));
                    }
                    tried.push(format!(
                        "{}: {}",
                        GenericKvParser::PARSER_NAME,
                        event.parse_error.as_deref().unwrap_or("failed")
                    ));
                } else {
                    let event = self.generic.parse(raw.clone());
                    diagnostics.push_attempt(
                        GenericKvParser::PARSER_ID,
                        GenericKvParser::PARSER_NAME,
                        "generic key/value detector did not match",
                        &event,
                    );
                }
                continue;
            }

            if parser_id == RuleBasedParser::PARSER_ID {
                if self.rules.can_parse(&raw) {
                    let event = self.rules.parse(raw.clone());
                    diagnostics.push_attempt(
                        RuleBasedParser::PARSER_ID,
                        RuleBasedParser::PARSER_NAME,
                        "rule parser configured",
                        &event,
                    );
                    if event.parse_status == ParseStatus::Parsed {
                        diagnostics.matched_parser_id =
                            Some(RuleBasedParser::PARSER_ID.to_string());
                        return ParseResult { event };
                    }
                    if event.parse_status == ParseStatus::Partial && best_partial.is_none() {
                        best_partial = Some((event.clone(), RuleBasedParser::PARSER_ID));
                    }
                    tried.push(format!(
                        "{}: {}",
                        RuleBasedParser::PARSER_NAME,
                        event.parse_error.as_deref().unwrap_or("failed")
                    ));
                }
                continue;
            }

            for adapter in &self.known_adapters {
                if adapter.parser_id() != parser_id {
                    continue;
                }
                let detect = adapter.detect(&raw);
                if !detect.is_match() {
                    continue;
                }
                matched_any_adapter = true;
                let event = adapter.parse(raw.clone());
                diagnostics.push_attempt(
                    adapter.parser_id(),
                    adapter.name(),
                    detect.reason,
                    &event,
                );
                if event.parse_status == ParseStatus::Parsed {
                    diagnostics.matched_parser_id = Some(adapter.parser_id().to_string());
                    return ParseResult { event };
                }
                if event.parse_status == ParseStatus::Partial && best_partial.is_none() {
                    best_partial = Some((event.clone(), adapter.parser_id()));
                }
                tried.push(format!(
                    "{}: {}",
                    adapter.name(),
                    event.parse_error.as_deref().unwrap_or("failed to parse")
                ));
            }
        }

        if !matched_any_adapter {
            for adapter in &self.known_adapters {
                let event = adapter.parse(raw.clone());
                diagnostics.push_attempt(
                    adapter.parser_id(),
                    adapter.name(),
                    "compatibility fallback after generic and rule parsers",
                    &event,
                );
                if event.parse_status == ParseStatus::Parsed {
                    diagnostics.matched_parser_id = Some(adapter.parser_id().to_string());
                    return ParseResult { event };
                }
                if event.parse_status == ParseStatus::Partial && best_partial.is_none() {
                    best_partial = Some((event.clone(), adapter.parser_id()));
                }
                tried.push(format!(
                    "{}: {}",
                    adapter.name(),
                    event.parse_error.as_deref().unwrap_or("failed to parse")
                ));
            }
        }

        if let Some((mut event, parser_id)) = best_partial {
            apply_active_rules(
                &self.adaptive_snapshot,
                &scope.scope_key,
                &raw_line,
                &mut event,
                diagnostics,
            );
            diagnostics.matched_parser_id = Some(parser_id.to_string());
            diagnostics.failure_reason = event.parse_error.clone();
            return ParseResult { event };
        }

        let failure_reason = format!("no parser matched; attempted [{}]", tried.join(", "));
        diagnostics.failure_reason = Some(failure_reason.clone());
        let mut event = CanonicalEvent::failed(raw, failure_reason);
        apply_active_rules(
            &self.adaptive_snapshot,
            &scope.scope_key,
            &raw_line,
            &mut event,
            diagnostics,
        );
        ParseResult { event }
    }

    pub fn adapter_count(&self) -> usize {
        self.known_adapters.len()
    }

    pub fn rule_count(&self) -> usize {
        self.rules.rule_count()
    }
}

impl Default for ParserEngine {
    fn default() -> Self {
        Self {
            known_adapters: vec![Box::new(SangforAdapter)],
            generic: GenericKvParser,
            rules: RuleBasedParser::empty(),
            route_snapshot: RouteSnapshot::default_static(),
            adaptive_snapshot: AdaptiveRuleSnapshot::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn raw(line: &str) -> RawLog {
        RawLog {
            ingest_time: Utc.timestamp_opt(1_778_808_000, 0).unwrap(),
            source_addr: "tcp://127.0.0.1:1514".to_string(),
            raw: line.to_string(),
        }
    }

    #[test]
    fn layer1_sangfor_adapter_parses_known_format() {
        let engine = ParserEngine::new();
        let event = engine.parse(raw(
            "Sangfor: src=192.168.1.10 dst=8.8.8.8 sport=51514 dport=53 proto=UDP action=allow severity=info",
        ));
        assert_eq!(event.parse_status, ParseStatus::Parsed);
        assert_eq!(event.vendor.as_deref(), Some("Sangfor"));
        assert_eq!(event.src_ip.as_deref(), Some("192.168.1.10"));
    }

    #[test]
    fn layer2_generic_kv_parses_unknown_format() {
        let engine = ParserEngine::new();
        let event = engine.parse(raw(
            "src=10.0.0.1 dst=10.0.0.2 sport=12345 dport=443 proto=TCP action=deny",
        ));
        assert_eq!(event.parse_status, ParseStatus::Parsed);
        assert_eq!(event.src_ip.as_deref(), Some("10.0.0.1"));
        assert_eq!(event.action.as_deref(), Some("deny"));
        assert_eq!(event.vendor, None);
    }

    #[test]
    fn layer3_rule_based_parses_configured_format() {
        let toml = r#"
[[rules]]
name = "CustomArrow"
priority = 1
match_pattern = '^(?P<ts>\S+) (?P<dev>\S+) .* (?P<src>\d+\.\d+\.\d+\.\d+) -> (?P<dst>\d+\.\d+\.\d+\.\d+) (?P<act>\S+)'
field_mappings = [
    { target = "src_ip", source = "src" },
    { target = "dst_ip", source = "dst" },
    { target = "action", source = "act" },
    { target = "device_id", source = "dev" },
]
"#;
        let engine = ParserEngine::new().with_rules_toml(toml).unwrap();

        let event = engine.parse(raw(
            "2024-01-15T10:00:00Z my-device [firewall] 172.16.0.1 -> 192.168.0.1 DROP",
        ));
        assert_eq!(event.parse_status, ParseStatus::Parsed);
        assert_eq!(event.src_ip.as_deref(), Some("172.16.0.1"));
        assert_eq!(event.dst_ip.as_deref(), Some("192.168.0.1"));
        assert_eq!(event.vendor.as_deref(), Some("CustomArrow"));
        assert_eq!(event.device_id.as_deref(), Some("my-device"));
        assert_eq!(event.action.as_deref(), Some("DROP"));
    }

    #[test]
    fn all_layers_fail_on_gibberish() {
        let engine = ParserEngine::new();
        let event = engine.parse(raw("totally unstructured garbage line"));
        assert_eq!(event.parse_status, ParseStatus::Failed);
        assert!(event
            .parse_error
            .as_deref()
            .unwrap()
            .contains("no parser matched"));
    }

    #[test]
    fn parse_inner_records_successful_parser() {
        let engine = ParserEngine::new();
        let mut diagnostics = ParseDiagnosticsBuffer::default();
        let result = engine.parse_inner(
            raw("Sangfor: src=192.168.1.10 dst=8.8.8.8 proto=UDP action=allow"),
            &mut diagnostics,
        );

        assert_eq!(result.event.parse_status, ParseStatus::Parsed);
        assert_eq!(
            diagnostics.matched_parser_id.as_deref(),
            Some("parser:sangfor_nat_v1")
        );
        assert!(diagnostics
            .attempts
            .iter()
            .any(|attempt| attempt.parser_id == "parser:sangfor_nat_v1"));
    }

    #[test]
    fn parse_inner_records_all_failed_attempts() {
        let engine = ParserEngine::new();
        let mut diagnostics = ParseDiagnosticsBuffer::default();
        let result = engine.parse_inner(raw("totally unstructured garbage line"), &mut diagnostics);

        assert_eq!(result.event.parse_status, ParseStatus::Failed);
        assert!(diagnostics
            .attempts
            .iter()
            .any(|attempt| attempt.parser_id == "parser:sangfor_nat_v1"));
        assert!(diagnostics
            .attempts
            .iter()
            .any(|attempt| attempt.parser_id == "parser:generic_kv_v1"));
        assert!(diagnostics
            .failure_reason
            .as_deref()
            .unwrap()
            .contains("no parser matched"));
    }

    #[test]
    fn parse_wrapper_matches_parse_inner_event() {
        let engine = ParserEngine::new();
        let line = "src=10.0.0.1 dst=10.0.0.2 sport=12345 dport=443 proto=TCP action=deny";
        let wrapped = engine.parse(raw(line));
        let mut diagnostics = ParseDiagnosticsBuffer::default();
        let inner = engine.parse_inner(raw(line), &mut diagnostics).event;

        assert_eq!(wrapped, inner);
        assert_eq!(
            diagnostics.matched_parser_id.as_deref(),
            Some("parser:generic_kv_v1")
        );
    }

    #[test]
    fn parse_returns_partial_for_incomplete_searchable_tuple() {
        let engine = ParserEngine::new();
        let event = engine.parse(raw("src=10.0.0.1 proto=TCP"));

        assert_eq!(event.parse_status, ParseStatus::Partial);
        assert_eq!(event.src_ip.as_deref(), Some("10.0.0.1"));
        assert_eq!(event.protocol.as_deref(), Some("TCP"));
        assert!(event
            .parse_error
            .as_deref()
            .unwrap()
            .contains("minimum searchable tuple"));
    }

    #[test]
    fn parse_inner_records_partial_parser_attribution() {
        let engine = ParserEngine::new();
        let mut diagnostics = ParseDiagnosticsBuffer::default();
        let result = engine.parse_inner(raw("src=10.0.0.1 proto=TCP"), &mut diagnostics);

        assert_eq!(result.event.parse_status, ParseStatus::Partial);
        assert_eq!(
            diagnostics.matched_parser_id.as_deref(),
            Some("parser:generic_kv_v1")
        );
    }

    #[test]
    fn pinned_route_can_prioritize_rule_parser_before_generic() {
        let toml = r#"
[[rules]]
name = "PinnedRule"
priority = 1
match_pattern = '^custom src=(?P<src>\d+\.\d+\.\d+\.\d+) dst=(?P<dst>\d+\.\d+\.\d+\.\d+) action=(?P<act>\S+)'
field_mappings = [
    { target = "src_ip", source = "src" },
    { target = "dst_ip", source = "dst" },
    { target = "action", source = "act" },
]
"#;
        let route = RouteSnapshot::with_pins(vec![PinnedScopeParsers {
            scope_key: "source:tcp://127.0.0.1".to_string(),
            parser_ids: vec![RuleBasedParser::PARSER_ID.to_string()],
        }]);
        let engine = ParserEngine::new()
            .with_rules_toml(toml)
            .unwrap()
            .with_route_snapshot(route);
        let mut diagnostics = ParseDiagnosticsBuffer::default();
        let result = engine.parse_inner(
            raw("custom src=10.0.0.1 dst=10.0.0.2 action=deny"),
            &mut diagnostics,
        );

        assert_eq!(result.event.parse_status, ParseStatus::Parsed);
        assert_eq!(result.event.vendor.as_deref(), Some("PinnedRule"));
        assert_eq!(
            diagnostics.matched_parser_id.as_deref(),
            Some(RuleBasedParser::PARSER_ID)
        );
    }

    #[derive(Debug)]
    struct CustomAdapter;

    impl LogAdapter for CustomAdapter {
        fn name(&self) -> &'static str {
            "CustomAdapter"
        }

        fn parser_id(&self) -> &'static str {
            "parser:custom_v1"
        }

        fn can_parse(&self, raw: &RawLog) -> bool {
            raw.raw.starts_with("custom ")
        }

        fn parse(&self, raw: RawLog) -> CanonicalEvent {
            let mut event = CanonicalEvent::failed(raw, "custom failed");
            event.vendor = Some("Custom".to_string());
            event.src_ip = Some("10.0.0.1".to_string());
            event.dst_ip = Some("10.0.0.2".to_string());
            event.action = Some("allow".to_string());
            event.classify_firewall_tuple();
            event
        }
    }

    #[test]
    fn custom_adapters_are_tried_before_generic_without_explicit_route_pin() {
        let engine = ParserEngine::new().with_adapter(CustomAdapter);
        let mut diagnostics = ParseDiagnosticsBuffer::default();
        let result = engine.parse_inner(
            raw("custom src=192.168.1.1 dst=192.168.1.2 action=deny"),
            &mut diagnostics,
        );

        assert_eq!(result.event.vendor.as_deref(), Some("Custom"));
        assert_eq!(
            diagnostics.matched_parser_id.as_deref(),
            Some("parser:custom_v1")
        );
    }

    #[test]
    fn custom_adapters_survive_route_snapshot_replacement() {
        let route = RouteSnapshot::with_pins(vec![PinnedScopeParsers {
            scope_key: "source:tcp://127.0.0.1".to_string(),
            parser_ids: vec![RuleBasedParser::PARSER_ID.to_string()],
        }]);
        let engine = ParserEngine::new()
            .with_adapter(CustomAdapter)
            .with_route_snapshot(route);
        let mut diagnostics = ParseDiagnosticsBuffer::default();
        let result = engine.parse_inner(
            raw("custom src=192.168.1.1 dst=192.168.1.2 action=deny"),
            &mut diagnostics,
        );

        assert_eq!(result.event.vendor.as_deref(), Some("Custom"));
        assert_eq!(
            diagnostics.matched_parser_id.as_deref(),
            Some("parser:custom_v1")
        );
    }

    #[test]
    fn parser_applies_active_adaptive_rule_after_partial_static_parse() {
        let snapshot = AdaptiveRuleSnapshot::from_rules(vec![AdaptiveFieldRule::active(
            "rule:actName",
            "source:tcp://127.0.0.1",
            "actName",
            CanonicalField::Action,
            AdaptiveValueType::Action,
        )]);
        let engine = ParserEngine::new().with_adaptive_snapshot(snapshot);
        let mut diagnostics = ParseDiagnosticsBuffer::default();

        let result = engine.parse_inner(
            raw("src=10.0.0.1 dst=10.0.0.2 actName=allow"),
            &mut diagnostics,
        );

        assert_eq!(result.event.parse_status, ParseStatus::Parsed);
        assert_eq!(result.event.action.as_deref(), Some("allow"));
        assert_eq!(diagnostics.applied_rules.len(), 1);
    }

    #[test]
    fn parser_records_failed_adaptive_rule_after_partial_static_parse() {
        let snapshot = AdaptiveRuleSnapshot::from_rules(vec![AdaptiveFieldRule::active(
            "rule:bad-dst",
            "source:tcp://127.0.0.1",
            "badDst",
            CanonicalField::DstIp,
            AdaptiveValueType::Ip,
        )]);
        let engine = ParserEngine::new().with_adaptive_snapshot(snapshot);
        let mut diagnostics = ParseDiagnosticsBuffer::default();

        let result = engine.parse_inner(
            raw("src=10.0.0.1 badDst=not-an-ip proto=TCP"),
            &mut diagnostics,
        );

        assert_ne!(result.event.dst_ip.as_deref(), Some("not-an-ip"));
        assert!(diagnostics.applied_rules.is_empty());
        assert_eq!(diagnostics.failed_rules[0].0, "rule:bad-dst");
    }

    #[test]
    fn engine_default_has_one_adapter() {
        let engine = ParserEngine::new();
        assert_eq!(engine.adapter_count(), 1);
    }

    #[test]
    fn with_adapter_increases_count() {
        let engine = ParserEngine::new().with_adapter(SangforAdapter);
        assert_eq!(engine.adapter_count(), 2);
    }

    #[test]
    fn chinese_kv_falls_through_to_layer2() {
        let engine = ParserEngine::new();
        let event = engine.parse(raw(
            "婧怚P:192.168.1.1, 鐩殑IP:10.0.0.1, 鍗忚:6, 鍔ㄤ綔:鍏佽",
        ));
        assert_eq!(event.parse_status, ParseStatus::Parsed);
        assert_eq!(event.protocol.as_deref(), Some("TCP"));
        assert_eq!(event.action.as_deref(), Some("鍏佽"));
    }
}
