use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
struct RuleFile {
    rules: Vec<ParseRule>,
}
use std::sync::OnceLock;

use fwlog_domain::{make_event_id, CanonicalEvent, ParseStatus, RawLog};
use regex::Regex;

/// 单条解析规则，可从 TOML/JSON 配置文件加载。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParseRule {
    pub name: String,
    /// 优先级，数字越小越先尝试
    pub priority: i32,
    /// 匹配整行日志的正则表达式
    pub match_pattern: String,
    /// 字段提取映射表
    pub field_mappings: Vec<FieldMapping>,
}

/// 字段映射：将正则捕获组映射到 CanonicalEvent 的字段
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMapping {
    /// 目标字段名（如 src_ip、dst_port、action 等）
    pub target: String,
    /// 正则捕获组名称或索引（如 "src" 或 "1"）
    pub source: String,
    /// 可选转换：normalize_protocol、uppercase、lowercase
    pub transform: Option<String>,
}

/// 基于外部配置规则的解析器，作为第三层兜底策略。
#[derive(Debug)]
pub struct RuleBasedParser {
    rules: Vec<(ParseRule, Regex)>,
}

/// 编译并缓存正则，避免每次重复编译
fn compile_regex(pattern: &str) -> Result<Regex, regex::Error> {
    static CACHE: OnceLock<std::sync::Mutex<std::collections::HashMap<String, Regex>>> =
        OnceLock::new();
    let cache = CACHE.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    let mut guard = cache.lock().unwrap();
    if let Some(re) = guard.get(pattern) {
        return Ok(re.clone());
    }
    let re = Regex::new(pattern)?;
    guard.insert(pattern.to_string(), re.clone());
    Ok(re)
}

impl RuleBasedParser {
    pub const PARSER_ID: &'static str = "parser:rule_based_v1";
    pub const PARSER_NAME: &'static str = "RuleBased";

    /// 返回已加载的规则数量
    pub fn rule_count(&self) -> usize {
        self.rules.len()
    }

    /// 创建一个空的规则解析器
    pub fn empty() -> Self {
        Self { rules: Vec::new() }
    }

    /// 从 TOML 字符串加载规则
    pub fn from_toml(toml_str: &str) -> Result<Self, String> {
        let file: RuleFile = toml::from_str(toml_str).map_err(|e| e.to_string())?;
        Self::from_rules(file.rules)
    }

    /// 从规则列表构建解析器
    pub fn from_rules(rules: Vec<ParseRule>) -> Result<Self, String> {
        let mut seen = std::collections::BTreeSet::new();
        for rule in &rules {
            if !seen.insert(rule.name.clone()) {
                return Err(format!("duplicate rule name '{}' in ruleset", rule.name));
            }
        }

        let mut compiled = Vec::new();
        for rule in rules {
            let regex = compile_regex(&rule.match_pattern)
                .map_err(|e| format!("invalid regex in rule '{}': {}", rule.name, e))?;
            compiled.push((rule, regex));
        }
        // 按优先级升序排列（数字小的优先）
        compiled.sort_by_key(|(r, _)| r.priority);
        Ok(Self { rules: compiled })
    }

    /// 是否有可用规则
    pub fn can_parse(&self, _raw: &RawLog) -> bool {
        !self.rules.is_empty()
    }

    /// 尝试用所有规则依次匹配，第一个成功匹配并提取到有效字段的规则胜出
    pub fn parse(&self, raw: RawLog) -> CanonicalEvent {
        for (rule, regex) in &self.rules {
            if let Some(caps) = regex.captures(&raw.raw) {
                let mut event = CanonicalEvent {
                    event_id: make_event_id(&raw.raw, raw.ingest_time, &raw.source_addr),
                    ingest_time: raw.ingest_time,
                    source_addr: raw.source_addr.clone(),
                    device_id: None,
                    event_time: None,
                    vendor: Some(rule.name.clone()),
                    product: None,
                    src_ip: None,
                    src_port: None,
                    dst_ip: None,
                    dst_port: None,
                    protocol: None,
                    action: None,
                    severity: None,
                    raw: raw.raw.clone(),
                    parse_status: ParseStatus::Parsed,
                    parse_error: None,
                };

                for mapping in &rule.field_mappings {
                    let value = caps
                        .name(&mapping.source)
                        .map(|m| m.as_str().to_string())
                        .or_else(|| {
                            mapping
                                .source
                                .parse::<usize>()
                                .ok()
                                .and_then(|idx| caps.get(idx).map(|m| m.as_str().to_string()))
                        });

                    if let Some(val) = value {
                        let transformed = apply_transform(&val, mapping.transform.as_deref());
                        match mapping.target.as_str() {
                            "src_ip" => event.src_ip = Some(transformed),
                            "dst_ip" => event.dst_ip = Some(transformed),
                            "src_port" => event.src_port = transformed.parse().ok(),
                            "dst_port" => event.dst_port = transformed.parse().ok(),
                            "protocol" => event.protocol = Some(transformed),
                            "action" => event.action = Some(transformed),
                            "severity" => event.severity = Some(transformed),
                            "device_id" => event.device_id = Some(transformed),
                            "vendor" => event.vendor = Some(transformed),
                            "product" => event.product = Some(transformed),
                            _ => {}
                        }
                    }
                }

                // 只要有 action 或 src_ip，就视为成功
                let has_any_field = event.src_ip.is_some()
                    || event.dst_ip.is_some()
                    || event.protocol.is_some()
                    || event.action.is_some();
                if has_any_field {
                    event.classify_firewall_tuple();
                    return event;
                }
            }
        }

        CanonicalEvent::failed(raw, "rule-based parser: no rule matched")
    }
}

fn apply_transform(value: &str, transform: Option<&str>) -> String {
    match transform {
        Some("normalize_protocol") => match value.to_lowercase().as_str() {
            "1" | "icmp" => "ICMP".to_string(),
            "6" | "tcp" => "TCP".to_string(),
            "17" | "udp" => "UDP".to_string(),
            _ => value.to_uppercase(),
        },
        Some("uppercase") => value.to_uppercase(),
        Some("lowercase") => value.to_lowercase(),
        _ => value.to_string(),
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
    fn parses_via_toml_rule() {
        let toml = r#"
[[rules]]
name = "Cisco ASA"
priority = 10
match_pattern = '^(?P<timestamp>\w+ \d+ \d+:\d+:\d+) (?P<device>\S+) .*src=(?P<src>\S+) dst=(?P<dst>\S+) .*action=(?P<action>\S+)'
field_mappings = [
    { target = "src_ip", source = "src" },
    { target = "dst_ip", source = "dst" },
    { target = "action", source = "action" },
    { target = "device_id", source = "device" },
]
"#;

        let parser = RuleBasedParser::from_toml(toml).unwrap();
        let line =
            "May 15 10:00:01 asa-fw-01 %ASA-6-302013: src=192.168.1.1 dst=10.0.0.1 action=permit";
        let event = parser.parse(raw(line));

        assert_eq!(event.parse_status, ParseStatus::Parsed);
        assert_eq!(event.src_ip.as_deref(), Some("192.168.1.1"));
        assert_eq!(event.dst_ip.as_deref(), Some("10.0.0.1"));
        assert_eq!(event.action.as_deref(), Some("permit"));
        assert_eq!(event.device_id.as_deref(), Some("asa-fw-01"));
        assert_eq!(event.vendor.as_deref(), Some("Cisco ASA"));
    }

    #[test]
    fn parses_with_transform() {
        let rule = ParseRule {
            name: "TestRule".to_string(),
            priority: 1,
            match_pattern: r"src=(?P<src>\S+) proto=(?P<proto>\S+) action=(?P<action>\S+)"
                .to_string(),
            field_mappings: vec![
                FieldMapping {
                    target: "src_ip".to_string(),
                    source: "src".to_string(),
                    transform: None,
                },
                FieldMapping {
                    target: "protocol".to_string(),
                    source: "proto".to_string(),
                    transform: Some("normalize_protocol".to_string()),
                },
                FieldMapping {
                    target: "action".to_string(),
                    source: "action".to_string(),
                    transform: Some("uppercase".to_string()),
                },
            ],
        };

        let parser = RuleBasedParser::from_rules(vec![rule]).unwrap();
        let event = parser.parse(raw("src=1.2.3.4 proto=6 action=allow"));

        assert_eq!(event.protocol.as_deref(), Some("TCP"));
        assert_eq!(event.action.as_deref(), Some("ALLOW"));
    }

    #[test]
    fn empty_parser_fails_gracefully() {
        let parser = RuleBasedParser::empty();
        let event = parser.parse(raw("anything"));
        assert_eq!(event.parse_status, ParseStatus::Failed);
    }

    #[test]
    fn priority_order_is_respected() {
        let rules = vec![
            ParseRule {
                name: "HighPriority".to_string(),
                priority: 1,
                match_pattern: r".*action=(?P<action>\S+).*".to_string(),
                field_mappings: vec![FieldMapping {
                    target: "action".to_string(),
                    source: "action".to_string(),
                    transform: None,
                }],
            },
            ParseRule {
                name: "LowPriority".to_string(),
                priority: 99,
                match_pattern: r".*action=(?P<action>\S+).*".to_string(),
                field_mappings: vec![FieldMapping {
                    target: "action".to_string(),
                    source: "action".to_string(),
                    transform: None,
                }],
            },
        ];

        let parser = RuleBasedParser::from_rules(rules).unwrap();
        let event = parser.parse(raw("action=block"));
        assert_eq!(event.vendor.as_deref(), Some("HighPriority"));
    }

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
}
