use std::collections::HashMap;
use std::sync::OnceLock;

use fwlog_domain::{make_event_id, CanonicalEvent, ParseStatus, RawLog};
use regex::Regex;

/// 通用键值对解析器，自动识别多种分隔符（=、: 等）和键名变体，
/// 作为第二层回退策略使用。
#[derive(Debug, Clone, Copy, Default)]
pub struct GenericKvParser;

/// 提取键值对的通用正则：支持 key=value、key: value、key="value"、key='value' 等形式
fn generic_kv_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(
            r#"(?:^|[\s,;|])([a-zA-Z0-9_\u4e00-\u9fa5]+)\s*[:=]\s*("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|[^\s,;|]+)"#,
        )
        .unwrap()
    })
}

/// IPv4 地址正则
fn ip_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(
            r#"\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b"#,
        )
        .unwrap()
    })
}

/// 将原始键名模糊映射到标准字段名
fn protocol_hint_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"(?i)(?:proto|protocol|鍗忚|协议)\s*[:=]\s*([^\s,;|]+)").unwrap()
    })
}

fn action_hint_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"(?i)(?:action|act|鍔ㄤ綔|动作|nat绫诲瀷|nat类型)\s*[:=]\s*([^\s,;|]+)")
            .unwrap()
    })
}

fn canonicalize_key(key: &str) -> Option<&'static str> {
    let lower = key.to_lowercase();
    match lower.as_str() {
        // src_ip
        "src" | "source" | "source_ip" | "saddr" | "srcaddr" | "src_ip" | "sip" | "srcip" => {
            Some("src_ip")
        }
        // dst_ip
        "dst" | "dest" | "destination" | "destination_ip" | "daddr" | "dstaddr" | "dst_ip"
        | "dip" | "dstip" => Some("dst_ip"),
        // src_port
        "sport" | "source_port" | "srcport" | "spt" | "src_port" => Some("src_port"),
        // dst_port
        "dport" | "destination_port" | "dstport" | "dpt" | "dst_port" => Some("dst_port"),
        // protocol
        "proto" | "protocol" | "prot" => Some("protocol"),
        // action
        "action" | "act" | "disposition" | "nat类型" => Some("action"),
        // severity
        "severity" | "level" | "priority" | "sev" => Some("severity"),
        // device_id
        "device" | "device_id" | "host" | "hostname" | "dev" => Some("device_id"),
        _ => {
            let k = lower.as_str();
            // 中文 src_ip
            if k.contains("源") && (k.contains("ip") || k.contains("地址")) {
                return Some("src_ip");
            }
            // 中文 dst_ip
            if (k.contains("目的") || k.contains("目标"))
                && (k.contains("ip") || k.contains("地址"))
            {
                return Some("dst_ip");
            }
            // 中文 src_port
            if k.contains("源") && k.contains("端口") {
                return Some("src_port");
            }
            // 中文 dst_port
            if (k.contains("目的") || k.contains("目标")) && k.contains("端口") {
                return Some("dst_port");
            }
            // 中文 protocol
            if k.contains("协议") {
                return Some("protocol");
            }
            // 中文 action
            if k.contains("动作") || (k.contains("nat") && k.contains("类型")) {
                return Some("action");
            }
            // 中文 severity
            if k.contains("级别") || k.contains("严重") || k.contains("等级") {
                return Some("severity");
            }
            None
        }
    }
}

/// 去除字符串首尾的引号
fn unquote(s: &str) -> &str {
    s.strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .or_else(|| s.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')))
        .unwrap_or(s)
}

/// 协议值归一化
fn normalize_protocol(value: &str) -> String {
    match value.to_lowercase().as_str() {
        "1" | "icmp" => "ICMP".to_string(),
        "6" | "tcp" => "TCP".to_string(),
        "17" | "udp" => "UDP".to_string(),
        _ => value.to_uppercase(),
    }
}

impl GenericKvParser {
    pub const PARSER_ID: &'static str = "parser:generic_kv_v1";
    pub const PARSER_NAME: &'static str = "GenericKv";

    /// 判断 raw 日志是否包含可识别的键值对结构
    pub fn can_parse(&self, raw: &RawLog) -> bool {
        generic_kv_regex().captures_iter(&raw.raw).next().is_some()
    }

    /// 尝试通用解析：提取所有键值对 → 模糊映射 → 补全缺失字段 → 构造 CanonicalEvent
    pub fn parse(&self, raw: RawLog) -> CanonicalEvent {
        let mut fields: HashMap<&'static str, String> = HashMap::new();

        for caps in generic_kv_regex().captures_iter(&raw.raw) {
            let key = caps.get(1).map(|m| m.as_str()).unwrap_or("");
            let val = caps.get(2).map(|m| m.as_str()).unwrap_or("").trim();
            let val = unquote(val);

            if let Some(ckey) = canonicalize_key(key) {
                // 同名字段只保留第一次出现的值
                fields.entry(ckey).or_insert_with(|| val.to_string());
            }
        }

        // 从显式键值中提取
        let mut src_ip = fields.get("src_ip").cloned();
        let mut dst_ip = fields.get("dst_ip").cloned();

        // 如果缺少 src_ip / dst_ip，尝试从原始文本中按出现顺序提取 IPv4
        if src_ip.is_none() || dst_ip.is_none() {
            let ips: Vec<String> = ip_regex()
                .find_iter(&raw.raw)
                .map(|m| m.as_str().to_string())
                .collect();
            if src_ip.is_none() && !ips.is_empty() {
                src_ip = Some(ips[0].clone());
            }
            if dst_ip.is_none() && ips.len() > 1 {
                dst_ip = Some(ips[1].clone());
            }
        }

        // 协议归一化
        let protocol = fields
            .get("protocol")
            .map(|v| normalize_protocol(v.as_str()))
            .or_else(|| {
                protocol_hint_regex()
                    .captures(&raw.raw)
                    .and_then(|caps| caps.get(1).map(|value| normalize_protocol(value.as_str())))
            });

        // 判定成功条件：提取到了 action，或者同时有 src_ip + dst_ip
        let action = fields.get("action").cloned().or_else(|| {
            action_hint_regex()
                .captures(&raw.raw)
                .and_then(|caps| caps.get(1).map(|value| value.as_str().to_string()))
        });
        if src_ip.is_none() && dst_ip.is_none() && protocol.is_none() && action.is_none() {
            return CanonicalEvent::failed(raw, "generic parser: insufficient fields extracted");
        }

        let mut event = CanonicalEvent {
            event_id: make_event_id(&raw.raw, raw.ingest_time, &raw.source_addr),
            ingest_time: raw.ingest_time,
            source_addr: raw.source_addr,
            device_id: fields.get("device_id").cloned(),
            event_time: None,
            vendor: None,
            product: None,
            src_ip,
            src_port: fields.get("src_port").and_then(|v| v.parse().ok()),
            dst_ip,
            dst_port: fields.get("dst_port").and_then(|v| v.parse().ok()),
            protocol,
            action,
            severity: fields.get("severity").cloned(),
            raw: raw.raw,
            parse_status: ParseStatus::Parsed,
            parse_error: None,
        };
        event.classify_firewall_tuple();
        event
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
    fn parses_plain_kv_line() {
        let line = "src=192.168.1.1 dst=10.0.0.1 sport=54321 dport=80 proto=TCP action=allow";
        let event = GenericKvParser.parse(raw(line));
        assert_eq!(event.parse_status, ParseStatus::Parsed);
        assert_eq!(event.src_ip.as_deref(), Some("192.168.1.1"));
        assert_eq!(event.dst_ip.as_deref(), Some("10.0.0.1"));
        assert_eq!(event.src_port, Some(54321));
        assert_eq!(event.dst_port, Some(80));
        assert_eq!(event.protocol.as_deref(), Some("TCP"));
        assert_eq!(event.action.as_deref(), Some("allow"));
    }

    #[test]
    fn parses_chinese_kv_line() {
        let line =
            "源IP:192.168.1.10, 目的IP:8.8.8.8, 源端口:12345, 目的端口:53, 协议:17, NAT类型:snat";
        let event = GenericKvParser.parse(raw(line));
        assert_eq!(event.parse_status, ParseStatus::Parsed);
        assert_eq!(event.src_ip.as_deref(), Some("192.168.1.10"));
        assert_eq!(event.dst_ip.as_deref(), Some("8.8.8.8"));
        assert_eq!(event.src_port, Some(12345));
        assert_eq!(event.dst_port, Some(53));
        assert_eq!(event.protocol.as_deref(), Some("UDP"));
        assert_eq!(event.action.as_deref(), Some("snat"));
    }

    #[test]
    fn falls_back_to_ip_extraction_when_keys_missing() {
        let line = "192.168.1.1 connected to 10.0.0.1 on port 443 action=block";
        let event = GenericKvParser.parse(raw(line));
        assert_eq!(event.parse_status, ParseStatus::Parsed);
        assert_eq!(event.src_ip.as_deref(), Some("192.168.1.1"));
        assert_eq!(event.dst_ip.as_deref(), Some("10.0.0.1"));
        assert_eq!(event.action.as_deref(), Some("block"));
    }

    #[test]
    fn fails_on_completely_unstructured_line() {
        let line = "this is just random text without any structure";
        let event = GenericKvParser.parse(raw(line));
        assert_eq!(event.parse_status, ParseStatus::Failed);
    }

    #[test]
    fn parses_quoted_values() {
        let line = r#"src="192.168.1.1" dst='10.0.0.1' action="deny" proto="udp""#;
        let event = GenericKvParser.parse(raw(line));
        assert_eq!(event.parse_status, ParseStatus::Parsed);
        assert_eq!(event.src_ip.as_deref(), Some("192.168.1.1"));
        assert_eq!(event.dst_ip.as_deref(), Some("10.0.0.1"));
    }

    #[test]
    fn normalizes_protocol_numbers() {
        let line = "src=1.1.1.1 dst=2.2.2.2 action=allow proto=6";
        let event = GenericKvParser.parse(raw(line));
        assert_eq!(event.protocol.as_deref(), Some("TCP"));
    }
}
