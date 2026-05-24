use std::sync::OnceLock;

use chrono::{Datelike, NaiveDateTime, TimeZone, Utc};
use fwlog_domain::{make_event_id, CanonicalEvent, ParseStatus, RawLog};
use regex::Regex;

pub trait LogAdapter: Send + Sync {
    fn name(&self) -> &'static str;

    fn parser_id(&self) -> &'static str {
        "parser:legacy_adapter"
    }

    fn can_parse(&self, raw: &RawLog) -> bool {
        let raw_lower = raw.raw.to_lowercase();
        raw_lower.contains("sangfor")
            || raw_lower.contains("日志类型")
            || raw_lower.contains("nat类型")
    }

    fn detect(&self, raw: &RawLog) -> crate::DetectOutcome {
        if self.can_parse(raw) {
            crate::DetectOutcome::matched("compatibility can_parse matched")
        } else {
            crate::DetectOutcome::no_match("compatibility can_parse did not match")
        }
    }

    fn parse(&self, raw: RawLog) -> CanonicalEvent;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SangforAdapter;

impl LogAdapter for SangforAdapter {
    fn name(&self) -> &'static str {
        "SangforAdapter"
    }

    fn parser_id(&self) -> &'static str {
        "parser:sangfor_nat_v1"
    }

    fn can_parse(&self, raw: &RawLog) -> bool {
        let raw_lower = raw.raw.to_lowercase();
        raw_lower.contains("sangfor")
            || raw_lower.contains("日志类型")
            || raw_lower.contains("nat类型")
    }

    fn parse(&self, raw: RawLog) -> CanonicalEvent {
        let mut src = None;
        let mut dst = None;
        let mut sport = None;
        let mut dport = None;
        let mut proto = None;
        let mut action = None;
        let mut severity = None;

        for caps in kv_regex().captures_iter(&raw.raw) {
            let key = caps.get(1).map(|m| m.as_str()).unwrap_or("");
            let val = caps.get(2).map(|m| m.as_str().trim()).unwrap_or("");

            match canonical_key(key) {
                Some("src") => {
                    if src.is_none() {
                        src = Some(val.to_string());
                    }
                }
                Some("dst") => {
                    if dst.is_none() {
                        dst = Some(val.to_string());
                    }
                }
                Some("sport") => {
                    if sport.is_none() {
                        sport = val.parse().ok();
                    }
                }
                Some("dport") => {
                    if dport.is_none() {
                        dport = val.parse().ok();
                    }
                }
                Some("proto") => {
                    if proto.is_none() {
                        proto = Some(normalize_protocol(val.to_string()));
                    }
                }
                Some("action") => {
                    if action.is_none() {
                        action = Some(val.to_string());
                    }
                }
                Some("severity") => {
                    if severity.is_none() {
                        severity = Some(val.to_string());
                    }
                }
                _ => {}
            }
        }

        if src.is_none() && dst.is_none() && proto.is_none() && action.is_none() {
            return CanonicalEvent::failed(raw, "missing required fields: src, dst, action");
        }

        let event_time = parse_syslog_timestamp(&raw.raw, raw.ingest_time);

        let mut event = CanonicalEvent {
            event_id: make_event_id(&raw.raw, raw.ingest_time, &raw.source_addr),
            ingest_time: raw.ingest_time,
            source_addr: raw.source_addr,
            device_id: None,
            event_time,
            vendor: Some("Sangfor".to_string()),
            product: Some("Firewall".to_string()),
            src_ip: src,
            src_port: sport,
            dst_ip: dst,
            dst_port: dport,
            protocol: proto,
            action,
            severity,
            raw: raw.raw,
            parse_status: ParseStatus::Parsed,
            parse_error: None,
        };
        event.classify_firewall_tuple();
        event
    }
}

fn kv_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(
            r"(?:\s|^|,)(src|dst|sport|dport|proto|action|severity|源IP|目的IP|源端口|目的端口|协议|NAT类型)(?:=|:)\s*([^\s,]+)",
        )
        .unwrap()
    })
}

fn canonical_key(key: &str) -> Option<&'static str> {
    match key {
        "src" | "源IP" => Some("src"),
        "dst" | "目的IP" => Some("dst"),
        "sport" | "源端口" => Some("sport"),
        "dport" | "目的端口" => Some("dport"),
        "proto" | "协议" => Some("proto"),
        "action" | "NAT类型" => Some("action"),
        "severity" => Some("severity"),
        _ if key.contains("IP") && key.contains("源") => Some("src"),
        _ if key.contains("IP") && key.contains("目的") => Some("dst"),
        _ if key.contains("端口") && key.contains("源") => Some("sport"),
        _ if key.contains("端口") && key.contains("目的") => Some("dport"),
        _ if key.contains("协议") => Some("proto"),
        _ if key.contains("NAT") && key.contains("类型") => Some("action"),
        _ => None,
    }
}

fn normalize_protocol(value: String) -> String {
    match value.as_str() {
        "1" => "ICMP".to_string(),
        "6" => "TCP".to_string(),
        "17" => "UDP".to_string(),
        _ => value.to_uppercase(),
    }
}

fn parse_syslog_timestamp(
    raw: &str,
    ingest_time: chrono::DateTime<Utc>,
) -> Option<chrono::DateTime<Utc>> {
    static SYSLOG_TS: OnceLock<Regex> = OnceLock::new();
    let re = SYSLOG_TS
        .get_or_init(|| Regex::new(r"^(?:<\d+>)?(\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})").unwrap());

    let caps = re.captures(raw)?;
    let ts_str = caps.get(1)?.as_str();

    let year = ingest_time.year();
    let with_year = format!("{} {}", year, ts_str);

    if let Ok(naive) = NaiveDateTime::parse_from_str(&with_year, "%Y %b %d %H:%M:%S") {
        return Utc.from_local_datetime(&naive).single();
    }

    None
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
    fn parses_syslog_prefixed_sangfor_line() {
        let event = SangforAdapter.parse(raw("<134>May 15 10:00:01 fw Sangfor: src=192.168.1.10 dst=8.8.8.8 sport=51514 dport=53 proto=UDP action=allow severity=info"));

        assert_eq!(event.parse_status, ParseStatus::Parsed);
        assert_eq!(event.src_ip.as_deref(), Some("192.168.1.10"));
        assert_eq!(event.dst_ip.as_deref(), Some("8.8.8.8"));
        assert_eq!(event.dst_port, Some(53));
        assert_eq!(event.protocol.as_deref(), Some("UDP"));
        assert_eq!(event.action.as_deref(), Some("allow"));
    }

    #[test]
    fn parses_four_supported_sample_shapes_and_fails_invalid_line() {
        let lines = [
            "<134>May 15 10:00:01 fw Sangfor: src=192.168.1.10 dst=8.8.8.8 sport=51514 dport=53 proto=UDP action=allow severity=info",
            "<134>May 15 10:00:02 fw Sangfor: src=192.168.1.20 dst=1.1.1.1 sport=44321 dport=443 proto=TCP action=deny severity=high",
            "Sangfor: src=10.0.0.5 dst=172.16.0.10 sport=12345 dport=80 proto=TCP action=allow severity=medium",
            "date=2026-05-15 src=10.10.10.10 dst=10.10.20.20 sport=60000 dport=22 proto=TCP action=deny severity=critical",
        ];

        for line in lines {
            assert_eq!(
                SangforAdapter.parse(raw(line)).parse_status,
                ParseStatus::Parsed
            );
        }

        let failed = SangforAdapter.parse(raw("this is not a valid firewall log"));
        assert_eq!(failed.parse_status, ParseStatus::Failed);
        assert!(failed.parse_error.is_some());
    }

    #[test]
    fn parses_chinese_sangfor_nat_line() {
        let event = SangforAdapter.parse(raw("Apr 23 20:09:52 localhost nat: 日志类型:NAT日志, NAT类型:snat, 源IP:2.55.80.6, 源端口:54213, 目的IP:211.93.49.88, 目的端口:46541, 协议:17, 转换后的IP:58.216.48.6, 转换后的端口:54213"));

        assert_eq!(event.parse_status, ParseStatus::Parsed);
        assert_eq!(event.src_ip.as_deref(), Some("2.55.80.6"));
        assert_eq!(event.src_port, Some(54213));
        assert_eq!(event.dst_ip.as_deref(), Some("211.93.49.88"));
        assert_eq!(event.dst_port, Some(46541));
        assert_eq!(event.protocol.as_deref(), Some("UDP"));
        assert_eq!(event.action.as_deref(), Some("snat"));
    }
}
