use std::sync::OnceLock;

use fwlog_domain::{make_event_id, CanonicalEvent, ParseStatus, RawLog};
use regex::Regex;

pub trait LogAdapter {
    fn parse(&self, raw: RawLog) -> CanonicalEvent;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SangforAdapter;

impl LogAdapter for SangforAdapter {
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

            match key {
                "src" | "源IP" => {
                    if src.is_none() {
                        src = Some(val.to_string());
                    }
                }
                "dst" | "目的IP" => {
                    if dst.is_none() {
                        dst = Some(val.to_string());
                    }
                }
                "sport" | "源端口" => {
                    if sport.is_none() {
                        sport = val.parse().ok();
                    }
                }
                "dport" | "目的端口" => {
                    if dport.is_none() {
                        dport = val.parse().ok();
                    }
                }
                "proto" | "协议" => {
                    if proto.is_none() {
                        proto = Some(normalize_protocol(val.to_string()));
                    }
                }
                "action" | "NAT类型" => {
                    if action.is_none() {
                        action = Some(val.to_string());
                    }
                }
                "severity" => {
                    if severity.is_none() {
                        severity = Some(val.to_string());
                    }
                }
                _ => {}
            }
        }

        if src.is_none() || dst.is_none() || action.is_none() {
            return CanonicalEvent::failed(raw, "missing required fields: src, dst, action");
        }

        CanonicalEvent {
            event_id: make_event_id(&raw.raw, raw.ingest_time, &raw.source_addr),
            ingest_time: raw.ingest_time,
            event_time: None,
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
        }
    }
}

fn kv_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"(?:\s|^)(src|dst|sport|dport|proto|action|severity|源IP|目的IP|源端口|目的端口|协议|NAT类型)(?:=|:)\s*([^\s,]+)")
            .unwrap()
    })
}

fn normalize_protocol(value: String) -> String {
    match value.as_str() {
        "1" => "ICMP".to_string(),
        "6" => "TCP".to_string(),
        "17" => "UDP".to_string(),
        _ => value.to_uppercase(),
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
            assert_eq!(SangforAdapter.parse(raw(line)).parse_status, ParseStatus::Parsed);
        }

        let failed = SangforAdapter.parse(raw("this is not a valid firewall log"));
        assert_eq!(failed.parse_status, ParseStatus::Failed);
        assert!(failed.parse_error.is_some());
    }

    #[test]
    fn parses_chinese_sangfor_nat_line() {
        let event = SangforAdapter.parse(raw("Apr 29 00:00:09 localhost nat: 日志类型:NAT日志, NAT类型:snat, 源IP:192.168.0.105, 源端口:21527, 目的IP:10.4.90.205, 目的端口:2048, 协议:1, 转换后的IP:58.216.48.6, 转换后的端口:21527"));

        assert_eq!(event.parse_status, ParseStatus::Parsed);
        assert_eq!(event.src_ip.as_deref(), Some("192.168.0.105"));
        assert_eq!(event.src_port, Some(21527));
        assert_eq!(event.dst_ip.as_deref(), Some("10.4.90.205"));
        assert_eq!(event.dst_port, Some(2048));
        assert_eq!(event.protocol.as_deref(), Some("ICMP"));
        assert_eq!(event.action.as_deref(), Some("snat"));
    }
}
