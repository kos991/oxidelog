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
        let src = capture(raw.raw.as_str(), src_regex());
        let dst = capture(raw.raw.as_str(), dst_regex());
        let action = capture(raw.raw.as_str(), action_regex());

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
            src_port: capture(raw.raw.as_str(), sport_regex()).and_then(|v| v.parse().ok()),
            dst_ip: dst,
            dst_port: capture(raw.raw.as_str(), dport_regex()).and_then(|v| v.parse().ok()),
            protocol: capture(raw.raw.as_str(), proto_regex()).map(|v| v.to_uppercase()),
            action,
            severity: capture(raw.raw.as_str(), severity_regex()),
            raw: raw.raw,
            parse_status: ParseStatus::Parsed,
            parse_error: None,
        }
    }
}

fn capture(input: &str, regex: &Regex) -> Option<String> {
    regex
        .captures(input)
        .and_then(|caps| caps.get(1))
        .map(|value| value.as_str().trim().to_string())
}

fn src_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r"(?:^|\s)src=([0-9a-fA-F:.]+)").unwrap())
}

fn dst_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r"(?:^|\s)dst=([0-9a-fA-F:.]+)").unwrap())
}

fn sport_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r"(?:^|\s)sport=(\d+)").unwrap())
}

fn dport_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r"(?:^|\s)dport=(\d+)").unwrap())
}

fn proto_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r"(?:^|\s)proto=([A-Za-z0-9_-]+)").unwrap())
}

fn action_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r"(?:^|\s)action=([A-Za-z0-9_-]+)").unwrap())
}

fn severity_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r"(?:^|\s)severity=([A-Za-z0-9_-]+)").unwrap())
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
}
