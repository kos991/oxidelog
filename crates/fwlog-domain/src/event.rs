use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ParseStatus {
    Parsed,
    Partial,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalEvent {
    pub event_id: String,
    pub ingest_time: DateTime<Utc>,
    pub source_addr: String,
    pub device_id: Option<String>,
    pub event_time: Option<DateTime<Utc>>,
    pub vendor: Option<String>,
    pub product: Option<String>,
    pub src_ip: Option<String>,
    pub src_port: Option<u16>,
    pub dst_ip: Option<String>,
    pub dst_port: Option<u16>,
    pub protocol: Option<String>,
    pub action: Option<String>,
    pub severity: Option<String>,
    pub raw: String,
    pub parse_status: ParseStatus,
    pub parse_error: Option<String>,
}

pub fn make_event_id(raw: &str, ingest_time: DateTime<Utc>, source_addr: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    hasher.update(
        ingest_time
            .timestamp_nanos_opt()
            .unwrap_or_default()
            .to_string(),
    );
    hasher.update(source_addr.as_bytes());
    format!("{:x}", hasher.finalize())
}

impl CanonicalEvent {
    pub fn failed(raw: crate::RawLog, reason: impl Into<String>) -> Self {
        Self {
            event_id: make_event_id(&raw.raw, raw.ingest_time, &raw.source_addr),
            ingest_time: raw.ingest_time,
            source_addr: raw.source_addr,
            device_id: None,
            event_time: None,
            vendor: None,
            product: None,
            src_ip: None,
            src_port: None,
            dst_ip: None,
            dst_port: None,
            protocol: None,
            action: None,
            severity: None,
            raw: raw.raw,
            parse_status: ParseStatus::Failed,
            parse_error: Some(reason.into()),
        }
    }

    pub fn classify_firewall_tuple(&mut self) {
        let has_src = self
            .src_ip
            .as_deref()
            .is_some_and(|value| !value.is_empty());
        let has_dst = self
            .dst_ip
            .as_deref()
            .is_some_and(|value| !value.is_empty());
        let has_action_or_protocol = self
            .action
            .as_deref()
            .is_some_and(|value| !value.is_empty())
            || self
                .protocol
                .as_deref()
                .is_some_and(|value| !value.is_empty());

        if has_src && has_dst && has_action_or_protocol {
            self.parse_status = ParseStatus::Parsed;
            self.parse_error = None;
        } else if has_src || has_dst || has_action_or_protocol {
            self.parse_status = ParseStatus::Partial;
            self.parse_error =
                Some("partial parse: minimum searchable tuple incomplete".to_string());
        } else {
            self.parse_status = ParseStatus::Failed;
            if self.parse_error.is_none() {
                self.parse_error = Some("failed parse: no useful canonical fields".to_string());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn parsed_events_serialize_status_as_lowercase() {
        let event = CanonicalEvent {
            event_id: "id".to_string(),
            ingest_time: Utc.timestamp_opt(1_778_808_000, 0).unwrap(),
            source_addr: "udp://192.168.1.1:514".to_string(),
            device_id: None,
            event_time: None,
            vendor: Some("Sangfor".to_string()),
            product: Some("Firewall".to_string()),
            src_ip: None,
            src_port: None,
            dst_ip: None,
            dst_port: None,
            protocol: None,
            action: None,
            severity: None,
            raw: "raw".to_string(),
            parse_status: ParseStatus::Parsed,
            parse_error: None,
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains(r#""parse_status":"parsed""#));
    }

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
        assert!(event
            .parse_error
            .as_deref()
            .unwrap()
            .contains("minimum searchable tuple"));

        event.src_ip = None;
        event.dst_ip = None;
        event.classify_firewall_tuple();
        assert_eq!(event.parse_status, ParseStatus::Failed);
    }

    #[test]
    fn failed_events_preserve_raw() {
        let raw = crate::RawLog {
            ingest_time: Utc.timestamp_opt(1_778_808_000, 0).unwrap(),
            source_addr: "tcp://127.0.0.1:1514".to_string(),
            raw: "not a firewall log".to_string(),
        };

        let event = CanonicalEvent::failed(raw, "missing required fields");

        assert_eq!(event.raw, "not a firewall log");
        assert_eq!(event.source_addr, "tcp://127.0.0.1:1514");
        assert_eq!(event.parse_status, ParseStatus::Failed);
        assert_eq!(
            event.parse_error.as_deref(),
            Some("missing required fields")
        );
    }

    #[test]
    fn event_id_is_stable_for_same_raw_source_and_timestamp() {
        let ts = Utc.timestamp_opt(1_778_808_000, 42).unwrap();

        let first = make_event_id("raw", ts, "tcp://127.0.0.1:1514");
        let second = make_event_id("raw", ts, "tcp://127.0.0.1:1514");

        assert_eq!(first, second);
    }
}
