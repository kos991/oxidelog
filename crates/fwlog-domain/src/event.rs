use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ParseStatus {
    Parsed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalEvent {
    pub event_id: String,
    pub ingest_time: DateTime<Utc>,
    pub source_addr: String,
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
    hasher.update(ingest_time.timestamp_nanos_opt().unwrap_or_default().to_string());
    hasher.update(source_addr.as_bytes());
    format!("{:x}", hasher.finalize())
}

impl CanonicalEvent {
    pub fn failed(raw: crate::RawLog, reason: impl Into<String>) -> Self {
        Self {
            event_id: make_event_id(&raw.raw, raw.ingest_time, &raw.source_addr),
            ingest_time: raw.ingest_time,
            source_addr: raw.source_addr,
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
        assert_eq!(event.parse_error.as_deref(), Some("missing required fields"));
    }

    #[test]
    fn event_id_is_stable_for_same_raw_source_and_timestamp() {
        let ts = Utc.timestamp_opt(1_778_808_000, 42).unwrap();

        let first = make_event_id("raw", ts, "tcp://127.0.0.1:1514");
        let second = make_event_id("raw", ts, "tcp://127.0.0.1:1514");

        assert_eq!(first, second);
    }
}
