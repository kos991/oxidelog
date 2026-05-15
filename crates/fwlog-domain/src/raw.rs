use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawLog {
    pub ingest_time: DateTime<Utc>,
    pub source_addr: String,
    pub raw: String,
}

impl RawLog {
    pub fn new(source_addr: impl Into<String>, raw: impl Into<String>) -> Self {
        Self {
            ingest_time: Utc::now(),
            source_addr: source_addr.into(),
            raw: raw.into(),
        }
    }
}
