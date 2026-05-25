use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info};

use fwlog_domain::{CanonicalEvent, ParseStatus};

/// ClickHouse storage client for historical data
pub struct ClickHouseStorage {
    client: Client,
    database: String,
}

impl ClickHouseStorage {
    /// Create a new ClickHouse storage client
    pub fn new(url: &str, database: &str) -> Result<Self> {
        let client = Client::default()
            .with_url(url)
            .with_database(database)
            .with_compression(clickhouse::Compression::Lz4);

        info!(
            url = url,
            database = database,
            "clickhouse storage initialized"
        );

        Ok(Self {
            client,
            database: database.to_string(),
        })
    }

    /// Insert a batch of events (async)
    pub async fn insert_batch(&self, events: &[CanonicalEvent]) -> Result<usize> {
        if events.is_empty() {
            return Ok(0);
        }

        let mut insert = self
            .client
            .insert("events")
            .context("failed to create insert")?;

        for event in events {
            let ch_event = ClickHouseEvent::from(event);
            insert
                .write(&ch_event)
                .await
                .context("failed to write row")?;
        }

        insert.end().await.context("failed to end insert")?;

        debug!(count = events.len(), "inserted events to clickhouse");
        Ok(events.len())
    }

    /// Query events with complex filters (EventQuery)
    pub async fn query_events_complex(
        &self,
        query: &crate::EventQuery,
        limit: usize,
    ) -> Result<Vec<CanonicalEvent>> {
        let mut sql = "SELECT * FROM events WHERE 1=1".to_string();

        if let Some(day) = &query.day {
            sql.push_str(&format!(" AND toDate(ingest_time) = '{}'", self.escape_sql(day)));
        }

        if let Some(date_from) = &query.date_from {
            if let Ok(dt) = crate::parse_any_date(date_from) {
                sql.push_str(&format!(
                    " AND ingest_time >= '{}'",
                    dt.format("%Y-%m-%d %H:%M:%S")
                ));
            }
        }

        if let Some(date_to) = &query.date_to {
            if let Ok(dt) = crate::parse_any_date(date_to) {
                sql.push_str(&format!(
                    " AND ingest_time <= '{}'",
                    dt.format("%Y-%m-%d %H:%M:%S")
                ));
            }
        }


        if let Some(src_ip) = &query.src_ip {
            sql.push_str(&format!(" AND src_ip = '{}'", self.escape_sql(src_ip)));
        }

        if let Some(dst_ip) = &query.dst_ip {
            sql.push_str(&format!(" AND dst_ip = '{}'", self.escape_sql(dst_ip)));
        }

        if let Some(protocol) = &query.protocol {
            sql.push_str(&format!(" AND protocol = '{}'", self.escape_sql(protocol)));
        }

        if let Some(action) = &query.action {
            sql.push_str(&format!(" AND action = '{}'", self.escape_sql(action)));
        }

        if let Some(device_id) = &query.device_id {
            sql.push_str(&format!(" AND device_id = '{}'", self.escape_sql(device_id)));
        }

        if let Some(keyword) = &query.keyword {
            let escaped = self.escape_sql(keyword);
            sql.push_str(&format!(
                " AND (raw LIKE '%{}%' OR parse_error LIKE '%{}%')",
                escaped, escaped
            ));
        }

        if !query.include_failed {
            sql.push_str(" AND parse_status = 'parsed'");
        }

        sql.push_str(&format!(" ORDER BY ingest_time DESC LIMIT {}", limit));

        debug!(sql = sql, "executing clickhouse complex query");

        let rows = self
            .client
            .query(&sql)
            .fetch_all::<ClickHouseEvent>()
            .await
            .context("failed to query events complex")?;

        Ok(rows.into_iter().map(|r| r.into()).collect())
    }

    fn escape_sql(&self, s: &str) -> String {
        s.replace('\'', "''")
    }

    /// Get total event count
    pub async fn count_events(&self) -> Result<u64> {
        let count: u64 = self
            .client
            .query("SELECT count() FROM events")
            .fetch_one()
            .await
            .context("failed to count events")?;

        Ok(count)
    }

    /// Get database size in bytes
    pub async fn database_size(&self) -> Result<u64> {
        let size: u64 = self
            .client
            .query(&format!(
                "SELECT sum(bytes_on_disk) FROM system.parts WHERE database = '{}'",
                self.database
            ))
            .fetch_one()
            .await
            .context("failed to get database size")?;

        Ok(size)
    }

    /// Health check
    pub async fn ping(&self) -> Result<()> {
        let _: u8 = self
            .client
            .query("SELECT 1")
            .fetch_one()
            .await
            .context("clickhouse ping failed")?;

        Ok(())
    }
}

/// ClickHouse event row structure
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
struct ClickHouseEvent {
    event_id: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    ingest_time: DateTime<Utc>,
    source_addr: String,
    device_id: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    event_time: DateTime<Utc>,
    vendor: String,
    product: String,
    src_ip: String,
    src_port: u16,
    dst_ip: String,
    dst_port: u16,
    protocol: String,
    action: String,
    severity: String,
    raw: String,
    parse_status: String,
    parse_error: String,
}

impl From<&CanonicalEvent> for ClickHouseEvent {
    fn from(event: &CanonicalEvent) -> Self {
        Self {
            event_id: event.event_id.clone(),
            ingest_time: event.ingest_time,
            source_addr: event.source_addr.clone(),
            device_id: event.device_id.clone().unwrap_or_default(),
            event_time: event.event_time.unwrap_or(event.ingest_time),
            vendor: event.vendor.clone().unwrap_or_default(),
            product: event.product.clone().unwrap_or_default(),
            src_ip: event.src_ip.clone().unwrap_or_default(),
            src_port: event.src_port.unwrap_or(0),
            dst_ip: event.dst_ip.clone().unwrap_or_default(),
            dst_port: event.dst_port.unwrap_or(0),
            protocol: event.protocol.clone().unwrap_or_default(),
            action: event.action.clone().unwrap_or_default(),
            severity: event.severity.clone().unwrap_or_default(),
            raw: event.raw.clone(),
            parse_status: status_str(event.parse_status).to_string(),
            parse_error: event.parse_error.clone().unwrap_or_default(),
        }
    }
}

impl From<ClickHouseEvent> for CanonicalEvent {
    fn from(ch: ClickHouseEvent) -> Self {
        Self {
            event_id: ch.event_id,
            ingest_time: ch.ingest_time,
            source_addr: ch.source_addr,
            device_id: if ch.device_id.is_empty() {
                None
            } else {
                Some(ch.device_id)
            },
            event_time: Some(ch.event_time),
            vendor: if ch.vendor.is_empty() {
                None
            } else {
                Some(ch.vendor)
            },
            product: if ch.product.is_empty() {
                None
            } else {
                Some(ch.product)
            },
            src_ip: if ch.src_ip.is_empty() {
                None
            } else {
                ch.src_ip.parse().ok()
            },
            src_port: if ch.src_port == 0 {
                None
            } else {
                Some(ch.src_port)
            },
            dst_ip: if ch.dst_ip.is_empty() {
                None
            } else {
                ch.dst_ip.parse().ok()
            },
            dst_port: if ch.dst_port == 0 {
                None
            } else {
                Some(ch.dst_port)
            },
            protocol: if ch.protocol.is_empty() {
                None
            } else {
                Some(ch.protocol)
            },
            action: if ch.action.is_empty() {
                None
            } else {
                Some(ch.action)
            },
            severity: if ch.severity.is_empty() {
                None
            } else {
                Some(ch.severity)
            },
            raw: ch.raw,
            parse_status: parse_status_from_str(&ch.parse_status),
            parse_error: if ch.parse_error.is_empty() {
                None
            } else {
                Some(ch.parse_error)
            },
        }
    }
}

fn status_str(status: ParseStatus) -> &'static str {
    match status {
        ParseStatus::Parsed => "parsed",
        ParseStatus::Partial => "partial",
        ParseStatus::Failed => "failed",
    }
}

fn parse_status_from_str(value: &str) -> ParseStatus {
    match value {
        "parsed" => ParseStatus::Parsed,
        "partial" => ParseStatus::Partial,
        _ => ParseStatus::Failed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn canonical_event_round_trips_through_clickhouse_row() {
        let event = CanonicalEvent {
            event_id: "evt-1".to_string(),
            ingest_time: Utc.timestamp_opt(1_778_808_000, 123_000_000).unwrap(),
            source_addr: "udp://192.168.1.10:514".to_string(),
            device_id: Some("fw-edge-01".to_string()),
            event_time: Some(Utc.timestamp_opt(1_778_807_940, 456_000_000).unwrap()),
            vendor: Some("Sangfor".to_string()),
            product: Some("Firewall".to_string()),
            src_ip: Some("192.168.1.20".to_string()),
            src_port: Some(54_321),
            dst_ip: Some("10.0.0.8".to_string()),
            dst_port: Some(443),
            protocol: Some("TCP".to_string()),
            action: Some("allow".to_string()),
            severity: Some("medium".to_string()),
            raw: "src=192.168.1.20 dst=10.0.0.8 action=allow".to_string(),
            parse_status: ParseStatus::Partial,
            parse_error: Some("missing policy id".to_string()),
        };

        let row = ClickHouseEvent::from(&event);

        assert_eq!(row.event_id, event.event_id);
        assert_eq!(row.ingest_time, event.ingest_time);
        assert_eq!(row.source_addr, event.source_addr);
        assert_eq!(row.device_id, "fw-edge-01");
        assert_eq!(row.event_time, event.event_time.unwrap());
        assert_eq!(row.vendor, "Sangfor");
        assert_eq!(row.product, "Firewall");
        assert_eq!(row.src_ip, "192.168.1.20");
        assert_eq!(row.src_port, 54_321);
        assert_eq!(row.dst_ip, "10.0.0.8");
        assert_eq!(row.dst_port, 443);
        assert_eq!(row.protocol, "TCP");
        assert_eq!(row.action, "allow");
        assert_eq!(row.severity, "medium");
        assert_eq!(row.raw, event.raw);
        assert_eq!(row.parse_status, "partial");
        assert_eq!(row.parse_error, "missing policy id");

        let round_tripped = CanonicalEvent::from(row);

        assert_eq!(round_tripped, event);
    }

    #[tokio::test]
    #[ignore] // Requires running ClickHouse
    async fn test_clickhouse_connection() {
        let storage = ClickHouseStorage::new("http://localhost:8123", "oxidelog").unwrap();
        storage.ping().await.unwrap();
    }
}
