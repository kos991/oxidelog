use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info};

use fwlog_domain::CanonicalEvent;

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
            insert.write(&ch_event).await.context("failed to write row")?;
        }

        insert.end().await.context("failed to end insert")?;

        debug!(count = events.len(), "inserted events to clickhouse");
        Ok(events.len())
    }

    /// Query events by time range and filters
    pub async fn query_events(
        &self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        source_addr: Option<&str>,
        protocol: Option<&str>,
        limit: usize,
    ) -> Result<Vec<CanonicalEvent>> {
        let mut sql = format!(
            "SELECT * FROM events WHERE ingest_time >= '{}' AND ingest_time < '{}'",
            start_time.format("%Y-%m-%d %H:%M:%S"),
            end_time.format("%Y-%m-%d %H:%M:%S")
        );

        if let Some(addr) = source_addr {
            sql.push_str(&format!(" AND source_addr = '{}'", addr));
        }

        if let Some(proto) = protocol {
            sql.push_str(&format!(" AND protocol = '{}'", proto));
        }

        sql.push_str(&format!(" ORDER BY ingest_time DESC LIMIT {}", limit));

        let rows = self
            .client
            .query(&sql)
            .fetch_all::<ClickHouseEvent>()
            .await
            .context("failed to query events")?;

        Ok(rows.into_iter().map(|r| r.into()).collect())
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
    ingest_time: DateTime<Utc>,
    source_addr: String,
    device_id: String,
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
            src_ip: event.src_ip.map(|ip| ip.to_string()).unwrap_or_default(),
            src_port: event.src_port.unwrap_or(0),
            dst_ip: event.dst_ip.map(|ip| ip.to_string()).unwrap_or_default(),
            dst_port: event.dst_port.unwrap_or(0),
            protocol: event.protocol.clone().unwrap_or_default(),
            action: event.action.clone().unwrap_or_default(),
            severity: event.severity.clone().unwrap_or_default(),
            raw: event.raw.clone(),
            parse_status: event.parse_status.clone(),
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
            parse_status: ch.parse_status,
            parse_error: if ch.parse_error.is_empty() {
                None
            } else {
                Some(ch.parse_error)
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Requires running ClickHouse
    async fn test_clickhouse_connection() {
        let storage = ClickHouseStorage::new("http://localhost:8123", "oxidelog").unwrap();
        storage.ping().await.unwrap();
    }
}
