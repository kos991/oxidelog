use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use tracing::{error, info, warn};

use crate::{clickhouse::ClickHouseStorage, DuckDbStore};
use fwlog_domain::CanonicalEvent;

/// Hybrid storage combining DuckDB (hot) and ClickHouse (historical)
pub struct HybridStorage {
    local: Arc<DuckDbStore>,
    remote: Option<Arc<ClickHouseStorage>>,
    remote_enabled: AtomicBool,
    config: HybridConfig,
    write_semaphore: Arc<tokio::sync::Semaphore>,
}

#[derive(Debug, Clone)]
pub struct HybridConfig {
    pub clickhouse_enabled: bool,
    pub clickhouse_url: String,
    pub clickhouse_database: String,
    pub hot_data_hours: i64, // DuckDB keeps last N hours
    pub max_concurrent_writes: usize,
}

impl Default for HybridConfig {
    fn default() -> Self {
        Self {
            clickhouse_enabled: false,
            clickhouse_url: "http://localhost:8123".to_string(),
            clickhouse_database: "oxidelog".to_string(),
            hot_data_hours: 1,
            max_concurrent_writes: 16,
        }
    }
}

impl HybridStorage {
    /// Create a new hybrid storage
    pub fn new(local: Arc<DuckDbStore>, config: HybridConfig) -> Result<Self> {
        let remote = if config.clickhouse_enabled {
            match ClickHouseStorage::new(&config.clickhouse_url, &config.clickhouse_database) {
                Ok(ch) => {
                    info!("clickhouse storage enabled");
                    Some(Arc::new(ch))
                }
                Err(err) => {
                    error!(error = %err, "failed to initialize clickhouse, running in local-only mode");
                    None
                }
            }
        } else {
            info!("clickhouse storage disabled, running in local-only mode");
            None
        };

        Ok(Self {
            local,
            remote,
            remote_enabled: AtomicBool::new(config.clickhouse_enabled),
            write_semaphore: Arc::new(tokio::sync::Semaphore::new(config.max_concurrent_writes)),
            config,
        })
    }

    /// Insert a batch of events (dual-write strategy)
    pub fn insert_batch(&self, events: &[CanonicalEvent]) -> Result<usize> {
        // 1. Always write to local DuckDB (primary, blocking)
        let local_inserted = self
            .local
            .insert_batch(events)
            .context("local duckdb insert failed")?;

        // 2. Async write to ClickHouse (non-blocking, best-effort with backpressure)
        if self.remote_enabled.load(Ordering::Relaxed) {
            if let Some(remote) = &self.remote {
                // Only clone and spawn if we have capacity to write
                if let Ok(permit) = Arc::clone(&self.write_semaphore).try_acquire_owned() {
                    let events_vec = events.to_vec();
                    let remote_clone = Arc::clone(remote);
                    
                    tokio::spawn(async move {
                        let _permit = permit; // Hold permit until write finishes
                        if let Err(err) = remote_clone.insert_batch(&events_vec).await {
                            error!(error = %err, count = events_vec.len(), "clickhouse async write failed");
                        }
                    });
                } else {
                    // Backpressure: drop the remote write if we are at capacity
                    // Local write is already done, so data is safe in DuckDB
                    warn!(count = events.len(), "clickhouse write queue full, dropping remote batch to protect system memory");
                }
            }
        }

        Ok(local_inserted)
    }

    /// Query events with automatic routing based on EventQuery
    pub async fn query_events_with_query(
        &self,
        query: &crate::EventQuery,
        limit: usize,
    ) -> Result<Vec<CanonicalEvent>> {
        let threshold = Utc::now() - Duration::hours(self.config.hot_data_hours);

        // Determine if we should use ClickHouse based on date_from
        let use_remote = if let Some(date_from_str) = &query.date_from {
            if let Ok(date_from) = crate::parse_any_date(date_from_str) {
                date_from < threshold && self.remote.is_some()
            } else {
                false
            }
        } else if let Some(day) = &query.day {
            // If day is provided, check if it's today
            let today = Utc::now().format("%Y-%m-%d").to_string();
            day != &today && self.remote.is_some()
        } else {
            false
        };

        if use_remote && self.remote_enabled.load(Ordering::Relaxed) {
            info!("routing complex query to clickhouse");
            self.query_remote_complex(query, limit).await
        } else {
            info!("routing complex query to local duckdb");
            self.local.query_events_without_raw(query, limit)
        }
    }

    async fn query_remote_complex(
        &self,
        query: &crate::EventQuery,
        limit: usize,
    ) -> Result<Vec<CanonicalEvent>> {
        let remote = self.remote.as_ref().context("clickhouse not initialized")?;

        remote.query_events_complex(query, limit).await
    }

    /// Determine query target based on time range
    fn route_query(&self, start_time: DateTime<Utc>) -> QueryTarget {
        let hot_threshold = Utc::now() - Duration::hours(self.config.hot_data_hours);

        if start_time >= hot_threshold {
            QueryTarget::Local
        } else if self.remote.is_some() {
            QueryTarget::Remote
        } else {
            warn!("historical query requested but clickhouse not available, falling back to local");
            QueryTarget::Local
        }
    }

    /// Query from local DuckDB
    fn query_local(
        &self,
        _start_time: DateTime<Utc>,
        _end_time: DateTime<Utc>,
        _source_addr: Option<&str>,
        _protocol: Option<&str>,
        limit: usize,
    ) -> Result<Vec<CanonicalEvent>> {
        // Use DuckDB's existing query_recent method for hot data
        // For more complex queries, use query_events with EventQuery
        self.local.query_recent(limit)
    }

    /// Query from remote ClickHouse
    async fn query_remote(
        &self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        source_addr: Option<&str>,
        protocol: Option<&str>,
        limit: usize,
    ) -> Result<Vec<CanonicalEvent>> {
        let remote = self.remote.as_ref().context("clickhouse not initialized")?;

        remote
            .query_events(start_time, end_time, source_addr, protocol, limit)
            .await
    }

    /// Get storage statistics
    pub async fn stats(&self) -> Result<HybridStats> {
        // Count events in local DuckDB (real count)
        let local_count = match self.local.event_stats() {
            Ok(stats) => stats.total,
            Err(_) => 0,
        };

        let (remote_count, remote_size_bytes) = if let Some(remote) = &self.remote {
            match tokio::try_join!(remote.count_events(), remote.database_size()) {
                Ok((count, size)) => (Some(count), Some(size)),
                Err(err) => {
                    warn!(error = %err, "failed to get clickhouse stats");
                    (None, None)
                }
            }
        } else {
            (None, None)
        };

        Ok(HybridStats {
            local_count,
            remote_count,
            remote_size_bytes,
        })
    }

    /// Health check
    pub async fn health_check(&self) -> HybridHealth {
        let local_ok = self.local.query_recent(1).is_ok();

        let remote_ok = if let Some(remote) = &self.remote {
            remote.ping().await.is_ok()
        } else {
            false
        };

        HybridHealth {
            local_ok,
            remote_ok,
            remote_enabled: self.remote_enabled.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum QueryTarget {
    Local,
    Remote,
}

#[derive(Debug, Clone)]
pub struct HybridStats {
    pub local_count: u64,
    pub remote_count: Option<u64>,
    pub remote_size_bytes: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct HybridHealth {
    pub local_ok: bool,
    pub remote_ok: bool,
    pub remote_enabled: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use fwlog_domain::{ParseStatus, RawLog};

    fn parsed_event(id: &str) -> CanonicalEvent {
        let raw = RawLog {
            ingest_time: Utc.timestamp_opt(1_778_808_000, 0).unwrap(),
            source_addr: "tcp://127.0.0.1:1514".to_string(),
            raw: format!("raw {id}"),
        };
        let mut event = CanonicalEvent::failed(raw, "bad");
        event.event_id = id.to_string();
        event.parse_status = ParseStatus::Parsed;
        event.vendor = Some("Sangfor".to_string());
        event.product = Some("Firewall".to_string());
        event.src_ip = Some("192.168.1.10".to_string());
        event.dst_ip = Some("8.8.8.8".to_string());
        event.protocol = Some("TCP".to_string());
        event.action = Some("allow".to_string());
        event.parse_error = None;
        event
    }

    #[test]
    fn insert_batch_writes_to_local_duckdb_when_clickhouse_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let local = Arc::new(DuckDbStore::open(&db_path).unwrap());
        let storage = HybridStorage::new(
            Arc::clone(&local),
            HybridConfig {
                clickhouse_enabled: false,
                ..HybridConfig::default()
            },
        )
        .unwrap();

        let inserted = storage.insert_batch(&[parsed_event("hybrid-local-1")]).unwrap();

        assert_eq!(inserted, 1);
        assert_eq!(local.event_stats().unwrap().total, 1);
    }

    #[tokio::test]
    async fn query_events_uses_local_duckdb_when_clickhouse_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let local = Arc::new(DuckDbStore::open(&db_path).unwrap());
        let storage = HybridStorage::new(
            Arc::clone(&local),
            HybridConfig {
                clickhouse_enabled: false,
                ..HybridConfig::default()
            },
        )
        .unwrap();
        let first = parsed_event("hybrid-query-1");
        let second = parsed_event("hybrid-query-2");
        storage.insert_batch(&[first.clone(), second.clone()]).unwrap();

        let rows = storage
            .query_events_with_query(&crate::EventQuery::default(), 10)
            .await
            .unwrap();

        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|event| event.event_id == first.event_id));
        assert!(rows.iter().any(|event| event.event_id == second.event_id));
    }

    #[tokio::test]
    async fn health_check_reports_local_ok_and_remote_disabled_without_clickhouse() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let local = Arc::new(DuckDbStore::open(&db_path).unwrap());
        let storage = HybridStorage::new(
            local,
            HybridConfig {
                clickhouse_enabled: false,
                ..HybridConfig::default()
            },
        )
        .unwrap();

        let health = storage.health_check().await;

        assert!(health.local_ok);
        assert!(!health.remote_ok);
        assert!(!health.remote_enabled);
    }
}
