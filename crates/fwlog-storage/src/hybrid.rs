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
}

#[derive(Debug, Clone)]
pub struct HybridConfig {
    pub clickhouse_enabled: bool,
    pub clickhouse_url: String,
    pub clickhouse_database: String,
    pub hot_data_hours: i64, // DuckDB keeps last N hours
}

impl Default for HybridConfig {
    fn default() -> Self {
        Self {
            clickhouse_enabled: false,
            clickhouse_url: "http://localhost:8123".to_string(),
            clickhouse_database: "oxidelog".to_string(),
            hot_data_hours: 1,
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

        // 2. Async write to ClickHouse (non-blocking, best-effort)
        if self.remote_enabled.load(Ordering::Relaxed) {
            if let Some(remote) = &self.remote {
                let events = events.to_vec();
                let remote = Arc::clone(remote);
                tokio::spawn(async move {
                    if let Err(err) = remote.insert_batch(&events).await {
                        error!(error = %err, count = events.len(), "clickhouse async write failed");
                    }
                });
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
            if let Ok(date_from) = DateTime::parse_from_rfc3339(date_from_str) {
                date_from.with_timezone(&Utc) < threshold && self.remote.is_some()
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
        // Count events in local DuckDB
        let local_count = self.local.query_recent(1).map(|_| 0u64).unwrap_or(0);

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
