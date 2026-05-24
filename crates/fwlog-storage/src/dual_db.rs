use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use duckdb::Connection;
use tracing::{error, info, warn};

use crate::DuckDbStore;

/// Dual-database rotation manager for read-write separation
///
/// Architecture:
/// - Write DB: Worker writes here (oxidelog-write.duckdb)
/// - Query DB: API reads here (oxidelog-query.duckdb)
/// - Sync task: Periodically copies incremental data from write to query
pub struct DualDbManager {
    write_path: PathBuf,
    query_path: PathBuf,
    config: DualDbConfig,
    metrics: Arc<DualDbMetrics>,
    last_sync_id: Arc<RwLock<String>>,
}

#[derive(Debug, Clone)]
pub struct DualDbConfig {
    pub enabled: bool,
    pub sync_interval_seconds: u64,
}

impl Default for DualDbConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            sync_interval_seconds: 300,
        }
    }
}

#[derive(Debug, Default)]
pub struct DualDbMetrics {
    sync_count: AtomicU64,
    sync_errors: AtomicU64,
    last_sync_duration_ms: AtomicU64,
    last_sync_rows: AtomicU64,
}

impl DualDbMetrics {
    pub fn sync_count(&self) -> u64 {
        self.sync_count.load(Ordering::Relaxed)
    }

    pub fn sync_errors(&self) -> u64 {
        self.sync_errors.load(Ordering::Relaxed)
    }

    pub fn last_sync_duration_ms(&self) -> u64 {
        self.last_sync_duration_ms.load(Ordering::Relaxed)
    }

    pub fn last_sync_rows(&self) -> u64 {
        self.last_sync_rows.load(Ordering::Relaxed)
    }

    fn record_sync(&self, duration: Duration, rows: u64) {
        self.sync_count.fetch_add(1, Ordering::Relaxed);
        self.last_sync_duration_ms
            .store(duration.as_millis() as u64, Ordering::Relaxed);
        self.last_sync_rows.store(rows, Ordering::Relaxed);
    }

    fn record_error(&self) {
        self.sync_errors.fetch_add(1, Ordering::Relaxed);
    }
}

impl DualDbManager {
    /// Create a new dual-database manager
    pub fn new(base_path: impl AsRef<Path>, config: DualDbConfig) -> Result<Self> {
        let base_path = base_path.as_ref();
        let parent = base_path
            .parent()
            .context("duckdb path must have parent directory")?;

        let write_path = parent.join("oxidelog-write.duckdb");
        let query_path = parent.join("oxidelog-query.duckdb");

        let manager = Self {
            write_path,
            query_path,
            config,
            metrics: Arc::new(DualDbMetrics::default()),
            last_sync_id: Arc::new(RwLock::new(String::new())),
        };

        if config.enabled {
            manager.initialize(base_path)?;
        }

        Ok(manager)
    }

    /// Initialize dual-database setup
    fn initialize(&self, legacy_path: &Path) -> Result<()> {
        fs::create_dir_all(
            self.write_path
                .parent()
                .context("write path must have parent")?,
        )
        .context("create duckdb directory")?;

        // If legacy single database exists, migrate to dual-database mode
        if legacy_path.exists() && !self.write_path.exists() && !self.query_path.exists() {
            info!(
                legacy = %legacy_path.display(),
                write = %self.write_path.display(),
                query = %self.query_path.display(),
                "migrating single database to dual-database mode"
            );
            self.migrate_from_single_db(legacy_path)?;
        }

        // Ensure both databases exist
        if !self.write_path.exists() {
            info!(path = %self.write_path.display(), "initializing write database");
            DuckDbStore::open(&self.write_path)?;
        }

        if !self.query_path.exists() {
            info!(path = %self.query_path.display(), "initializing query database");
            if self.write_path.exists() {
                // Copy write database to query database
                fs::copy(&self.write_path, &self.query_path)
                    .context("copy write database to query database")?;
            } else {
                DuckDbStore::open(&self.query_path)?;
            }
        }

        Ok(())
    }

    /// Migrate from single database to dual-database mode
    fn migrate_from_single_db(&self, legacy_path: &Path) -> Result<()> {
        // Copy legacy database to both write and query databases
        fs::copy(legacy_path, &self.write_path)
            .context("copy legacy database to write database")?;
        fs::copy(legacy_path, &self.query_path)
            .context("copy legacy database to query database")?;

        // Rename legacy database to backup
        let backup_path = legacy_path.with_extension("duckdb.backup");
        fs::rename(legacy_path, &backup_path).context("rename legacy database to backup")?;

        info!(
            backup = %backup_path.display(),
            "legacy database backed up"
        );

        Ok(())
    }

    /// Get the write database path (for worker)
    pub fn write_path(&self) -> &Path {
        if self.config.enabled {
            &self.write_path
        } else {
            // Fallback to legacy single-database mode
            &self.write_path
        }
    }

    /// Get the query database path (for API)
    pub fn query_path(&self) -> &Path {
        if self.config.enabled {
            &self.query_path
        } else {
            // Fallback to legacy single-database mode
            &self.write_path
        }
    }

    /// Get metrics
    pub fn metrics(&self) -> Arc<DualDbMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Run synchronization cycle
    pub fn sync(&self) -> Result<SyncReport> {
        if !self.config.enabled {
            return Ok(SyncReport::default());
        }

        let start = Instant::now();
        let result = self.sync_inner();
        let duration = start.elapsed();

        match &result {
            Ok(report) => {
                self.metrics.record_sync(duration, report.rows_synced);
                info!(
                    rows_synced = report.rows_synced,
                    duration_ms = duration.as_millis(),
                    "dual-database sync completed"
                );
            }
            Err(err) => {
                self.metrics.record_error();
                error!(error = %err, "dual-database sync failed");
            }
        }

        result
    }

    fn sync_inner(&self) -> Result<SyncReport> {
        // Open write database (read-only for sync)
        let write_store = DuckDbStore::open_read_only(&self.write_path)
            .context("open write database for sync")?;

        // Get the last synced event_id
        let last_sync_id = self.last_sync_id.read().unwrap().clone();

        // Find new events in write database
        let new_events = if last_sync_id.is_empty() {
            // First sync: get the latest event_id from query database
            let query_store = DuckDbStore::open_read_only(&self.query_path)
                .context("open query database to find last event")?;
            let last_event_id = query_store
                .conn
                .query_row(
                    "SELECT event_id FROM events ORDER BY ingest_time DESC, event_id DESC LIMIT 1",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .ok();
            drop(query_store);

            if let Some(last_id) = last_event_id {
                *self.last_sync_id.write().unwrap() = last_id.clone();
                self.query_new_events(&write_store, &last_id)?
            } else {
                // Query database is empty, sync all events
                self.query_all_events(&write_store)?
            }
        } else {
            self.query_new_events(&write_store, &last_sync_id)?
        };

        if new_events.is_empty() {
            return Ok(SyncReport {
                rows_synced: 0,
                last_event_id: last_sync_id,
            });
        }

        // Update last_sync_id before writing
        let new_last_id = new_events
            .last()
            .map(|e| e.event_id.clone())
            .unwrap_or(last_sync_id.clone());

        // Write new events to query database
        let rows_synced = self.write_to_query_db(&new_events)?;

        // Update last_sync_id after successful write
        *self.last_sync_id.write().unwrap() = new_last_id.clone();

        Ok(SyncReport {
            rows_synced: rows_synced as u64,
            last_event_id: new_last_id,
        })
    }

    fn query_new_events(
        &self,
        write_store: &DuckDbStore,
        last_sync_id: &str,
    ) -> Result<Vec<fwlog_domain::CanonicalEvent>> {
        let mut stmt = write_store.conn.prepare(
            r#"
            SELECT event_id, ingest_time, source_addr, device_id, event_time, vendor, product,
                   src_ip, src_port, dst_ip, dst_port, protocol, action, severity,
                   raw, parse_status, parse_error
            FROM events
            WHERE event_id > ?
            ORDER BY ingest_time ASC, event_id ASC
            LIMIT 100000
            "#,
        )?;

        let rows = stmt.query_map([last_sync_id], |row| {
            let ingest_time: String = row.get(1)?;
            let event_time: Option<String> = row.get(4)?;
            let src_port: Option<i64> = row.get(8)?;
            let dst_port: Option<i64> = row.get(10)?;
            let parse_status: String = row.get(15)?;

            Ok(fwlog_domain::CanonicalEvent {
                event_id: row.get(0)?,
                ingest_time: chrono::DateTime::parse_from_rfc3339(&ingest_time)
                    .map(|v| v.with_timezone(&chrono::Utc))
                    .unwrap_or_else(|_| chrono::Utc::now()),
                source_addr: row.get(2)?,
                device_id: row.get(3)?,
                event_time: event_time.and_then(|value| {
                    chrono::DateTime::parse_from_rfc3339(&value)
                        .map(|v| v.with_timezone(&chrono::Utc))
                        .ok()
                }),
                vendor: row.get(5)?,
                product: row.get(6)?,
                src_ip: row.get(7)?,
                src_port: src_port.and_then(|v| u16::try_from(v).ok()),
                dst_ip: row.get(9)?,
                dst_port: dst_port.and_then(|v| u16::try_from(v).ok()),
                protocol: row.get(11)?,
                action: row.get(12)?,
                severity: row.get(13)?,
                raw: row.get(14)?,
                parse_status: parse_status_from_str(&parse_status),
                parse_error: row.get(16)?,
            })
        })?;

        rows.collect::<duckdb::Result<Vec<_>>>()
            .context("query new events from write database")
    }

    fn query_all_events(
        &self,
        write_store: &DuckDbStore,
    ) -> Result<Vec<fwlog_domain::CanonicalEvent>> {
        write_store
            .query_recent(100000)
            .context("query all events from write database")
    }

    fn write_to_query_db(&self, events: &[fwlog_domain::CanonicalEvent]) -> Result<usize> {
        if events.is_empty() {
            return Ok(0);
        }

        let mut query_store =
            DuckDbStore::open(&self.query_path).context("open query database for write")?;

        query_store
            .append_events(events)
            .context("append events to query database")
    }

    /// Run sync loop in background
    pub async fn run_sync_loop(self: Arc<Self>) {
        let interval = Duration::from_secs(self.config.sync_interval_seconds.max(60));

        loop {
            tokio::time::sleep(interval).await;

            if let Err(err) = self.sync() {
                warn!(error = %err, "dual-database sync cycle failed");
            }
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct SyncReport {
    pub rows_synced: u64,
    pub last_event_id: String,
}

fn parse_status_from_str(value: &str) -> fwlog_domain::ParseStatus {
    match value {
        "parsed" => fwlog_domain::ParseStatus::Parsed,
        "partial" => fwlog_domain::ParseStatus::Partial,
        _ => fwlog_domain::ParseStatus::Failed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use fwlog_domain::{CanonicalEvent, ParseStatus, RawLog};

    fn test_event(id: &str, ingest_offset_secs: i64) -> CanonicalEvent {
        let raw = RawLog {
            ingest_time: Utc::now() + chrono::Duration::seconds(ingest_offset_secs),
            source_addr: "tcp://127.0.0.1:1514".to_string(),
            raw: format!("raw {id}"),
        };
        let mut event = CanonicalEvent::failed(raw, "test");
        event.event_id = id.to_string();
        event.parse_status = ParseStatus::Parsed;
        event.src_ip = Some("192.168.1.1".to_string());
        event.dst_ip = Some("8.8.8.8".to_string());
        event
    }

    #[test]
    fn initializes_dual_databases_from_scratch() {
        let dir = tempfile::tempdir().unwrap();
        let base_path = dir.path().join("oxidelog.duckdb");

        let config = DualDbConfig {
            enabled: true,
            sync_interval_seconds: 300,
        };

        let manager = DualDbManager::new(&base_path, config).unwrap();

        assert!(manager.write_path().exists());
        assert!(manager.query_path().exists());
    }

    #[test]
    fn migrates_legacy_single_database_to_dual_mode() {
        let dir = tempfile::tempdir().unwrap();
        let legacy_path = dir.path().join("oxidelog.duckdb");

        // Create legacy database with some data
        let mut legacy_store = DuckDbStore::open(&legacy_path).unwrap();
        legacy_store
            .insert_batch(&[test_event("legacy-1", 0)])
            .unwrap();
        drop(legacy_store);

        let config = DualDbConfig {
            enabled: true,
            sync_interval_seconds: 300,
        };

        let manager = DualDbManager::new(&legacy_path, config).unwrap();

        // Legacy database should be backed up
        assert!(legacy_path.with_extension("duckdb.backup").exists());

        // Both write and query databases should exist with data
        let write_store = DuckDbStore::open_read_only(manager.write_path()).unwrap();
        let query_store = DuckDbStore::open_read_only(manager.query_path()).unwrap();

        let write_events = write_store.query_recent(10).unwrap();
        let query_events = query_store.query_recent(10).unwrap();

        assert_eq!(write_events.len(), 1);
        assert_eq!(query_events.len(), 1);
        assert_eq!(write_events[0].event_id, "legacy-1");
        assert_eq!(query_events[0].event_id, "legacy-1");
    }

    #[test]
    fn syncs_new_events_from_write_to_query() {
        let dir = tempfile::tempdir().unwrap();
        let base_path = dir.path().join("oxidelog.duckdb");

        let config = DualDbConfig {
            enabled: true,
            sync_interval_seconds: 300,
        };

        let manager = DualDbManager::new(&base_path, config).unwrap();

        // Write initial events to write database
        let mut write_store = DuckDbStore::open(manager.write_path()).unwrap();
        write_store
            .insert_batch(&[test_event("event-1", 0), test_event("event-2", 1)])
            .unwrap();
        drop(write_store);

        // First sync
        let report = manager.sync().unwrap();
        assert_eq!(report.rows_synced, 2);

        // Verify query database has the events
        let query_store = DuckDbStore::open_read_only(manager.query_path()).unwrap();
        let events = query_store.query_recent(10).unwrap();
        assert_eq!(events.len(), 2);

        // Write more events to write database
        let mut write_store = DuckDbStore::open(manager.write_path()).unwrap();
        write_store
            .insert_batch(&[test_event("event-3", 2)])
            .unwrap();
        drop(write_store);

        // Second sync should only sync new events
        let report = manager.sync().unwrap();
        assert_eq!(report.rows_synced, 1);

        // Verify query database has all events
        let query_store = DuckDbStore::open_read_only(manager.query_path()).unwrap();
        let events = query_store.query_recent(10).unwrap();
        assert_eq!(events.len(), 3);
    }

    #[test]
    fn disabled_mode_uses_single_database() {
        let dir = tempfile::tempdir().unwrap();
        let base_path = dir.path().join("oxidelog.duckdb");

        let config = DualDbConfig {
            enabled: false,
            sync_interval_seconds: 300,
        };

        let manager = DualDbManager::new(&base_path, config).unwrap();

        // In disabled mode, write_path and query_path should be the same
        assert_eq!(manager.write_path(), manager.query_path());
    }
}
