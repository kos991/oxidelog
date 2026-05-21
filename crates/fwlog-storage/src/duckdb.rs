use std::{fs, path::Path};

use anyhow::{bail, Context, Result};
use duckdb::{params, params_from_iter, AccessMode, Config, Connection};
use fwlog_domain::{CanonicalEvent, ParseStatus};

use crate::archive::ArchiveFile;

pub struct DuckDbStore {
    pub(crate) conn: Connection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MinuteMetricQuery {
    pub hours: u32,
    pub limit: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceMetricQuery {
    pub hours: u32,
    pub limit: usize,
}

impl Default for MinuteMetricQuery {
    fn default() -> Self {
        Self {
            hours: 24,
            limit: 1440,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct MinuteMetricPoint {
    pub bucket_minute: String,
    pub total: u64,
    pub parsed: u64,
    pub partial: u64,
    pub failed: u64,
    pub raw_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct HourMetricPoint {
    pub bucket_hour: String,
    pub total: u64,
    pub parsed: u64,
    pub partial: u64,
    pub failed: u64,
    pub raw_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ParseErrorSummary {
    pub reason: String,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SourceMetricBucket {
    pub source_addr: String,
    pub total: u64,
    pub parsed: u64,
    pub partial: u64,
    pub failed: u64,
    pub raw_bytes: u64,
    pub last_seen: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventStats {
    pub total: u64,
    pub parsed: u64,
    pub partial: u64,
    pub failed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct FrozenArchiveIndex {
    pub archive_path: String,
    pub day: String,
    pub source_addr: String,
    pub bytes: u64,
    pub line_count: u64,
    pub first_seen: Option<String>,
    pub last_seen: Option<String>,
    pub indexed_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct IpRegionCacheEntry {
    pub ip: String,
    pub region: Option<String>,
    pub country: Option<String>,
    pub province: Option<String>,
    pub city: Option<String>,
    pub isp: Option<String>,
    pub source: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ParserProfileRow {
    pub scope_key: String,
    pub parser_id: String,
    pub parser_name: String,
    pub success_count: i64,
    pub partial_count: i64,
    pub fail_count: i64,
    pub last_seen: String,
    pub priority_boost: f64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct AdaptiveFieldRuleRow {
    pub rule_id: String,
    pub scope_key: String,
    pub raw_key: String,
    pub canonical_field: String,
    pub value_type: String,
    pub status: String,
    pub confidence: f64,
    pub wins: i64,
    pub sample_count: i64,
    pub created_at: String,
    pub activated_at: Option<String>,
    pub disabled_at: Option<String>,
    pub disabled_reason: Option<String>,
    pub recovery_sample_rate: Option<f64>,
    pub recovery_attempts: Option<i64>,
    pub last_recovery_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ParserDiagnosticRow {
    pub fingerprint: String,
    pub scope_key: Option<String>,
    pub reason: String,
    pub sample_raw: Option<String>,
    pub sample_raw_truncated: bool,
    pub count: i64,
    pub suggested_rule_id: Option<String>,
    pub last_seen: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ParserScopeRow {
    pub scope_key: String,
    pub source_high_entropy: bool,
    pub adaptive_learning_enabled: bool,
    pub unknown_source_bucket: bool,
    pub metrics_gap: bool,
    pub metrics_gap_since: Option<String>,
    pub malformed_flood_until: Option<String>,
    pub shadow_rule_cooldown_until: Option<String>,
    pub adaptive_quarantine_until: Option<String>,
    pub quarantine_backoff_seconds: i64,
    pub quarantine_attempts: i64,
    pub last_state_change: String,
    pub last_seen: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct SourceDeviceAliasRow {
    pub source_key: String,
    pub raw_source_addr: String,
    pub device_id: String,
    pub first_seen: String,
    pub last_seen: String,
    pub confidence: f64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ParserCheckpointVersionRow {
    pub snapshot_version: i64,
    pub created_at: String,
    pub published_at: Option<String>,
    pub status: String,
    pub profiles_count: i64,
    pub rules_count: i64,
    pub diagnostics_count: i64,
    pub scope_state_count: i64,
    pub aliases_count: i64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ParserAdaptiveCheckpoint {
    pub snapshot_version: i64,
    pub created_at: String,
    pub published_at: Option<String>,
    pub profiles: Vec<ParserProfileCheckpointRow>,
    pub rules: Vec<AdaptiveFieldRuleCheckpointRow>,
    pub diagnostics: Vec<ParserDiagnosticCheckpointRow>,
    pub scopes: Vec<ParserScopeCheckpointRow>,
    pub aliases: Vec<SourceDeviceAliasCheckpointRow>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParserProfileCheckpointRow {
    pub scope_key: String,
    pub parser_id: String,
    pub parser_name: String,
    pub success_count: i64,
    pub partial_count: i64,
    pub fail_count: i64,
    pub last_seen: String,
    pub priority_boost: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AdaptiveFieldRuleCheckpointRow {
    pub rule_id: String,
    pub scope_key: String,
    pub raw_key: String,
    pub canonical_field: String,
    pub value_type: String,
    pub status: String,
    pub confidence: f64,
    pub wins: i64,
    pub sample_count: i64,
    pub created_at: String,
    pub activated_at: Option<String>,
    pub disabled_at: Option<String>,
    pub disabled_reason: Option<String>,
    pub recovery_sample_rate: Option<f64>,
    pub recovery_attempts: Option<i64>,
    pub last_recovery_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParserDiagnosticCheckpointRow {
    pub fingerprint: String,
    pub scope_key: Option<String>,
    pub reason: String,
    pub sample_raw: Option<String>,
    pub sample_raw_truncated: bool,
    pub count: i64,
    pub suggested_rule_id: Option<String>,
    pub last_seen: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParserScopeCheckpointRow {
    pub scope_key: String,
    pub source_high_entropy: bool,
    pub adaptive_learning_enabled: bool,
    pub unknown_source_bucket: bool,
    pub metrics_gap: bool,
    pub metrics_gap_since: Option<String>,
    pub malformed_flood_until: Option<String>,
    pub shadow_rule_cooldown_until: Option<String>,
    pub adaptive_quarantine_until: Option<String>,
    pub quarantine_backoff_seconds: i64,
    pub quarantine_attempts: i64,
    pub last_state_change: String,
    pub last_seen: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourceDeviceAliasCheckpointRow {
    pub source_key: String,
    pub raw_source_addr: String,
    pub device_id: String,
    pub first_seen: String,
    pub last_seen: String,
    pub confidence: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceBinding {
    pub id: String,
    pub protocol: String,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EventQuery {
    pub day: Option<String>,
    pub date_from: Option<String>,
    pub date_to: Option<String>,
    pub device: Option<String>,
    pub device_id: Option<String>,
    pub src_ip: Option<String>,
    pub dst_ip: Option<String>,
    pub protocol: Option<String>,
    pub action: Option<String>,
    pub keyword: Option<String>,
    pub include_failed: bool,
}

impl DuckDbStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_retry(path, false)
    }

    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_retry(path, true)
    }

    fn open_with_retry(path: impl AsRef<Path>, read_only: bool) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut last_err = None;
        for _ in 0..600 {
            match Self::open_with_mode(&path, read_only) {
                Ok(store) => return Ok(store),
                Err(err) => {
                    let message = format!("{err:#}");
                    if message.contains("already open")
                        || message.contains("正在使用此文件")
                        || message.contains("file is already open")
                    {
                        last_err = Some(err);
                        std::thread::sleep(std::time::Duration::from_millis(100));
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        let mode = if read_only { "read-only" } else { "read-write" };
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("open duckdb database {mode} failed")))
    }

    fn open_with_mode(path: impl AsRef<Path>, read_only: bool) -> Result<Self> {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent).context("create duckdb directory")?;
        }
        let conn = if read_only {
            Connection::open_with_flags(
                path.as_ref(),
                Config::default().access_mode(AccessMode::ReadOnly)?,
            )
            .context("open duckdb database read-only")?
        } else {
            Connection::open(path.as_ref()).context("open duckdb database")?
        };
        let store = Self { conn };
        if !read_only {
            store.init()?;
        }
        Ok(store)
    }

    fn init(&self) -> Result<()> {
        self.conn
            .execute_batch(&create_events_table_sql("events", true))?;
        self.migrate_events_schema()?;
        self.conn
            .execute_batch(&create_minute_metrics_table_sql("nat_minute_metrics", true))?;
        self.conn
            .execute_batch(&create_frozen_archive_index_table_sql(true))?;
        self.conn
            .execute_batch(&create_ip_region_cache_table_sql(true))?;
        self.conn
            .execute_batch(create_parser_adaptive_tables_sql())?;
        self.conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_events_ingest_time ON events(ingest_time);"
        )?;
        self.conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_events_src_ip ON events(src_ip);"
        )?;
        self.conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_events_dst_ip ON events(dst_ip);"
        )?;
        self.conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_events_protocol ON events(protocol);"
        )?;
        self.conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_events_action ON events(action);"
        )?;
        Ok(())
    }

    fn migrate_events_schema(&self) -> Result<()> {
        let has_source_addr: bool = self
            .conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM pragma_table_info('events') WHERE name = 'source_addr'",
                [],
                |row| row.get(0),
            )
            .context("inspect events schema")?;
        if !has_source_addr {
            self.conn.execute_batch(
                r#"
                ALTER TABLE events ADD COLUMN source_addr TEXT;
                UPDATE events SET source_addr = 'unknown://legacy' WHERE source_addr IS NULL OR source_addr = '';
                "#,
            )?;
        }
        let has_device_id: bool = self
            .conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM pragma_table_info('events') WHERE name = 'device_id'",
                [],
                |row| row.get(0),
            )
            .context("inspect events device_id schema")?;
        if !has_device_id {
            self.conn
                .execute_batch("ALTER TABLE events ADD COLUMN device_id TEXT;")?;
        }
        self.conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_events_ingest_time ON events(ingest_time);"
        )?;
        self.conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_events_src_ip ON events(src_ip);"
        )?;
        self.conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_events_dst_ip ON events(dst_ip);"
        )?;
        self.conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_events_protocol ON events(protocol);"
        )?;
        self.conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_events_action ON events(action);"
        )?;
        Ok(())
    }

    pub fn insert_batch(&mut self, events: &[CanonicalEvent]) -> Result<usize> {
        if events.is_empty() {
            return Ok(0);
        }
        self.conn.execute_batch(
            r#"
            DROP TABLE IF EXISTS import_events;
            CREATE TEMP TABLE import_events AS SELECT * FROM events LIMIT 0;
            "#,
        )?;
        {
            let mut app = self
                .conn
                .appender("import_events")
                .context("create appender")?;
            for event in events {
                app.append_row(params![
                    event.event_id.as_str(),
                    event.ingest_time.to_rfc3339(),
                    event.source_addr.as_str(),
                    event.device_id.as_deref(),
                    event.event_time.as_ref().map(|v| v.to_rfc3339()),
                    event.vendor.as_deref(),
                    event.product.as_deref(),
                    event.src_ip.as_deref(),
                    event.src_port.map(i64::from),
                    event.dst_ip.as_deref(),
                    event.dst_port.map(i64::from),
                    event.protocol.as_deref(),
                    event.action.as_deref(),
                    event.severity.as_deref(),
                    event.raw.as_str(),
                    status_str(event.parse_status),
                    event.parse_error.as_deref(),
                ])?;
            }
        }
        let inserted = self
            .conn
            .execute(
                "INSERT OR IGNORE INTO events SELECT * FROM import_events",
                [],
            )
            .context("insert from temp table")?;
        self.conn.execute("DELETE FROM import_events", []).ok();
        self.refresh_minute_metrics_for_events(events)?;
        Ok(inserted)
    }

    fn checkpoint(&self) -> Result<()> {
        self.conn.execute_batch("CHECKPOINT")?;
        Ok(())
    }
    pub fn import_events_csv(&self, path: impl AsRef<Path>) -> Result<usize> {
        let path = path.as_ref();
        let sql_path = path.to_string_lossy().replace('\'', "''");
        let sql = format!(
            r#"
            CREATE TEMP TABLE import_events AS SELECT * FROM events LIMIT 0;
            COPY import_events FROM '{}' (HEADER, AUTO_DETECT TRUE);
            INSERT OR IGNORE INTO events SELECT * FROM import_events;
            DROP TABLE import_events;
            "#,
            sql_path
        );
        self.conn
            .execute_batch(&sql)
            .with_context(|| format!("import events csv {}", path.display()))?;
        self.rebuild_minute_metrics()?;
        Ok(0)
    }

    pub fn append_events(&self, events: &[CanonicalEvent]) -> Result<usize> {
        if events.is_empty() {
            return Ok(0);
        }

        self.conn.execute_batch(
            r#"
            DROP TABLE IF EXISTS import_events;
            CREATE TEMP TABLE import_events AS SELECT * FROM events LIMIT 0;
            "#,
        )?;
        {
            let mut app = self
                .conn
                .appender("import_events")
                .context("create appender")?;
            for event in events {
                app.append_row(params![
                    event.event_id.as_str(),
                    event.ingest_time.to_rfc3339(),
                    event.source_addr.as_str(),
                    event.device_id.as_deref(),
                    event.event_time.as_ref().map(|v| v.to_rfc3339()),
                    event.vendor.as_deref(),
                    event.product.as_deref(),
                    event.src_ip.as_deref(),
                    event.src_port.map(i64::from),
                    event.dst_ip.as_deref(),
                    event.dst_port.map(i64::from),
                    event.protocol.as_deref(),
                    event.action.as_deref(),
                    event.severity.as_deref(),
                    event.raw.as_str(),
                    status_str(event.parse_status),
                    event.parse_error.as_deref(),
                ])?;
            }
        }

        self.conn.execute_batch(
            r#"
            INSERT OR IGNORE INTO events SELECT * FROM import_events;
            DELETE FROM import_events;
            "#,
        )?;
        self.refresh_minute_metrics_for_events(events)?;

        Ok(events.len())
    }

    pub fn replace_all_events(
        &self,
        events: &[CanonicalEvent],
        expected_source_rows: u64,
    ) -> Result<usize> {
        let empty_raw_rows = self.empty_raw_rows()?;
        if empty_raw_rows > 0 {
            bail!(
                "refuse to reparse: {} events have empty raw payloads",
                empty_raw_rows
            );
        }

        self.conn.execute_batch(
            r#"
            DROP TABLE IF EXISTS events_reparse;
            "#,
        )?;
        self.conn
            .execute_batch(&create_events_table_sql("events_reparse", false))?;
        {
            let mut app = self
                .conn
                .appender("events_reparse")
                .context("create reparse appender")?;
            for event in events {
                app.append_row(params![
                    event.event_id.as_str(),
                    event.ingest_time.to_rfc3339(),
                    event.source_addr.as_str(),
                    event.device_id.as_deref(),
                    event.event_time.as_ref().map(|v| v.to_rfc3339()),
                    event.vendor.as_deref(),
                    event.product.as_deref(),
                    event.src_ip.as_deref(),
                    event.src_port.map(i64::from),
                    event.dst_ip.as_deref(),
                    event.dst_port.map(i64::from),
                    event.protocol.as_deref(),
                    event.action.as_deref(),
                    event.severity.as_deref(),
                    event.raw.as_str(),
                    status_str(event.parse_status),
                    event.parse_error.as_deref(),
                ])?;
            }
        }
        let current_rows = self.event_stats()?.total;
        if current_rows != expected_source_rows {
            self.conn
                .execute_batch("DROP TABLE IF EXISTS events_reparse;")?;
            bail!(
                "refuse to replace events: source row count changed from {} to {}",
                expected_source_rows,
                current_rows
            );
        }
        self.conn.execute_batch(
            r#"
            BEGIN TRANSACTION;
            DROP TABLE events;
            ALTER TABLE events_reparse RENAME TO events;
            COMMIT;
            "#,
        )?;
        self.rebuild_minute_metrics()?;
        Ok(events.len())
    }

    pub fn empty_raw_rows(&self) -> Result<u64> {
        let count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM events WHERE raw = ''", [], |row| {
                row.get(0)
            })
            .context("count empty raw events")?;
        Ok(count.max(0) as u64)
    }

    pub fn prune_parsed_raw(&self) -> Result<usize> {
        let changed = self
            .conn
            .execute(
                "UPDATE events SET raw = '' WHERE parse_status = 'parsed' AND raw <> ''",
                [],
            )
            .context("prune parsed event raw payloads")?;
        Ok(changed)
    }

    pub fn compact_to(
        &self,
        output_path: impl AsRef<Path>,
        drop_parsed_raw: bool,
    ) -> Result<usize> {
        self.compact_selected_to(output_path, drop_parsed_raw, None, false, None)
    }

    pub fn compact_hot_to(
        &self,
        output_path: impl AsRef<Path>,
        hot_limit: usize,
        drop_parsed_raw: bool,
    ) -> Result<usize> {
        self.compact_selected_to(output_path, drop_parsed_raw, Some(hot_limit), true, None)
    }

    pub fn compact_limit_to(
        &self,
        output_path: impl AsRef<Path>,
        limit: usize,
        drop_parsed_raw: bool,
    ) -> Result<usize> {
        self.compact_selected_to(output_path, drop_parsed_raw, Some(limit), false, None)
    }

    pub fn compact_time_range_to(
        &self,
        output_path: impl AsRef<Path>,
        days: u32,
        drop_parsed_raw: bool,
    ) -> Result<usize> {
        self.compact_selected_to(output_path, drop_parsed_raw, None, false, Some(days))
    }

    fn compact_selected_to(
        &self,
        output_path: impl AsRef<Path>,
        drop_parsed_raw: bool,
        hot_limit: Option<usize>,
        order_by_newest: bool,
        days: Option<u32>,
    ) -> Result<usize> {
        let output_path = output_path.as_ref();
        if output_path.exists() {
            bail!("compact output already exists: {}", output_path.display());
        }
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent).context("create compact output directory")?;
        }

        let sql_path = output_path.to_string_lossy().replace('\'', "''");
        let raw_expr = if drop_parsed_raw {
            "CASE WHEN parse_status = 'parsed' THEN '' ELSE raw END"
        } else {
            "raw"
        };

        let where_clause = if let Some(days) = days {
            let cutoff = chrono::Utc::now() - chrono::Duration::days(days as i64);
            format!("WHERE ingest_time >= '{}'", cutoff.to_rfc3339())
        } else {
            String::new()
        };

        let limit_clause = hot_limit
            .map(|limit| format!("LIMIT {}", limit))
            .unwrap_or_default();
        let order_clause = if order_by_newest {
            "ORDER BY ingest_time DESC, event_id DESC"
        } else {
            ""
        };
        let sql = format!(
            r#"
            ATTACH '{}' AS compact;
            {}
            INSERT INTO compact.events (
              event_id, ingest_time, source_addr, device_id, event_time, vendor, product,
              src_ip, src_port, dst_ip, dst_port, protocol, action, severity,
              raw, parse_status, parse_error
            )
            SELECT
              event_id, ingest_time, source_addr, device_id, event_time, vendor, product,
              src_ip, src_port, dst_ip, dst_port, protocol, action, severity,
              {}, parse_status, parse_error
            FROM events
            {}
            {}
            {};
            DETACH compact;
            "#,
            sql_path,
            create_events_table_sql("compact.events", false),
            raw_expr,
            where_clause,
            order_clause,
            limit_clause
        );
        self.conn.execute_batch(&sql).context("compact duckdb")?;

        let compact = Connection::open(output_path).context("open compacted duckdb")?;
        let count: i64 = compact
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .context("count compacted events")?;
        Ok(count as usize)
    }

    pub fn query_recent(&self, limit: usize) -> Result<Vec<CanonicalEvent>> {
        self.query_recent_with_raw(limit, true)
    }

    pub fn query_recent_without_raw(&self, limit: usize) -> Result<Vec<CanonicalEvent>> {
        self.query_recent_with_raw(limit, false)
    }

    fn query_recent_with_raw(
        &self,
        limit: usize,
        include_raw: bool,
    ) -> Result<Vec<CanonicalEvent>> {
        let raw_expr = if include_raw { "raw" } else { "'' AS raw" };
        let mut stmt = self.conn.prepare(&format!(
            r#"
            SELECT event_id, ingest_time, source_addr, device_id, event_time, vendor, product,
                   src_ip, src_port, dst_ip, dst_port, protocol, action, severity,
                   {raw_expr}, parse_status, parse_error
            FROM events
            ORDER BY ingest_time DESC
            LIMIT ?
            "#
        ))?;
        let rows = stmt.query_map([limit as i64], row_to_event)?;
        rows.collect::<duckdb::Result<Vec<_>>>()
            .context("query duckdb events")
    }

    pub fn upsert_frozen_archive_index(
        &self,
        archive_path: &str,
        day: &str,
        source_addr: &str,
        bytes: u64,
        line_count: u64,
    ) -> Result<()> {
        self.upsert_frozen_archive_index_with_times(
            archive_path,
            day,
            source_addr,
            bytes,
            line_count,
            None,
            None,
        )
    }

    pub fn upsert_frozen_archive_index_with_times(
        &self,
        archive_path: &str,
        day: &str,
        source_addr: &str,
        bytes: u64,
        line_count: u64,
        first_seen: Option<&str>,
        last_seen: Option<&str>,
    ) -> Result<()> {
        self.conn
            .execute(
                "DELETE FROM frozen_archive_index WHERE archive_path = ?",
                params![archive_path],
            )
            .context("replace frozen archive index")?;
        self.conn
            .execute(
                r#"
                INSERT INTO frozen_archive_index (
                  archive_path, day, source_addr, bytes, line_count, first_seen, last_seen, indexed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                "#,
                params![
                    archive_path,
                    day,
                    source_addr,
                    bytes as i64,
                    line_count as i64,
                    first_seen,
                    last_seen,
                    chrono::Utc::now().to_rfc3339(),
                ],
            )
            .context("upsert frozen archive index")?;
        Ok(())
    }

    pub fn find_frozen_archives(
        &self,
        day: &str,
        source_addr: Option<&str>,
    ) -> Result<Vec<String>> {
        let normalized_day = normalize_day(day).unwrap_or_else(|| day.to_string());
        let mut values = vec![normalized_day];
        let source_clause = if let Some(source_addr) = source_addr.filter(|value| !value.is_empty())
        {
            values.push(source_addr.to_string());
            " AND source_addr = ?"
        } else {
            ""
        };
        let sql = format!(
            r#"
            SELECT archive_path
            FROM frozen_archive_index
            WHERE day = ?{source_clause}
            ORDER BY archive_path ASC
            "#
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(values.iter()), |row| row.get(0))?;
        rows.collect::<duckdb::Result<Vec<_>>>()
            .context("find frozen archives")
    }

    pub fn list_frozen_archive_index(
        &self,
        day: Option<&str>,
        limit: usize,
    ) -> Result<Vec<FrozenArchiveIndex>> {
        let mut values = Vec::new();
        let where_clause = if let Some(day) = day.and_then(normalize_day) {
            values.push(day);
            "WHERE day = ?"
        } else {
            ""
        };
        let sql = format!(
            r#"
            SELECT archive_path, day, source_addr, bytes, line_count, first_seen, last_seen, indexed_at
            FROM frozen_archive_index
            {where_clause}
            ORDER BY day DESC, archive_path ASC
            LIMIT ?
            "#
        );
        values.push(limit.clamp(1, 10_000).to_string());
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(values.iter()), |row| {
            Ok(FrozenArchiveIndex {
                archive_path: row.get(0)?,
                day: row.get(1)?,
                source_addr: row.get(2)?,
                bytes: row.get::<_, i64>(3)?.max(0) as u64,
                line_count: row.get::<_, i64>(4)?.max(0) as u64,
                first_seen: row.get(5)?,
                last_seen: row.get(6)?,
                indexed_at: row.get(7)?,
            })
        })?;
        rows.collect::<duckdb::Result<Vec<_>>>()
            .context("list frozen archive index")
    }

    pub fn get_ip_region_cache(&self, ip: &str) -> Result<Option<IpRegionCacheEntry>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT ip, region, country, province, city, isp, source, updated_at
            FROM ip_region_cache
            WHERE ip = ?
            "#,
        )?;
        let mut rows = stmt.query_map([ip], |row| {
            Ok(IpRegionCacheEntry {
                ip: row.get(0)?,
                region: row.get(1)?,
                country: row.get(2)?,
                province: row.get(3)?,
                city: row.get(4)?,
                isp: row.get(5)?,
                source: row.get(6)?,
                updated_at: row.get(7)?,
            })
        })?;
        rows.next().transpose().context("get ip region cache")
    }

    pub fn upsert_ip_region_cache(&self, entry: &IpRegionCacheEntry) -> Result<()> {
        self.conn
            .execute(
                "DELETE FROM ip_region_cache WHERE ip = ?",
                params![entry.ip.as_str()],
            )
            .context("replace ip region cache")?;
        self.conn
            .execute(
                r#"
                INSERT INTO ip_region_cache (
                  ip, region, country, province, city, isp, source, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                "#,
                params![
                    entry.ip.as_str(),
                    entry.region.as_deref(),
                    entry.country.as_deref(),
                    entry.province.as_deref(),
                    entry.city.as_deref(),
                    entry.isp.as_deref(),
                    entry.source.as_str(),
                    entry.updated_at.as_str(),
                ],
            )
            .context("upsert ip region cache")?;
        Ok(())
    }

    pub fn backfill_device_ids(&self, bindings: &[DeviceBinding]) -> Result<usize> {
        let mut changed = 0_usize;
        for binding in bindings {
            let source_addr = format!(
                "{}://{}:{}",
                binding.protocol.to_ascii_lowercase(),
                binding.host,
                binding.port
            );
            changed += self
                .conn
                .execute(
                    "UPDATE events SET device_id = ? WHERE source_addr = ?",
                    params![binding.id.as_str(), source_addr],
                )
                .with_context(|| format!("backfill device id {}", binding.id))?;
        }
        Ok(changed)
    }

    pub fn event_stats(&self) -> Result<EventStats> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN parse_status = 'parsed' THEN 1 ELSE 0 END) AS parsed,
              SUM(CASE WHEN parse_status = 'partial' THEN 1 ELSE 0 END) AS partial,
              SUM(CASE WHEN parse_status = 'failed' THEN 1 ELSE 0 END) AS failed
            FROM events
            "#,
        )?;
        let stats = stmt.query_row([], |row| {
            Ok(EventStats {
                total: row.get::<_, i64>(0)?.max(0) as u64,
                parsed: row.get::<_, Option<i64>>(1)?.unwrap_or(0).max(0) as u64,
                partial: row.get::<_, Option<i64>>(2)?.unwrap_or(0).max(0) as u64,
                failed: row.get::<_, Option<i64>>(3)?.unwrap_or(0).max(0) as u64,
            })
        })?;
        Ok(stats)
    }

    pub fn query_minute_metrics(
        &self,
        query: &MinuteMetricQuery,
    ) -> Result<Vec<MinuteMetricPoint>> {
        let hours = i64::from(query.hours.clamp(1, 24 * 366));
        let limit = query.limit.clamp(1, 24 * 366 * 60);
        let cutoff = metric_cutoff(hours);
        let mut stmt = self.conn.prepare(
            r#"
            SELECT *
            FROM (
              SELECT bucket_minute, total, parsed, partial, failed, raw_bytes
              FROM (
                SELECT bucket_minute,
                       SUM(total_count) AS total,
                       SUM(CASE WHEN parse_status = 'parsed' THEN total_count ELSE 0 END) AS parsed,
                       SUM(CASE WHEN parse_status = 'partial' THEN total_count ELSE 0 END) AS partial,
                       SUM(CASE WHEN parse_status = 'failed' THEN total_count ELSE 0 END) AS failed,
                       SUM(raw_bytes) AS raw_bytes
                FROM nat_minute_metrics
                WHERE bucket_minute >= ?
                GROUP BY bucket_minute
              )
              ORDER BY bucket_minute DESC
              LIMIT ?
            )
            ORDER BY bucket_minute ASC
            "#,
        )?;
        let rows = stmt.query_map(params![cutoff, limit as i64], |row| {
            Ok(MinuteMetricPoint {
                bucket_minute: row.get(0)?,
                total: row.get::<_, Option<i64>>(1)?.unwrap_or(0).max(0) as u64,
                parsed: row.get::<_, Option<i64>>(2)?.unwrap_or(0).max(0) as u64,
                partial: row.get::<_, Option<i64>>(3)?.unwrap_or(0).max(0) as u64,
                failed: row.get::<_, Option<i64>>(4)?.unwrap_or(0).max(0) as u64,
                raw_bytes: row.get::<_, Option<i64>>(5)?.unwrap_or(0).max(0) as u64,
            })
        })?;
        rows.collect::<duckdb::Result<Vec<_>>>()
            .context("query minute metrics")
    }

    pub fn query_source_metrics(
        &self,
        query: &SourceMetricQuery,
    ) -> Result<Vec<SourceMetricBucket>> {
        let hours = i64::from(query.hours.clamp(1, 24 * 366));
        let limit = query.limit.clamp(1, 24 * 366 * 60);
        let cutoff = metric_cutoff(hours);
        let mut stmt = self.conn.prepare(
            r#"
            SELECT source_addr,
                   SUM(total_count) AS total,
                   SUM(CASE WHEN parse_status = 'parsed' THEN total_count ELSE 0 END) AS parsed,
                   SUM(CASE WHEN parse_status = 'partial' THEN total_count ELSE 0 END) AS partial,
                   SUM(CASE WHEN parse_status = 'failed' THEN total_count ELSE 0 END) AS failed,
                   SUM(raw_bytes) AS raw_bytes,
                   MAX(bucket_minute) AS last_seen
            FROM nat_minute_metrics
            WHERE bucket_minute >= ?
            GROUP BY source_addr
            ORDER BY total DESC, source_addr ASC
            LIMIT ?
            "#,
        )?;
        let rows = stmt.query_map(params![cutoff, limit as i64], |row| {
            Ok(SourceMetricBucket {
                source_addr: row.get(0)?,
                total: row.get::<_, Option<i64>>(1)?.unwrap_or(0).max(0) as u64,
                parsed: row.get::<_, Option<i64>>(2)?.unwrap_or(0).max(0) as u64,
                partial: row.get::<_, Option<i64>>(3)?.unwrap_or(0).max(0) as u64,
                failed: row.get::<_, Option<i64>>(4)?.unwrap_or(0).max(0) as u64,
                raw_bytes: row.get::<_, Option<i64>>(5)?.unwrap_or(0).max(0) as u64,
                last_seen: row.get(6)?,
            })
        })?;
        rows.collect::<duckdb::Result<Vec<_>>>()
            .context("query source metrics")
    }

    pub fn list_parser_profiles(&self) -> Result<Vec<ParserProfileRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT scope_key, parser_id, parser_name, success_count, partial_count, fail_count,
                   CAST(last_seen AS TEXT), priority_boost
            FROM parser_profiles
            ORDER BY last_seen DESC, scope_key, parser_id
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(ParserProfileRow {
                scope_key: row.get(0)?,
                parser_id: row.get(1)?,
                parser_name: row.get(2)?,
                success_count: row.get(3)?,
                partial_count: row.get(4)?,
                fail_count: row.get(5)?,
                last_seen: row.get(6)?,
                priority_boost: row.get(7)?,
            })
        })?;
        rows.collect::<duckdb::Result<Vec<_>>>()
            .context("list parser profiles")
    }

    pub fn list_adaptive_field_rules(&self) -> Result<Vec<AdaptiveFieldRuleRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT rule_id, scope_key, raw_key, canonical_field, value_type, status,
                   confidence, wins, sample_count, CAST(created_at AS TEXT),
                   CAST(activated_at AS TEXT), CAST(disabled_at AS TEXT), disabled_reason,
                   recovery_sample_rate, recovery_attempts, CAST(last_recovery_at AS TEXT)
            FROM adaptive_field_rules
            ORDER BY scope_key, raw_key, canonical_field
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(AdaptiveFieldRuleRow {
                rule_id: row.get(0)?,
                scope_key: row.get(1)?,
                raw_key: row.get(2)?,
                canonical_field: row.get(3)?,
                value_type: row.get(4)?,
                status: row.get(5)?,
                confidence: row.get(6)?,
                wins: row.get(7)?,
                sample_count: row.get(8)?,
                created_at: row.get(9)?,
                activated_at: row.get(10)?,
                disabled_at: row.get(11)?,
                disabled_reason: row.get(12)?,
                recovery_sample_rate: row.get(13)?,
                recovery_attempts: row.get(14)?,
                last_recovery_at: row.get(15)?,
            })
        })?;
        rows.collect::<duckdb::Result<Vec<_>>>()
            .context("list adaptive field rules")
    }

    pub fn list_parser_diagnostics(&self) -> Result<Vec<ParserDiagnosticRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT fingerprint, scope_key, reason, sample_raw, sample_raw_truncated,
                   count, suggested_rule_id, CAST(last_seen AS TEXT)
            FROM parser_diagnostics
            ORDER BY last_seen DESC, fingerprint
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(ParserDiagnosticRow {
                fingerprint: row.get(0)?,
                scope_key: row.get(1)?,
                reason: row.get(2)?,
                sample_raw: row.get(3)?,
                sample_raw_truncated: row.get(4)?,
                count: row.get(5)?,
                suggested_rule_id: row.get(6)?,
                last_seen: row.get(7)?,
            })
        })?;
        rows.collect::<duckdb::Result<Vec<_>>>()
            .context("list parser diagnostics")
    }

    pub fn list_parser_scopes(&self) -> Result<Vec<ParserScopeRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT scope_key, source_high_entropy, adaptive_learning_enabled,
                   unknown_source_bucket, metrics_gap, CAST(metrics_gap_since AS TEXT),
                   CAST(malformed_flood_until AS TEXT), CAST(shadow_rule_cooldown_until AS TEXT),
                   CAST(adaptive_quarantine_until AS TEXT), quarantine_backoff_seconds,
                   quarantine_attempts, CAST(last_state_change AS TEXT), CAST(last_seen AS TEXT)
            FROM parser_scope_state
            ORDER BY last_seen DESC, scope_key
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(ParserScopeRow {
                scope_key: row.get(0)?,
                source_high_entropy: row.get(1)?,
                adaptive_learning_enabled: row.get(2)?,
                unknown_source_bucket: row.get(3)?,
                metrics_gap: row.get(4)?,
                metrics_gap_since: row.get(5)?,
                malformed_flood_until: row.get(6)?,
                shadow_rule_cooldown_until: row.get(7)?,
                adaptive_quarantine_until: row.get(8)?,
                quarantine_backoff_seconds: row.get(9)?,
                quarantine_attempts: row.get(10)?,
                last_state_change: row.get(11)?,
                last_seen: row.get(12)?,
            })
        })?;
        rows.collect::<duckdb::Result<Vec<_>>>()
            .context("list parser scopes")
    }

    pub fn list_source_device_aliases(&self) -> Result<Vec<SourceDeviceAliasRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT source_key, raw_source_addr, device_id, CAST(first_seen AS TEXT),
                   CAST(last_seen AS TEXT), confidence
            FROM source_device_aliases
            ORDER BY last_seen DESC, source_key, raw_source_addr, device_id
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(SourceDeviceAliasRow {
                source_key: row.get(0)?,
                raw_source_addr: row.get(1)?,
                device_id: row.get(2)?,
                first_seen: row.get(3)?,
                last_seen: row.get(4)?,
                confidence: row.get(5)?,
            })
        })?;
        rows.collect::<duckdb::Result<Vec<_>>>()
            .context("list source device aliases")
    }

    pub fn list_parser_checkpoint_versions(&self) -> Result<Vec<ParserCheckpointVersionRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT snapshot_version, CAST(created_at AS TEXT), CAST(published_at AS TEXT),
                   status, profiles_count, rules_count, diagnostics_count,
                   scope_state_count, aliases_count
            FROM parser_checkpoint_version
            ORDER BY snapshot_version DESC
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(ParserCheckpointVersionRow {
                snapshot_version: row.get(0)?,
                created_at: row.get(1)?,
                published_at: row.get(2)?,
                status: row.get(3)?,
                profiles_count: row.get(4)?,
                rules_count: row.get(5)?,
                diagnostics_count: row.get(6)?,
                scope_state_count: row.get(7)?,
                aliases_count: row.get(8)?,
            })
        })?;
        rows.collect::<duckdb::Result<Vec<_>>>()
            .context("list parser checkpoint versions")
    }

    pub fn checkpoint_parser_adaptive_state(
        &self,
        checkpoint: &ParserAdaptiveCheckpoint,
    ) -> Result<()> {
        self.conn
            .execute_batch(
                r#"
                BEGIN TRANSACTION;
                DELETE FROM parser_profiles;
                DELETE FROM adaptive_field_rules;
                DELETE FROM parser_scope_state;
                DELETE FROM parser_diagnostics;
                DELETE FROM source_device_aliases;
                "#,
            )
            .context("begin parser adaptive checkpoint")?;

        let result = (|| -> Result<()> {
            for profile in &checkpoint.profiles {
                self.conn.execute(
                    r#"
                    INSERT INTO parser_profiles (
                      scope_key, parser_id, parser_name, success_count, partial_count,
                      fail_count, last_seen, priority_boost
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    "#,
                    params![
                        profile.scope_key.as_str(),
                        profile.parser_id.as_str(),
                        profile.parser_name.as_str(),
                        profile.success_count,
                        profile.partial_count,
                        profile.fail_count,
                        profile.last_seen.as_str(),
                        profile.priority_boost,
                    ],
                )?;
            }

            for rule in &checkpoint.rules {
                self.conn.execute(
                    r#"
                    INSERT INTO adaptive_field_rules (
                      rule_id, scope_key, raw_key, canonical_field, value_type, status,
                      confidence, wins, sample_count, created_at, activated_at,
                      disabled_at, disabled_reason, recovery_sample_rate, recovery_attempts,
                      last_recovery_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    "#,
                    params![
                        rule.rule_id.as_str(),
                        rule.scope_key.as_str(),
                        rule.raw_key.as_str(),
                        rule.canonical_field.as_str(),
                        rule.value_type.as_str(),
                        rule.status.as_str(),
                        rule.confidence,
                        rule.wins,
                        rule.sample_count,
                        rule.created_at.as_str(),
                        rule.activated_at.as_deref(),
                        rule.disabled_at.as_deref(),
                        rule.disabled_reason.as_deref(),
                        rule.recovery_sample_rate,
                        rule.recovery_attempts,
                        rule.last_recovery_at.as_deref(),
                    ],
                )?;
            }

            for scope in &checkpoint.scopes {
                self.conn.execute(
                    r#"
                    INSERT INTO parser_scope_state (
                      scope_key, source_high_entropy, adaptive_learning_enabled,
                      unknown_source_bucket, metrics_gap, metrics_gap_since,
                      malformed_flood_until, shadow_rule_cooldown_until,
                      adaptive_quarantine_until, quarantine_backoff_seconds,
                      quarantine_attempts, last_state_change, last_seen
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    "#,
                    params![
                        scope.scope_key.as_str(),
                        scope.source_high_entropy,
                        scope.adaptive_learning_enabled,
                        scope.unknown_source_bucket,
                        scope.metrics_gap,
                        scope.metrics_gap_since.as_deref(),
                        scope.malformed_flood_until.as_deref(),
                        scope.shadow_rule_cooldown_until.as_deref(),
                        scope.adaptive_quarantine_until.as_deref(),
                        scope.quarantine_backoff_seconds,
                        scope.quarantine_attempts,
                        scope.last_state_change.as_str(),
                        scope.last_seen.as_str(),
                    ],
                )?;
            }

            for diagnostic in &checkpoint.diagnostics {
                self.conn.execute(
                    r#"
                    INSERT INTO parser_diagnostics (
                      fingerprint, scope_key, reason, sample_raw, sample_raw_truncated,
                      count, suggested_rule_id, last_seen
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    "#,
                    params![
                        diagnostic.fingerprint.as_str(),
                        diagnostic.scope_key.as_deref(),
                        diagnostic.reason.as_str(),
                        diagnostic.sample_raw.as_deref(),
                        diagnostic.sample_raw_truncated,
                        diagnostic.count,
                        diagnostic.suggested_rule_id.as_deref(),
                        diagnostic.last_seen.as_str(),
                    ],
                )?;
            }

            for alias in &checkpoint.aliases {
                self.conn.execute(
                    r#"
                    INSERT INTO source_device_aliases (
                      source_key, raw_source_addr, device_id, first_seen, last_seen, confidence
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    "#,
                    params![
                        alias.source_key.as_str(),
                        alias.raw_source_addr.as_str(),
                        alias.device_id.as_str(),
                        alias.first_seen.as_str(),
                        alias.last_seen.as_str(),
                        alias.confidence,
                    ],
                )?;
            }

            self.conn.execute(
                r#"
                INSERT INTO parser_checkpoint_version (
                  snapshot_version, created_at, published_at, status, profiles_count,
                  rules_count, diagnostics_count, scope_state_count, aliases_count
                ) VALUES (?, ?, ?, 'published', ?, ?, ?, ?, ?)
                "#,
                params![
                    checkpoint.snapshot_version,
                    checkpoint.created_at.as_str(),
                    checkpoint.published_at.as_deref(),
                    checkpoint.profiles.len() as i64,
                    checkpoint.rules.len() as i64,
                    checkpoint.diagnostics.len() as i64,
                    checkpoint.scopes.len() as i64,
                    checkpoint.aliases.len() as i64,
                ],
            )?;

            Ok(())
        })();

        if let Err(err) = result {
            let _ = self.conn.execute_batch("ROLLBACK;");
            return Err(err).context("write parser adaptive checkpoint");
        }

        self.conn
            .execute_batch("COMMIT;")
            .context("commit parser adaptive checkpoint")
    }

    fn rebuild_minute_metrics(&self) -> Result<()> {
        self.conn
            .execute_batch("DELETE FROM nat_minute_metrics;")
            .context("clear minute metrics")?;
        self.insert_minute_metrics_from_events(None)
    }

    fn refresh_minute_metrics_for_events(&self, events: &[CanonicalEvent]) -> Result<()> {
        let buckets = minute_buckets(events);
        if buckets.is_empty() {
            return Ok(());
        }
        let placeholders = std::iter::repeat("?")
            .take(buckets.len())
            .collect::<Vec<_>>()
            .join(", ");
        let delete_sql =
            format!("DELETE FROM nat_minute_metrics WHERE bucket_minute IN ({placeholders})");
        self.conn
            .execute(&delete_sql, params_from_iter(buckets.iter()))
            .context("delete stale minute metrics")?;
        self.insert_minute_metrics_from_events(Some(&buckets))
    }

    fn insert_minute_metrics_from_events(&self, buckets: Option<&[String]>) -> Result<()> {
        let (where_clause, values): (String, Vec<String>) = match buckets {
            Some(buckets) if !buckets.is_empty() => {
                let min = buckets.iter().min().unwrap().clone();
                let max = buckets.iter().max().unwrap();
                let max_dt = chrono::NaiveDateTime::parse_from_str(max, "%Y-%m-%dT%H:%M:%SZ")
                    .with_context(|| format!("parse max bucket time: {max}"))?;
                let max_exclusive = (max_dt + chrono::Duration::minutes(1))
                    .format("%Y-%m-%dT%H:%M:%SZ")
                    .to_string();
                (
                    "WHERE ingest_time >= ? AND ingest_time < ?".to_string(),
                    vec![min, max_exclusive],
                )
            }
            _ => (String::new(), Vec::new()),
        };
        let raw_bytes_expr = if buckets.is_some() {
            "SUM(length(raw))"
        } else {
            "0"
        };
        let sql = format!(
            r#"
            INSERT INTO nat_minute_metrics (
              bucket_minute, source_addr, protocol, action, parse_status, total_count, raw_bytes
            )
            SELECT
              concat(substr(ingest_time, 1, 16), ':00Z') AS bucket_minute,
              COALESCE(NULLIF(source_addr, ''), 'unknown') AS source_addr,
              COALESCE(NULLIF(upper(protocol), ''), 'UNKNOWN') AS protocol,
              COALESCE(NULLIF(lower(action), ''), 'unknown') AS action,
              parse_status,
              COUNT(*) AS total_count,
              {raw_bytes_expr} AS raw_bytes
            FROM events
            {where_clause}
            GROUP BY bucket_minute, source_addr, protocol, action, parse_status
            "#
        );
        if values.is_empty() {
            self.conn
                .execute_batch(&sql)
                .context("insert all minute metrics")?;
        } else {
            self.conn
                .execute(&sql, params_from_iter(values.iter()))
                .map(|_| ())
                .context("insert selected minute metrics")?;
        }
        Ok(())
    }

    pub fn export_csv(&self, path: impl AsRef<Path>, limit: usize) -> Result<usize> {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent).context("create export directory")?;
        }
        let events = self.query_recent(limit)?;
        let mut writer = csv::Writer::from_path(path.as_ref()).context("create csv export")?;
        for event in &events {
            writer.serialize(event).context("write csv event")?;
        }
        writer.flush().context("flush csv export")?;
        Ok(events.len())
    }

    pub fn archive_parquet(
        &self,
        output_path: impl AsRef<Path>,
        limit: usize,
    ) -> Result<ArchiveFile> {
        let output_path = output_path.as_ref();
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent).context("create archive directory")?;
        }

        let sql_path = output_path.to_string_lossy().replace('\'', "''");
        let sql = format!(
            "COPY (SELECT * FROM events ORDER BY ingest_time DESC LIMIT {}) TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD)",
            limit, sql_path
        );
        self.conn
            .execute_batch(&sql)
            .with_context(|| format!("archive events to parquet {}", output_path.display()))?;
        let bytes = fs::metadata(output_path)
            .with_context(|| format!("read archive metadata {}", output_path.display()))?
            .len();
        Ok(ArchiveFile {
            path: output_path.to_path_buf(),
            bytes,
        })
    }

    pub fn query_events(&self, query: &EventQuery, limit: usize) -> Result<Vec<CanonicalEvent>> {
        self.query_events_with_raw(query, limit, true)
    }

    pub fn query_events_without_raw(
        &self,
        query: &EventQuery,
        limit: usize,
    ) -> Result<Vec<CanonicalEvent>> {
        self.query_events_with_raw(query, limit, false)
    }

    fn query_events_with_raw(
        &self,
        query: &EventQuery,
        limit: usize,
        include_raw: bool,
    ) -> Result<Vec<CanonicalEvent>> {
        let mut clauses = Vec::new();
        let mut values = Vec::new();

        if let Some(value) = query.device.as_deref().filter(|value| !value.is_empty()) {
            clauses.push("source_addr LIKE ?");
            values.push(format!("%{value}%"));
        }
        if let Some(value) = query.device_id.as_deref().filter(|value| !value.is_empty()) {
            clauses.push("device_id = ?");
            values.push(value.to_string());
        }
        if let Some(value) = query.day.as_deref().and_then(normalize_day) {
            if include_raw {
                clauses.push("(ingest_time LIKE ? OR event_time LIKE ? OR raw LIKE ?)");
            } else {
                clauses.push("(ingest_time LIKE ? OR event_time LIKE ?)");
            }
            values.push(format!("{value}%"));
            values.push(format!("{value}%"));
            if include_raw {
                values.push(format!("%{value}%"));
            }
        }
        if let Some(value) = query.date_from.as_deref().and_then(normalize_day) {
            clauses.push("substr(COALESCE(event_time, ingest_time), 1, 10) >= ?");
            values.push(value);
        }
        if let Some(value) = query.date_to.as_deref().and_then(normalize_day) {
            clauses.push("substr(COALESCE(event_time, ingest_time), 1, 10) <= ?");
            values.push(value);
        }
        if let Some(value) = query.src_ip.as_deref().filter(|value| !value.is_empty()) {
            clauses.push("src_ip = ?");
            values.push(value.to_string());
        }
        if let Some(value) = query.dst_ip.as_deref().filter(|value| !value.is_empty()) {
            clauses.push("dst_ip = ?");
            values.push(value.to_string());
        }
        if let Some(value) = query.protocol.as_deref().filter(|value| !value.is_empty()) {
            clauses.push("upper(protocol) = ?");
            values.push(normalize_protocol(value));
        }
        if let Some(value) = query.action.as_deref().filter(|value| !value.is_empty()) {
            clauses.push("lower(action) = ?");
            values.push(value.to_ascii_lowercase());
        }
        if let Some(value) = query.keyword.as_deref().filter(|value| !value.is_empty()) {
            if include_raw {
                clauses.push("raw LIKE ?");
                values.push(format!("%{value}%"));
            } else {
                clauses.push("FALSE");
                let _ = value;
            }
        }
        if !query.include_failed {
            clauses.push("parse_status <> 'failed'");
        }

        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", clauses.join(" AND "))
        };
        let raw_expr = if include_raw { "raw" } else { "'' AS raw" };
        let sql = format!(
            r#"
            SELECT event_id, ingest_time, source_addr, device_id, event_time, vendor, product,
                   src_ip, src_port, dst_ip, dst_port, protocol, action, severity,
                   {raw_expr}, parse_status, parse_error
            FROM events
            {where_clause}
            ORDER BY ingest_time DESC, event_id DESC
            LIMIT ?
            "#
        );
        values.push(limit.to_string());
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(values.iter()), row_to_event)?;
        rows.collect::<duckdb::Result<Vec<_>>>()
            .context("query duckdb events with filters")
    }

    pub fn archive_slim_parquet(
        &self,
        output_path: impl AsRef<Path>,
        limit: Option<usize>,
    ) -> Result<ArchiveFile> {
        let output_path = output_path.as_ref();
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent).context("create slim archive directory")?;
        }

        let sql_path = output_path.to_string_lossy().replace('\'', "''");
        let limit_clause = limit
            .map(|value| format!("LIMIT {}", value))
            .unwrap_or_default();
        let sql = format!(
            r#"
            COPY (
              SELECT
                ingest_time,
                source_addr,
                event_time,
                vendor,
                product,
                src_ip,
                src_port,
                dst_ip,
                dst_port,
                protocol,
                action,
                severity,
                parse_status
              FROM events
              {}
            ) TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD)
            "#,
            limit_clause, sql_path
        );
        self.conn
            .execute_batch(&sql)
            .with_context(|| format!("archive slim events to parquet {}", output_path.display()))?;
        let bytes = fs::metadata(output_path)
            .with_context(|| format!("read slim archive metadata {}", output_path.display()))?
            .len();
        Ok(ArchiveFile {
            path: output_path.to_path_buf(),
            bytes,
        })
    }

    pub fn archive_slim_parquet_from_parquet(
        &self,
        input_path: impl AsRef<Path>,
        output_path: impl AsRef<Path>,
    ) -> Result<ArchiveFile> {
        let input_path = input_path.as_ref();
        let output_path = output_path.as_ref();
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent).context("create slim archive directory")?;
        }

        let input_sql_path = input_path.to_string_lossy().replace('\'', "''");
        let output_sql_path = output_path.to_string_lossy().replace('\'', "''");
        let sql = format!(
            r#"
            COPY (
              SELECT
                ingest_time,
                COALESCE(source_addr, '') AS source_addr,
                event_time,
                vendor,
                product,
                src_ip,
                src_port,
                dst_ip,
                dst_port,
                protocol,
                action,
                severity,
                parse_status
              FROM read_parquet('{}')
            ) TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD)
            "#,
            input_sql_path, output_sql_path
        );
        self.conn
            .execute_batch(&sql)
            .with_context(|| format!("write slim parquet {}", output_path.display()))?;
        let bytes = fs::metadata(output_path)
            .with_context(|| format!("read slim archive metadata {}", output_path.display()))?
            .len();
        Ok(ArchiveFile {
            path: output_path.to_path_buf(),
            bytes,
        })
    }

    pub fn archive_events_parquet(
        &self,
        output_path: impl AsRef<Path>,
        events: &[CanonicalEvent],
    ) -> Result<ArchiveFile> {
        let output_path = output_path.as_ref();
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent).context("create archive directory")?;
        }

        let sql_path = output_path.to_string_lossy().replace('\'', "''");
        let copy_sql = format!(
            "COPY archive_events TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD)",
            sql_path
        );
        let tx = self.conn.unchecked_transaction()?;
        tx.execute_batch(
            r#"
            CREATE TEMP TABLE archive_events (
              event_id TEXT,
              ingest_time TEXT NOT NULL,
              source_addr TEXT NOT NULL,
              device_id TEXT,
              event_time TEXT,
              vendor TEXT,
              product TEXT,
              src_ip TEXT,
              src_port INTEGER,
              dst_ip TEXT,
              dst_port INTEGER,
              protocol TEXT,
              action TEXT,
              severity TEXT,
              raw TEXT NOT NULL,
              parse_status TEXT NOT NULL,
              parse_error TEXT
            );
            "#,
        )?;
        {
            let mut stmt = tx.prepare(
                r#"
                INSERT INTO archive_events (
                  event_id, ingest_time, source_addr, device_id, event_time, vendor, product,
                  src_ip, src_port, dst_ip, dst_port, protocol, action, severity,
                  raw, parse_status, parse_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )?;
            for event in events {
                stmt.execute(params![
                    event.event_id.as_str(),
                    event.ingest_time.to_rfc3339(),
                    event.source_addr.as_str(),
                    event.device_id.as_deref(),
                    event.event_time.as_ref().map(|v| v.to_rfc3339()),
                    event.vendor.as_deref(),
                    event.product.as_deref(),
                    event.src_ip.as_deref(),
                    event.src_port.map(i64::from),
                    event.dst_ip.as_deref(),
                    event.dst_port.map(i64::from),
                    event.protocol.as_deref(),
                    event.action.as_deref(),
                    event.severity.as_deref(),
                    event.raw.as_str(),
                    status_str(event.parse_status),
                    event.parse_error.as_deref(),
                ])?;
            }
        }
        tx.execute_batch(&copy_sql).with_context(|| {
            format!(
                "archive selected events to parquet {}",
                output_path.display()
            )
        })?;
        tx.commit()
            .context("commit selected event parquet archive")?;

        let bytes = fs::metadata(output_path)
            .with_context(|| format!("read archive metadata {}", output_path.display()))?
            .len();
        Ok(ArchiveFile {
            path: output_path.to_path_buf(),
            bytes,
        })
    }
}

fn row_to_event(row: &duckdb::Row<'_>) -> duckdb::Result<CanonicalEvent> {
    let ingest_time: String = row.get(1)?;
    let event_time: Option<String> = row.get(4)?;
    let src_port: Option<i64> = row.get(8)?;
    let dst_port: Option<i64> = row.get(10)?;
    let parse_status: String = row.get(15)?;
    Ok(CanonicalEvent {
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

fn metric_cutoff(hours: i64) -> String {
    let cutoff = (chrono::Utc::now() - chrono::Duration::hours(hours))
        .to_rfc3339()
        .chars()
        .take(16)
        .collect::<String>();
    format!("{cutoff}:00Z")
}

fn create_events_table_sql(table: &str, if_not_exists: bool) -> String {
    let if_not_exists = if if_not_exists { "IF NOT EXISTS " } else { "" };
    format!(
        r#"
        CREATE TABLE {if_not_exists}{table} (
          event_id TEXT PRIMARY KEY,
          ingest_time TEXT NOT NULL,
          source_addr TEXT NOT NULL,
          device_id TEXT,
          event_time TEXT,
          vendor TEXT,
          product TEXT,
          src_ip TEXT,
          src_port INTEGER,
          dst_ip TEXT,
          dst_port INTEGER,
          protocol TEXT,
          action TEXT,
          severity TEXT,
          raw TEXT NOT NULL,
          parse_status TEXT NOT NULL,
          parse_error TEXT
        );
        "#
    )
}

fn create_frozen_archive_index_table_sql(if_not_exists: bool) -> String {
    let if_not_exists = if if_not_exists { "IF NOT EXISTS " } else { "" };
    format!(
        r#"
        CREATE TABLE {if_not_exists}frozen_archive_index (
          archive_path TEXT PRIMARY KEY,
          day TEXT NOT NULL,
          source_addr TEXT NOT NULL,
          bytes BIGINT NOT NULL,
          line_count BIGINT NOT NULL,
          first_seen TEXT,
          last_seen TEXT,
          indexed_at TEXT NOT NULL
        );
        "#
    )
}

fn create_ip_region_cache_table_sql(if_not_exists: bool) -> String {
    let if_not_exists = if if_not_exists { "IF NOT EXISTS " } else { "" };
    format!(
        r#"
        CREATE TABLE {if_not_exists}ip_region_cache (
          ip TEXT PRIMARY KEY,
          region TEXT,
          country TEXT,
          province TEXT,
          city TEXT,
          isp TEXT,
          source TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        "#
    )
}

fn create_parser_adaptive_tables_sql() -> &'static str {
    r#"
    CREATE TABLE IF NOT EXISTS parser_profiles (
      scope_key TEXT NOT NULL,
      parser_id TEXT NOT NULL,
      parser_name TEXT NOT NULL,
      success_count BIGINT NOT NULL,
      partial_count BIGINT NOT NULL,
      fail_count BIGINT NOT NULL,
      last_seen TIMESTAMPTZ NOT NULL,
      priority_boost DOUBLE NOT NULL,
      PRIMARY KEY (scope_key, parser_id)
    );

    CREATE TABLE IF NOT EXISTS adaptive_field_rules (
      rule_id TEXT PRIMARY KEY,
      scope_key TEXT NOT NULL,
      raw_key TEXT NOT NULL,
      canonical_field TEXT NOT NULL,
      value_type TEXT NOT NULL,
      status TEXT NOT NULL,
      confidence DOUBLE NOT NULL,
      wins BIGINT NOT NULL,
      sample_count BIGINT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      activated_at TIMESTAMPTZ,
      disabled_at TIMESTAMPTZ,
      disabled_reason TEXT,
      recovery_sample_rate DOUBLE,
      recovery_attempts BIGINT,
      last_recovery_at TIMESTAMPTZ
    );

    CREATE TABLE IF NOT EXISTS parser_scope_state (
      scope_key TEXT PRIMARY KEY,
      source_high_entropy BOOLEAN NOT NULL,
      adaptive_learning_enabled BOOLEAN NOT NULL,
      unknown_source_bucket BOOLEAN NOT NULL,
      metrics_gap BOOLEAN NOT NULL,
      metrics_gap_since TIMESTAMPTZ,
      malformed_flood_until TIMESTAMPTZ,
      shadow_rule_cooldown_until TIMESTAMPTZ,
      adaptive_quarantine_until TIMESTAMPTZ,
      quarantine_backoff_seconds BIGINT NOT NULL,
      quarantine_attempts BIGINT NOT NULL,
      last_state_change TIMESTAMPTZ NOT NULL,
      last_seen TIMESTAMPTZ NOT NULL
    );

    CREATE TABLE IF NOT EXISTS parser_diagnostics (
      fingerprint TEXT PRIMARY KEY,
      scope_key TEXT,
      reason TEXT NOT NULL,
      sample_raw TEXT,
      sample_raw_truncated BOOLEAN NOT NULL,
      count BIGINT NOT NULL,
      suggested_rule_id TEXT,
      last_seen TIMESTAMPTZ NOT NULL
    );

    CREATE TABLE IF NOT EXISTS source_device_aliases (
      source_key TEXT NOT NULL,
      raw_source_addr TEXT NOT NULL,
      device_id TEXT NOT NULL,
      first_seen TIMESTAMPTZ NOT NULL,
      last_seen TIMESTAMPTZ NOT NULL,
      confidence DOUBLE NOT NULL,
      PRIMARY KEY (source_key, raw_source_addr, device_id)
    );

    CREATE TABLE IF NOT EXISTS parser_checkpoint_version (
      snapshot_version BIGINT PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL,
      published_at TIMESTAMPTZ,
      status TEXT NOT NULL,
      profiles_count BIGINT NOT NULL,
      rules_count BIGINT NOT NULL,
      diagnostics_count BIGINT NOT NULL,
      scope_state_count BIGINT NOT NULL,
      aliases_count BIGINT NOT NULL
    );
    "#
}

fn create_minute_metrics_table_sql(table: &str, if_not_exists: bool) -> String {
    let if_not_exists = if if_not_exists { "IF NOT EXISTS " } else { "" };
    format!(
        r#"
        CREATE TABLE {if_not_exists}{table} (
          bucket_minute TEXT NOT NULL,
          source_addr TEXT NOT NULL,
          protocol TEXT NOT NULL,
          action TEXT NOT NULL,
          parse_status TEXT NOT NULL,
          total_count BIGINT NOT NULL,
          raw_bytes BIGINT NOT NULL
        );
        "#
    )
}

fn minute_buckets(events: &[CanonicalEvent]) -> Vec<String> {
    let mut buckets = events
        .iter()
        .map(|event| {
            let text = event.ingest_time.to_rfc3339();
            format!("{}:00Z", &text[..16])
        })
        .collect::<Vec<_>>();
    buckets.sort();
    buckets.dedup();
    buckets
}

fn normalize_day(day: &str) -> Option<String> {
    let digits = day
        .chars()
        .filter(|value| value.is_ascii_digit())
        .collect::<String>();
    if digits.len() == 8 {
        Some(format!(
            "{}-{}-{}",
            &digits[0..4],
            &digits[4..6],
            &digits[6..8]
        ))
    } else {
        None
    }
}

fn normalize_protocol(value: &str) -> String {
    match value.to_ascii_uppercase().as_str() {
        "17" => "UDP".to_string(),
        "6" => "TCP".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use fwlog_domain::RawLog;

    fn event(id: &str, status: ParseStatus) -> CanonicalEvent {
        let raw = RawLog {
            ingest_time: Utc.timestamp_opt(1_778_808_000, 0).unwrap(),
            source_addr: "tcp://127.0.0.1:1514".to_string(),
            raw: format!("raw {id}"),
        };
        let mut event = CanonicalEvent::failed(raw, "bad");
        event.event_id = id.to_string();
        event.parse_status = status;
        if status == ParseStatus::Parsed {
            event.vendor = Some("Sangfor".to_string());
            event.product = Some("Firewall".to_string());
            event.src_ip = Some("192.168.1.10".to_string());
            event.dst_ip = Some("8.8.8.8".to_string());
            event.action = Some("allow".to_string());
            event.parse_error = None;
        }
        event
    }

    fn parsed_event(
        id: &str,
        ingest: i64,
        source: &str,
        src_ip: &str,
        dst_ip: &str,
        protocol: &str,
        action: &str,
    ) -> CanonicalEvent {
        let raw = RawLog {
            ingest_time: Utc.timestamp_opt(ingest, 0).unwrap(),
            source_addr: source.to_string(),
            raw: format!("raw {id} {src_ip} {dst_ip} {protocol} {action}"),
        };
        let mut event = CanonicalEvent::failed(raw, "bad");
        event.event_id = id.to_string();
        event.parse_status = ParseStatus::Parsed;
        event.vendor = Some("Sangfor".to_string());
        event.product = Some("Firewall".to_string());
        event.src_ip = Some(src_ip.to_string());
        event.dst_ip = Some(dst_ip.to_string());
        event.protocol = Some(protocol.to_string());
        event.action = Some(action.to_string());
        event.parse_error = None;
        event
    }

    #[test]
    fn initializes_parser_adaptive_tables() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let store = DuckDbStore::open(&db_path).unwrap();

        let expected = [
            "parser_profiles",
            "adaptive_field_rules",
            "parser_scope_state",
            "parser_diagnostics",
            "source_device_aliases",
            "parser_checkpoint_version",
        ];
        for table in expected {
            let exists: bool = store
                .conn
                .query_row(
                    "SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_name = ?",
                    [table],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(exists, "expected {table} to be initialized");
        }

        assert!(store.list_parser_profiles().unwrap().is_empty());
        assert!(store.list_adaptive_field_rules().unwrap().is_empty());
        assert!(store.list_parser_diagnostics().unwrap().is_empty());
        assert!(store.list_parser_scopes().unwrap().is_empty());
        assert!(store.list_source_device_aliases().unwrap().is_empty());
        assert!(store.list_parser_checkpoint_versions().unwrap().is_empty());
    }

    #[test]
    fn reads_source_aliases_and_parser_checkpoint_versions() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let store = DuckDbStore::open(&db_path).unwrap();

        store
            .conn
            .execute(
                r#"
                INSERT INTO source_device_aliases (
                  source_key, raw_source_addr, device_id, first_seen, last_seen, confidence
                ) VALUES (?, ?, ?, ?, ?, ?)
                "#,
                params![
                    "source:udp://192.168.1.10",
                    "udp://192.168.1.10:55123",
                    "fw-1",
                    "2026-05-19T00:00:00Z",
                    "2026-05-19T00:01:00Z",
                    0.98_f64,
                ],
            )
            .unwrap();
        store
            .conn
            .execute(
                r#"
                INSERT INTO parser_checkpoint_version (
                  snapshot_version, created_at, published_at, status, profiles_count,
                  rules_count, diagnostics_count, scope_state_count, aliases_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
                params![
                    7_i64,
                    "2026-05-19T00:00:00Z",
                    "2026-05-19T00:02:00Z",
                    "published",
                    1_i64,
                    2_i64,
                    3_i64,
                    4_i64,
                    5_i64,
                ],
            )
            .unwrap();

        let aliases = store.list_source_device_aliases().unwrap();
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0].source_key, "source:udp://192.168.1.10");
        assert_eq!(aliases[0].device_id, "fw-1");
        assert_eq!(aliases[0].confidence, 0.98);

        let checkpoints = store.list_parser_checkpoint_versions().unwrap();
        assert_eq!(checkpoints.len(), 1);
        assert_eq!(checkpoints[0].snapshot_version, 7);
        assert_eq!(checkpoints[0].status, "published");
        assert_eq!(checkpoints[0].aliases_count, 5);
    }

    #[test]
    fn checkpoints_complete_adaptive_state_transactionally() {
        let dir = tempfile::tempdir().unwrap();
        let store = DuckDbStore::open(dir.path().join("oxidelog.duckdb")).unwrap();
        let checkpoint = ParserAdaptiveCheckpoint {
            snapshot_version: 42,
            created_at: "2026-05-20T00:00:00Z".to_string(),
            published_at: Some("2026-05-20T00:00:01Z".to_string()),
            profiles: vec![ParserProfileCheckpointRow {
                scope_key: "source:tcp://127.0.0.1".to_string(),
                parser_id: "parser:generic_kv_v1".to_string(),
                parser_name: "GenericKv".to_string(),
                success_count: 10,
                partial_count: 1,
                fail_count: 2,
                last_seen: "2026-05-20T00:00:00Z".to_string(),
                priority_boost: 0.5,
            }],
            rules: vec![AdaptiveFieldRuleCheckpointRow {
                rule_id: "rule:actName".to_string(),
                scope_key: "source:tcp://127.0.0.1".to_string(),
                raw_key: "actName".to_string(),
                canonical_field: "action".to_string(),
                value_type: "action".to_string(),
                status: "active".to_string(),
                confidence: 0.95,
                wins: 200,
                sample_count: 200,
                created_at: "2026-05-20T00:00:00Z".to_string(),
                activated_at: Some("2026-05-20T00:00:01Z".to_string()),
                disabled_at: None,
                disabled_reason: None,
                recovery_sample_rate: None,
                recovery_attempts: None,
                last_recovery_at: None,
            }],
            diagnostics: vec![ParserDiagnosticCheckpointRow {
                fingerprint: "fp1".to_string(),
                scope_key: Some("source:tcp://127.0.0.1".to_string()),
                reason: "partial parse".to_string(),
                sample_raw: Some("raw".to_string()),
                sample_raw_truncated: false,
                count: 3,
                suggested_rule_id: Some("rule:actName".to_string()),
                last_seen: "2026-05-20T00:00:00Z".to_string(),
            }],
            scopes: vec![ParserScopeCheckpointRow {
                scope_key: "source:tcp://127.0.0.1".to_string(),
                source_high_entropy: false,
                adaptive_learning_enabled: true,
                unknown_source_bucket: false,
                metrics_gap: false,
                metrics_gap_since: None,
                malformed_flood_until: None,
                shadow_rule_cooldown_until: None,
                adaptive_quarantine_until: None,
                quarantine_backoff_seconds: 0,
                quarantine_attempts: 0,
                last_state_change: "2026-05-20T00:00:00Z".to_string(),
                last_seen: "2026-05-20T00:00:00Z".to_string(),
            }],
            aliases: vec![SourceDeviceAliasCheckpointRow {
                source_key: "source:tcp://127.0.0.1".to_string(),
                raw_source_addr: "tcp://127.0.0.1:1514".to_string(),
                device_id: "device-a".to_string(),
                first_seen: "2026-05-20T00:00:00Z".to_string(),
                last_seen: "2026-05-20T00:00:00Z".to_string(),
                confidence: 1.0,
            }],
        };

        store.checkpoint_parser_adaptive_state(&checkpoint).unwrap();

        assert_eq!(store.list_parser_profiles().unwrap().len(), 1);
        assert_eq!(store.list_adaptive_field_rules().unwrap().len(), 1);
        assert_eq!(store.list_parser_diagnostics().unwrap().len(), 1);
        assert_eq!(store.list_parser_scopes().unwrap().len(), 1);
        assert_eq!(store.list_source_device_aliases().unwrap().len(), 1);
        let versions = store.list_parser_checkpoint_versions().unwrap();
        assert_eq!(versions[0].snapshot_version, 42);
        assert_eq!(versions[0].status, "published");
        assert_eq!(versions[0].rules_count, 1);
    }

    #[test]
    fn prune_parsed_raw_keeps_failed_raw_and_preserves_fields() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();
        let mut parsed = event("parsed", ParseStatus::Parsed);
        parsed.raw = "large parsed raw".to_string();
        parsed.src_ip = Some("2.55.80.6".to_string());
        let mut failed = event("failed", ParseStatus::Failed);
        failed.raw = "failed raw must stay".to_string();
        store.insert_batch(&[parsed, failed]).unwrap();

        let changed = store.prune_parsed_raw().unwrap();

        assert_eq!(changed, 1);
        let rows = store.query_recent(10).unwrap();
        let parsed = rows.iter().find(|row| row.event_id == "parsed").unwrap();
        let failed = rows.iter().find(|row| row.event_id == "failed").unwrap();
        assert_eq!(parsed.raw, "");
        assert_eq!(parsed.src_ip.as_deref(), Some("2.55.80.6"));
        assert_eq!(failed.raw, "failed raw must stay");
    }

    #[test]
    fn compact_hot_retains_newest_rows_and_prunes_parsed_raw() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let compact_path = dir.path().join("hot.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();
        let mut old = event("old", ParseStatus::Parsed);
        old.ingest_time = Utc.timestamp_opt(1_778_808_000, 0).unwrap();
        let mut new = event("new", ParseStatus::Parsed);
        new.ingest_time = Utc.timestamp_opt(1_778_808_060, 0).unwrap();
        store.insert_batch(&[old, new]).unwrap();

        let copied = store.compact_hot_to(&compact_path, 1, true).unwrap();

        assert_eq!(copied, 1);
        let compact = DuckDbStore::open(&compact_path).unwrap();
        let rows = compact.query_recent(10).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_id, "new");
        assert_eq!(rows[0].raw, "");
    }

    #[test]
    fn initializes_inserts_queries_and_exports_events() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let csv_path = dir.path().join("events.csv");
        let mut store = DuckDbStore::open(&db_path).unwrap();

        let inserted = store
            .insert_batch(&[
                event("one", ParseStatus::Parsed),
                event("two", ParseStatus::Failed),
            ])
            .unwrap();
        assert_eq!(inserted, 2);

        let rows = store.query_recent(10).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows
            .iter()
            .all(|row| row.source_addr == "tcp://127.0.0.1:1514"));

        let exported = store.export_csv(&csv_path, 10).unwrap();
        assert_eq!(exported, 2);
        let csv = std::fs::read_to_string(csv_path).unwrap();
        assert!(csv.contains("event_id"));
        assert!(csv.contains("one"));
        assert!(csv.contains("two"));
    }

    #[test]
    fn archives_recent_events_to_parquet() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let parquet_path = dir.path().join("archives").join("events.parquet");
        let mut store = DuckDbStore::open(&db_path).unwrap();

        store
            .insert_batch(&[
                event("one", ParseStatus::Parsed),
                event("two", ParseStatus::Failed),
            ])
            .unwrap();

        let archive = store.archive_parquet(&parquet_path, 10).unwrap();

        assert_eq!(archive.path, parquet_path);
        assert!(archive.path.exists());
        assert!(archive.bytes > 0);
    }

    #[test]
    fn archives_selected_events_to_parquet() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let parquet_path = dir.path().join("archives").join("selected.parquet");
        let store = DuckDbStore::open(&db_path).unwrap();
        let events = vec![
            event("one", ParseStatus::Parsed),
            event("two", ParseStatus::Failed),
        ];

        let archive = store
            .archive_events_parquet(&parquet_path, &events)
            .unwrap();

        assert_eq!(archive.path, parquet_path);
        assert!(archive.path.exists());
        assert!(archive.bytes > 0);
    }

    #[test]
    fn reports_event_stats_by_parse_status() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();

        let empty = store.event_stats().unwrap();
        assert_eq!(
            empty,
            EventStats {
                total: 0,
                parsed: 0,
                partial: 0,
                failed: 0
            }
        );

        store
            .insert_batch(&[
                event("one", ParseStatus::Parsed),
                event("two", ParseStatus::Parsed),
                event("partial", ParseStatus::Partial),
                event("three", ParseStatus::Failed),
            ])
            .unwrap();

        let stats = store.event_stats().unwrap();
        assert_eq!(
            stats,
            EventStats {
                total: 4,
                parsed: 2,
                partial: 1,
                failed: 1
            }
        );
    }

    #[test]
    fn maintains_minute_metrics_without_querying_recent_events() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();
        let mut first = parsed_event(
            "metric-one",
            1_778_894_401,
            "udp://192.168.0.1:514",
            "2.55.80.6",
            "211.93.49.88",
            "UDP",
            "snat",
        );
        first.raw = "first raw".to_string();
        let mut second = parsed_event(
            "metric-two",
            1_778_894_459,
            "udp://192.168.0.1:514",
            "2.55.80.7",
            "211.93.49.88",
            "UDP",
            "dnat",
        );
        second.raw = "second raw".to_string();
        let mut failed = event("metric-failed", ParseStatus::Failed);
        failed.ingest_time = Utc.timestamp_opt(1_778_894_459, 0).unwrap();
        failed.source_addr = "udp://192.168.0.1:514".to_string();
        failed.raw = "failed raw".to_string();
        let mut partial = event("metric-partial", ParseStatus::Partial);
        partial.ingest_time = Utc.timestamp_opt(1_778_894_459, 0).unwrap();
        partial.source_addr = "udp://192.168.0.1:514".to_string();
        partial.raw = "partial raw".to_string();

        store
            .insert_batch(&[first, second, failed, partial])
            .unwrap();

        let metrics = store
            .query_minute_metrics(&MinuteMetricQuery {
                hours: 24 * 365,
                limit: 10,
            })
            .unwrap();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].bucket_minute, "2026-05-16T01:20:00Z");
        assert_eq!(metrics[0].total, 4);
        assert_eq!(metrics[0].parsed, 2);
        assert_eq!(metrics[0].partial, 1);
        assert_eq!(metrics[0].failed, 1);
        assert_eq!(metrics[0].raw_bytes, 40);
    }

    #[test]
    fn reports_source_metrics_from_precomputed_minute_table() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();
        let mut first = parsed_event(
            "source-one",
            1_778_894_401,
            "udp://192.168.0.1:514",
            "2.55.80.6",
            "211.93.49.88",
            "UDP",
            "snat",
        );
        first.raw = "first raw".to_string();
        let mut second = parsed_event(
            "source-two",
            1_778_894_459,
            "udp://192.168.0.2:514",
            "2.55.80.7",
            "211.93.49.88",
            "UDP",
            "dnat",
        );
        second.raw = "second raw".to_string();
        let mut failed = event("source-failed", ParseStatus::Failed);
        failed.ingest_time = Utc.timestamp_opt(1_778_894_459, 0).unwrap();
        failed.source_addr = "udp://192.168.0.1:514".to_string();
        failed.raw = "failed raw".to_string();
        let mut partial = event("source-partial", ParseStatus::Partial);
        partial.ingest_time = Utc.timestamp_opt(1_778_894_459, 0).unwrap();
        partial.source_addr = "udp://192.168.0.1:514".to_string();
        partial.raw = "partial raw".to_string();

        store
            .insert_batch(&[first, second, failed, partial])
            .unwrap();

        let metrics = store
            .query_source_metrics(&SourceMetricQuery {
                hours: 24 * 365,
                limit: 10,
            })
            .unwrap();
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0].source_addr, "udp://192.168.0.1:514");
        assert_eq!(metrics[0].total, 3);
        assert_eq!(metrics[0].parsed, 1);
        assert_eq!(metrics[0].partial, 1);
        assert_eq!(metrics[0].failed, 1);
        assert_eq!(metrics[0].raw_bytes, 30);
        assert_eq!(metrics[0].last_seen, "2026-05-16T01:20:00Z");
        assert_eq!(metrics[1].source_addr, "udp://192.168.0.2:514");
        assert_eq!(metrics[1].total, 1);
        assert_eq!(metrics[1].parsed, 1);
        assert_eq!(metrics[1].partial, 0);
        assert_eq!(metrics[1].failed, 0);
        assert_eq!(metrics[1].raw_bytes, 10);
        assert_eq!(metrics[1].last_seen, "2026-05-16T01:20:00Z");
    }

    #[test]
    fn query_events_filters_in_duckdb_by_common_fields() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();
        let mut failed = event("failed", ParseStatus::Failed);
        failed.ingest_time = Utc.timestamp_opt(1_778_894_400, 0).unwrap();

        store
            .insert_batch(&[
                parsed_event(
                    "hit",
                    1_778_894_400,
                    "udp://192.168.0.1:514",
                    "2.55.80.6",
                    "211.93.49.88",
                    "UDP",
                    "snat",
                ),
                parsed_event(
                    "wrong-day",
                    1_778_980_800,
                    "udp://192.168.0.1:514",
                    "2.55.80.6",
                    "211.93.49.88",
                    "UDP",
                    "snat",
                ),
                parsed_event(
                    "wrong-proto",
                    1_778_894_400,
                    "udp://192.168.0.1:514",
                    "2.55.80.6",
                    "211.93.49.88",
                    "TCP",
                    "snat",
                ),
                failed,
            ])
            .unwrap();

        let rows = store
            .query_events(
                &EventQuery {
                    day: Some("2026-05-16".to_string()),
                    date_from: None,
                    date_to: None,
                    device: Some("192.168.0.1".to_string()),
                    device_id: None,
                    src_ip: Some("2.55.80.6".to_string()),
                    dst_ip: Some("211.93.49.88".to_string()),
                    protocol: Some("UDP".to_string()),
                    action: Some("snat".to_string()),
                    keyword: Some("2.55.80.6".to_string()),
                    include_failed: false,
                },
                20,
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_id, "hit");
    }

    #[test]
    fn include_failed_false_keeps_partial_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();

        store
            .insert_batch(&[
                event("parsed", ParseStatus::Parsed),
                event("partial", ParseStatus::Partial),
                event("failed", ParseStatus::Failed),
            ])
            .unwrap();

        let rows = store
            .query_events(
                &EventQuery {
                    include_failed: false,
                    ..EventQuery::default()
                },
                10,
            )
            .unwrap();

        let ids: Vec<_> = rows.iter().map(|row| row.event_id.as_str()).collect();
        assert!(ids.contains(&"parsed"));
        assert!(ids.contains(&"partial"));
        assert!(!ids.contains(&"failed"));
    }

    #[test]
    fn archive_index_filters_by_day_before_scan() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let store = DuckDbStore::open(&db_path).unwrap();
        store
            .upsert_frozen_archive_index(
                "raw-import-20260425-a.tar.zst",
                "2026-04-25",
                "udp://192.168.0.1:514",
                100,
                10,
            )
            .unwrap();
        store
            .upsert_frozen_archive_index(
                "raw-import-20260426-a.tar.zst",
                "2026-04-26",
                "udp://192.168.0.1:514",
                100,
                10,
            )
            .unwrap();

        let files = store.find_frozen_archives("2026-04-25", None).unwrap();

        assert_eq!(files, vec!["raw-import-20260425-a.tar.zst".to_string()]);
    }

    #[test]
    fn query_events_filters_by_device_id() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();
        let mut first = parsed_event(
            "device-hit",
            1_778_894_400,
            "udp://192.168.0.1:514",
            "2.55.80.6",
            "211.93.49.88",
            "UDP",
            "snat",
        );
        first.device_id = Some("device-a".to_string());
        let mut second = parsed_event(
            "device-miss",
            1_778_894_401,
            "udp://192.168.0.2:514",
            "2.55.80.7",
            "211.93.49.88",
            "UDP",
            "snat",
        );
        second.device_id = Some("device-b".to_string());
        store.insert_batch(&[first, second]).unwrap();

        let rows = store
            .query_events(
                &EventQuery {
                    device_id: Some("device-a".to_string()),
                    include_failed: false,
                    ..EventQuery::default()
                },
                20,
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_id, "device-hit");
        assert_eq!(rows[0].device_id.as_deref(), Some("device-a"));
    }

    #[test]
    fn backfills_device_ids_from_source_address_bindings() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();
        let first = parsed_event(
            "backfill-hit",
            1_778_894_400,
            "udp://192.168.0.1:514",
            "2.55.80.6",
            "211.93.49.88",
            "UDP",
            "snat",
        );
        let second = parsed_event(
            "backfill-miss",
            1_778_894_401,
            "udp://192.168.0.2:514",
            "2.55.80.7",
            "211.93.49.88",
            "UDP",
            "snat",
        );
        store.insert_batch(&[first, second]).unwrap();

        let updated = store
            .backfill_device_ids(&[DeviceBinding {
                id: "device-a".to_string(),
                protocol: "udp".to_string(),
                host: "192.168.0.1".to_string(),
                port: 514,
            }])
            .unwrap();

        assert_eq!(updated, 1);
        let rows = store
            .query_events(
                &EventQuery {
                    device_id: Some("device-a".to_string()),
                    include_failed: false,
                    ..EventQuery::default()
                },
                20,
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_id, "backfill-hit");
    }

    #[test]
    fn stores_and_reads_ip_region_cache_entries() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let store = DuckDbStore::open(&db_path).unwrap();
        store
            .upsert_ip_region_cache(&IpRegionCacheEntry {
                ip: "2.55.80.6".to_string(),
                region: Some("RU Moscow ISP".to_string()),
                country: Some("RU".to_string()),
                province: Some("Moscow".to_string()),
                city: Some("Moscow".to_string()),
                isp: Some("ISP".to_string()),
                source: "ip2region".to_string(),
                updated_at: "2026-05-18T00:00:00Z".to_string(),
            })
            .unwrap();

        let cached = store.get_ip_region_cache("2.55.80.6").unwrap().unwrap();

        assert_eq!(cached.region.as_deref(), Some("RU Moscow ISP"));
        assert_eq!(cached.source, "ip2region");
    }

    #[test]
    fn migrates_existing_database_without_source_addr_column() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("legacy.duckdb");
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                r#"
                CREATE TABLE events (
                  event_id TEXT PRIMARY KEY,
                  ingest_time TEXT NOT NULL,
                  event_time TEXT,
                  vendor TEXT,
                  product TEXT,
                  src_ip TEXT,
                  src_port INTEGER,
                  dst_ip TEXT,
                  dst_port INTEGER,
                  protocol TEXT,
                  action TEXT,
                  severity TEXT,
                  raw TEXT NOT NULL,
                  parse_status TEXT NOT NULL,
                  parse_error TEXT
                );
                INSERT INTO events (
                  event_id, ingest_time, event_time, vendor, product,
                  src_ip, src_port, dst_ip, dst_port, protocol, action, severity,
                  raw, parse_status, parse_error
                ) VALUES (
                  'legacy-one', '2026-05-16T00:00:00Z', NULL, 'Sangfor', 'Firewall',
                  '192.168.1.10', 12345, '8.8.8.8', 53, 'UDP', 'snat', NULL,
                  'raw legacy', 'parsed', NULL
                );
                "#,
            )
            .unwrap();
        }

        let store = DuckDbStore::open(&db_path).unwrap();
        let rows = store.query_recent(10).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].source_addr, "unknown://legacy");
    }

    #[test]
    fn replace_all_events_preserves_ids_and_counts() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();
        let original = vec![
            event("one", ParseStatus::Parsed),
            event("two", ParseStatus::Failed),
        ];
        store.insert_batch(&original).unwrap();

        let mut replacement = original.clone();
        replacement[0].action = Some("snat".to_string());
        let rows = store.replace_all_events(&replacement, 2).unwrap();

        assert_eq!(rows, 2);
        let stored = store.query_recent(10).unwrap();
        assert_eq!(stored.len(), 2);
        assert!(stored.iter().any(|row| row.event_id == "one"));
        assert!(stored.iter().any(|row| row.event_id == "two"));
        assert_eq!(store.event_stats().unwrap().total, 2);
    }

    #[test]
    fn replace_all_events_rejects_empty_raw_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();
        let mut compacted = event("one", ParseStatus::Parsed);
        compacted.raw = String::new();
        store.insert_batch(&[compacted]).unwrap();

        let err = store
            .replace_all_events(&[event("replacement", ParseStatus::Parsed)], 1)
            .unwrap_err()
            .to_string();

        assert!(err.contains("empty raw payloads"));
        assert_eq!(store.event_stats().unwrap().total, 1);
    }

    #[test]
    fn replace_all_events_rejects_changed_source_row_count() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();
        store
            .insert_batch(&[event("one", ParseStatus::Parsed)])
            .unwrap();

        let err = store
            .replace_all_events(&[event("replacement", ParseStatus::Parsed)], 2)
            .unwrap_err()
            .to_string();

        assert!(err.contains("source row count changed"));
        let stored = store.query_recent(10).unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].event_id, "one");
    }

    #[test]
    fn compacts_to_new_database_and_drops_only_parsed_raw() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let compact_path = dir.path().join("compact.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();

        store
            .insert_batch(&[
                event("one", ParseStatus::Parsed),
                event("two", ParseStatus::Failed),
            ])
            .unwrap();

        let copied = store.compact_to(&compact_path, true).unwrap();
        assert_eq!(copied, 2);

        let compact = DuckDbStore::open(&compact_path).unwrap();
        let rows = compact.query_recent(10).unwrap();
        let parsed = rows.iter().find(|row| row.event_id == "one").unwrap();
        let failed = rows.iter().find(|row| row.event_id == "two").unwrap();
        assert_eq!(parsed.raw, "");
        assert_eq!(failed.raw, "raw two");
    }

    #[test]
    fn compacts_to_new_database_with_hot_limit() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let compact_path = dir.path().join("hot.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();

        let mut old = event("old", ParseStatus::Parsed);
        old.ingest_time = Utc.timestamp_opt(1_778_808_000, 0).unwrap();
        let mut middle = event("middle", ParseStatus::Parsed);
        middle.ingest_time = Utc.timestamp_opt(1_778_808_001, 0).unwrap();
        let mut new = event("new", ParseStatus::Parsed);
        new.ingest_time = Utc.timestamp_opt(1_778_808_002, 0).unwrap();
        store.insert_batch(&[old, middle, new]).unwrap();

        let copied = store.compact_hot_to(&compact_path, 2, true).unwrap();
        assert_eq!(copied, 2);

        let compact = DuckDbStore::open(&compact_path).unwrap();
        let rows = compact.query_recent(10).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|row| row.event_id == "new"));
        assert!(rows.iter().any(|row| row.event_id == "middle"));
        assert!(!rows.iter().any(|row| row.event_id == "old"));
    }

    #[test]
    fn compacts_to_new_database_with_limit_without_sorting() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let compact_path = dir.path().join("limited.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();

        store
            .insert_batch(&[
                event("one", ParseStatus::Parsed),
                event("two", ParseStatus::Parsed),
                event("three", ParseStatus::Failed),
            ])
            .unwrap();

        let copied = store.compact_limit_to(&compact_path, 2, true).unwrap();
        assert_eq!(copied, 2);

        let compact = DuckDbStore::open(&compact_path).unwrap();
        let rows = compact.query_recent(10).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.raw.is_empty()));
    }

    #[test]
    fn archives_slim_parquet_without_raw_event_id_or_parse_error() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let parquet_path = dir.path().join("archives").join("slim.parquet");
        let mut store = DuckDbStore::open(&db_path).unwrap();

        store
            .insert_batch(&[
                event("one", ParseStatus::Parsed),
                event("two", ParseStatus::Failed),
            ])
            .unwrap();

        let archive = store.archive_slim_parquet(&parquet_path, None).unwrap();
        assert!(archive.bytes > 0);

        let conn = Connection::open_in_memory().unwrap();
        let columns: String = conn
            .query_row(
                &format!(
                    "SELECT string_agg(column_name, ',') FROM (DESCRIBE SELECT * FROM read_parquet('{}'))",
                    parquet_path.to_string_lossy().replace('\'', "''")
                ),
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(columns.contains("source_addr"));
        assert!(columns.contains("src_ip"));
        assert!(!columns.contains("raw"));
        assert!(!columns.contains("event_id"));
        assert!(!columns.contains("parse_error"));
    }
}
