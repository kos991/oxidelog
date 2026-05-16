use std::{fs, path::Path};

use anyhow::{bail, Context, Result};
use duckdb::{params, Connection};
use fwlog_domain::{CanonicalEvent, ParseStatus};

use crate::archive::ArchiveFile;

pub struct DuckDbStore {
    conn: Connection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventStats {
    pub total: u64,
    pub parsed: u64,
    pub failed: u64,
}

impl DuckDbStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent).context("create duckdb directory")?;
        }
        let conn = Connection::open(path.as_ref()).context("open duckdb database")?;
        let store = Self { conn };
        store.init()?;
        Ok(store)
    }

    fn init(&self) -> Result<()> {
        self.conn.execute_batch(&create_events_table_sql("events", true))?;
        Ok(())
    }

    pub fn insert_batch(&mut self, events: &[CanonicalEvent]) -> Result<usize> {
        let tx = self.conn.transaction().context("begin duckdb transaction")?;
        let mut inserted = 0;
        {
            let mut stmt = tx.prepare(
                r#"
                INSERT OR IGNORE INTO events (
                  event_id, ingest_time, event_time, vendor, product,
                  src_ip, src_port, dst_ip, dst_port, protocol, action, severity,
                  raw, parse_status, parse_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )?;
            for event in events {
                inserted += stmt.execute(params![
                    event.event_id.as_str(),
                    event.ingest_time.to_rfc3339(),
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
        tx.commit().context("commit duckdb transaction")?;
        Ok(inserted)
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
        Ok(0)
    }

    pub fn append_events(&self, events: &[CanonicalEvent]) -> Result<usize> {
        if events.is_empty() {
            return Ok(0);
        }

        self.conn.execute_batch("CREATE TEMP TABLE IF NOT EXISTS import_events AS SELECT * FROM events LIMIT 0;")?;
        {
            let mut app = self.conn.appender("import_events").context("create appender")?;
            for event in events {
                app.append_row(params![
                    event.event_id.as_str(),
                    event.ingest_time.to_rfc3339(),
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

        Ok(events.len())
    }

    pub fn compact_to(
        &self,
        output_path: impl AsRef<Path>,
        drop_parsed_raw: bool,
    ) -> Result<usize> {
        self.compact_selected_to(output_path, drop_parsed_raw, None, false)
    }

    pub fn compact_hot_to(
        &self,
        output_path: impl AsRef<Path>,
        hot_limit: usize,
        drop_parsed_raw: bool,
    ) -> Result<usize> {
        self.compact_selected_to(output_path, drop_parsed_raw, Some(hot_limit), true)
    }

    pub fn compact_limit_to(
        &self,
        output_path: impl AsRef<Path>,
        limit: usize,
        drop_parsed_raw: bool,
    ) -> Result<usize> {
        self.compact_selected_to(output_path, drop_parsed_raw, Some(limit), false)
    }

    fn compact_selected_to(
        &self,
        output_path: impl AsRef<Path>,
        drop_parsed_raw: bool,
        hot_limit: Option<usize>,
        order_by_newest: bool,
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
              event_id, ingest_time, event_time, vendor, product,
              src_ip, src_port, dst_ip, dst_port, protocol, action, severity,
              raw, parse_status, parse_error
            )
            SELECT
              event_id, ingest_time, event_time, vendor, product,
              src_ip, src_port, dst_ip, dst_port, protocol, action, severity,
              {}, parse_status, parse_error
            FROM events
            {}
            {};
            DETACH compact;
            "#,
            sql_path,
            create_events_table_sql("compact.events", false),
            raw_expr,
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
        let mut stmt = self.conn.prepare(
            r#"
            SELECT event_id, ingest_time, event_time, vendor, product,
                   src_ip, src_port, dst_ip, dst_port, protocol, action, severity,
                   raw, parse_status, parse_error
            FROM events
            ORDER BY ingest_time DESC
            LIMIT ?
            "#,
        )?;
        let rows = stmt.query_map([limit as i64], row_to_event)?;
        rows.collect::<duckdb::Result<Vec<_>>>()
            .context("query duckdb events")
    }

    pub fn event_stats(&self) -> Result<EventStats> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN parse_status = 'parsed' THEN 1 ELSE 0 END) AS parsed,
              SUM(CASE WHEN parse_status = 'failed' THEN 1 ELSE 0 END) AS failed
            FROM events
            "#,
        )?;
        let stats = stmt.query_row([], |row| {
            Ok(EventStats {
                total: row.get::<_, i64>(0)?.max(0) as u64,
                parsed: row.get::<_, Option<i64>>(1)?.unwrap_or(0).max(0) as u64,
                failed: row.get::<_, Option<i64>>(2)?.unwrap_or(0).max(0) as u64,
            })
        })?;
        Ok(stats)
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
                  event_id, ingest_time, event_time, vendor, product,
                  src_ip, src_port, dst_ip, dst_port, protocol, action, severity,
                  raw, parse_status, parse_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )?;
            for event in events {
                stmt.execute(params![
                    event.event_id.as_str(),
                    event.ingest_time.to_rfc3339(),
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
        tx.execute_batch(&copy_sql)
            .with_context(|| format!("archive selected events to parquet {}", output_path.display()))?;
        tx.commit().context("commit selected event parquet archive")?;

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
    let event_time: Option<String> = row.get(2)?;
    let src_port: Option<i64> = row.get(6)?;
    let dst_port: Option<i64> = row.get(8)?;
    let parse_status: String = row.get(13)?;
    Ok(CanonicalEvent {
        event_id: row.get(0)?,
        ingest_time: chrono::DateTime::parse_from_rfc3339(&ingest_time)
            .map(|v| v.with_timezone(&chrono::Utc))
            .unwrap_or_else(|_| chrono::Utc::now()),
        event_time: event_time.and_then(|value| {
            chrono::DateTime::parse_from_rfc3339(&value)
                .map(|v| v.with_timezone(&chrono::Utc))
                .ok()
        }),
        vendor: row.get(3)?,
        product: row.get(4)?,
        src_ip: row.get(5)?,
        src_port: src_port.and_then(|v| u16::try_from(v).ok()),
        dst_ip: row.get(7)?,
        dst_port: dst_port.and_then(|v| u16::try_from(v).ok()),
        protocol: row.get(9)?,
        action: row.get(10)?,
        severity: row.get(11)?,
        raw: row.get(12)?,
        parse_status: if parse_status == "parsed" {
            ParseStatus::Parsed
        } else {
            ParseStatus::Failed
        },
        parse_error: row.get(14)?,
    })
}

fn status_str(status: ParseStatus) -> &'static str {
    match status {
        ParseStatus::Parsed => "parsed",
        ParseStatus::Failed => "failed",
    }
}

fn create_events_table_sql(table: &str, if_not_exists: bool) -> String {
    let if_not_exists = if if_not_exists { "IF NOT EXISTS " } else { "" };
    format!(
        r#"
        CREATE TABLE {if_not_exists}{table} (
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
        "#
    )
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

    #[test]
    fn initializes_inserts_queries_and_exports_events() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let csv_path = dir.path().join("events.csv");
        let mut store = DuckDbStore::open(&db_path).unwrap();

        let inserted = store
            .insert_batch(&[event("one", ParseStatus::Parsed), event("two", ParseStatus::Failed)])
            .unwrap();
        assert_eq!(inserted, 2);

        let rows = store.query_recent(10).unwrap();
        assert_eq!(rows.len(), 2);

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
            .insert_batch(&[event("one", ParseStatus::Parsed), event("two", ParseStatus::Failed)])
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
        let events = vec![event("one", ParseStatus::Parsed), event("two", ParseStatus::Failed)];

        let archive = store.archive_events_parquet(&parquet_path, &events).unwrap();

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
                failed: 0
            }
        );

        store
            .insert_batch(&[
                event("one", ParseStatus::Parsed),
                event("two", ParseStatus::Parsed),
                event("three", ParseStatus::Failed),
            ])
            .unwrap();

        let stats = store.event_stats().unwrap();
        assert_eq!(
            stats,
            EventStats {
                total: 3,
                parsed: 2,
                failed: 1
            }
        );
    }

    #[test]
    fn compacts_to_new_database_and_drops_only_parsed_raw() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let compact_path = dir.path().join("compact.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();

        store
            .insert_batch(&[event("one", ParseStatus::Parsed), event("two", ParseStatus::Failed)])
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
}
