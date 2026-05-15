use std::{fs, path::Path};

use anyhow::{Context, Result};
use duckdb::{params, Connection};
use fwlog_domain::{CanonicalEvent, ParseStatus};

pub struct DuckDbStore {
    conn: Connection,
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
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS events (
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
            "#,
        )?;
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
}
