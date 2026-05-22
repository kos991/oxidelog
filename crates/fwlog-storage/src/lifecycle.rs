use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::DuckDbStore;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct LifecycleReport {
    pub hot_limit: usize,
    pub compacted_rows: usize,
    pub pruned_raw_rows: usize,
    pub output_path: PathBuf,
}

pub fn run_lifecycle_to(
    duckdb_path: impl AsRef<Path>,
    output_path: impl AsRef<Path>,
    hot_limit: usize,
    drop_parsed_raw: bool,
) -> Result<LifecycleReport> {
    let output_path = output_path.as_ref().to_path_buf();
    let store = DuckDbStore::open(duckdb_path)?;
    let compacted_rows = store.compact_hot_to(&output_path, hot_limit.max(1), drop_parsed_raw)?;
    Ok(LifecycleReport {
        hot_limit: hot_limit.max(1),
        compacted_rows,
        pruned_raw_rows: 0,
        output_path,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use fwlog_domain::{CanonicalEvent, ParseStatus, RawLog};

    #[test]
    fn lifecycle_compacts_to_output_without_pruning_active_database() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("active.duckdb");
        let output_path = dir.path().join("compact.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();
        let raw = RawLog {
            ingest_time: Utc::now(),
            source_addr: "udp://192.168.0.1:514".to_string(),
            raw: "parsed raw must remain active".to_string(),
        };
        let mut event = CanonicalEvent::failed(raw, "bad");
        event.event_id = "lifecycle-active".to_string();
        event.parse_status = ParseStatus::Parsed;
        event.parse_error = None;
        store.insert_batch(&[event]).unwrap();
        drop(store);

        let report = run_lifecycle_to(&db_path, &output_path, 10, true).unwrap();

        assert_eq!(report.compacted_rows, 1);
        assert_eq!(report.pruned_raw_rows, 0);
        let active = DuckDbStore::open(&db_path)
            .unwrap()
            .query_recent(10)
            .unwrap();
        assert_eq!(active[0].raw, "parsed raw must remain active");
        let compact = DuckDbStore::open(&output_path)
            .unwrap()
            .query_recent(10)
            .unwrap();
        assert_eq!(compact[0].raw, "");
    }
}
