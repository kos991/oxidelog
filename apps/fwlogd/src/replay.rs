use std::path::PathBuf;

use anyhow::{Context, Result};
use fwlog_adapter::LogAdapter;
use fwlog_domain::{CanonicalEvent, RawLog, RuntimeMetrics};
use fwlog_spool::{discover_uncommitted_segments, rename_segment_to_committed, replay_segment, ReplaySegment, SpoolCheckpoint};
use fwlog_storage::DuckDbStore;
use tracing::{error, info, warn};

#[derive(Debug, Default)]
pub struct ReplayReport {
    pub segments_found: usize,
    pub segments_replayed: usize,
    pub records_replayed: usize,
    pub events_stored: usize,
    pub segments_committed: usize,
    pub segments_failed: usize,
}

pub fn replay_spool_on_startup<A: LogAdapter>(
    spool_dir: PathBuf,
    duckdb_path: PathBuf,
    adapter: &A,
    metrics: &RuntimeMetrics,
) -> Result<ReplayReport> {
    let segments = discover_uncommitted_segments(&spool_dir)
        .context("discover uncommitted segments")?;

    if segments.is_empty() {
        info!("no uncommitted segments found, skipping replay");
        return Ok(ReplayReport::default());
    }

    info!(
        segments = segments.len(),
        "starting spool replay"
    );

    let mut report = ReplayReport {
        segments_found: segments.len(),
        ..Default::default()
    };

    for segment in segments {
        match replay_single_segment(
            &segment,
            &duckdb_path,
            adapter,
            metrics,
        ) {
            Ok(segment_report) => {
                report.segments_replayed += 1;
                report.records_replayed += segment_report.records_replayed;
                report.events_stored += segment_report.events_stored;
                report.segments_committed += 1;

                info!(
                    segment = %segment.path.display(),
                    records = segment_report.records_replayed,
                    events = segment_report.events_stored,
                    "segment replayed successfully"
                );
            }
            Err(err) => {
                report.segments_failed += 1;
                error!(
                    segment = %segment.path.display(),
                    error = %err,
                    "segment replay failed"
                );
            }
        }
    }

    info!(
        segments_replayed = report.segments_replayed,
        records_replayed = report.records_replayed,
        events_stored = report.events_stored,
        segments_failed = report.segments_failed,
        "spool replay completed"
    );

    Ok(report)
}

#[derive(Debug, Default)]
struct SegmentReplayReport {
    records_replayed: usize,
    events_stored: usize,
}

fn replay_single_segment<A: LogAdapter>(
    segment: &ReplaySegment,
    duckdb_path: &PathBuf,
    adapter: &A,
    metrics: &RuntimeMetrics,
) -> Result<SegmentReplayReport> {
    let checkpoint = SpoolCheckpoint::default();
    let records = replay_segment(segment, checkpoint)
        .with_context(|| format!("replay segment {}", segment.path.display()))?;

    if records.is_empty() {
        warn!(
            segment = %segment.path.display(),
            "segment has no records to replay"
        );
        rename_segment_to_committed(segment)
            .with_context(|| format!("mark empty segment as committed {}", segment.path.display()))?;
        return Ok(SegmentReplayReport::default());
    }

    let mut events = Vec::with_capacity(records.len());
    for record in &records {
        let raw = RawLog {
            ingest_time: record.ingest_time,
            source_addr: record.source_addr.clone(),
            raw: record.raw.clone(),
        };

        let event = adapter.parse(raw);
        events.push(event);
    }

    let mut store = DuckDbStore::open(duckdb_path)
        .with_context(|| format!("open duckdb for replay {}", duckdb_path.display()))?;

    let inserted = store
        .insert_batch(&mut events)
        .context("insert replayed events")?;

    metrics.add_events_stored(inserted as u64);
    metrics.add_spool_replayed(records.len() as u64);

    rename_segment_to_committed(segment)
        .with_context(|| format!("mark segment as committed {}", segment.path.display()))?;

    Ok(SegmentReplayReport {
        records_replayed: records.len(),
        events_stored: inserted,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use fwlog_adapter::SangforAdapter;
    use fwlog_spool::SegmentWriter;

    fn raw(value: &str) -> RawLog {
        RawLog {
            ingest_time: Utc.timestamp_opt(1_778_808_000, 0).unwrap(),
            source_addr: "tcp://127.0.0.1:1514".to_string(),
            raw: value.to_string(),
        }
    }

    #[test]
    fn replay_empty_spool_returns_zero_report() {
        let spool_dir = tempfile::tempdir().unwrap();
        let db_dir = tempfile::tempdir().unwrap();
        let duckdb_path = db_dir.path().join("test.duckdb");
        let adapter = SangforAdapter;
        let metrics = RuntimeMetrics::default();

        let report = replay_spool_on_startup(
            spool_dir.path().to_path_buf(),
            duckdb_path,
            &adapter,
            &metrics,
        )
        .unwrap();

        assert_eq!(report.segments_found, 0);
        assert_eq!(report.segments_replayed, 0);
    }

    #[test]
    fn replay_single_segment_with_three_records() {
        let spool_dir = tempfile::tempdir().unwrap();
        let db_dir = tempfile::tempdir().unwrap();
        let duckdb_path = db_dir.path().join("test.duckdb");

        let mut writer = SegmentWriter::create(spool_dir.path(), "segment-test").unwrap();
        writer.append(raw("log line 1")).unwrap();
        writer.append(raw("log line 2")).unwrap();
        writer.append(raw("log line 3")).unwrap();
        writer.seal().unwrap();

        DuckDbStore::open(&duckdb_path).unwrap();

        let adapter = SangforAdapter;
        let metrics = RuntimeMetrics::default();

        let report = replay_spool_on_startup(
            spool_dir.path().to_path_buf(),
            duckdb_path.clone(),
            &adapter,
            &metrics,
        )
        .unwrap();

        assert_eq!(report.segments_found, 1);
        assert_eq!(report.segments_replayed, 1);
        assert_eq!(report.records_replayed, 3);
        assert_eq!(report.events_stored, 3);
        assert_eq!(report.segments_committed, 1);
        assert_eq!(report.segments_failed, 0);

        let segments = discover_uncommitted_segments(spool_dir.path()).unwrap();
        assert_eq!(segments.len(), 0);
    }
}
