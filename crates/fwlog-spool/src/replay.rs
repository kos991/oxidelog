use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use tracing::{info, warn};

use crate::{SegmentReader, SpoolCheckpoint, SpoolRecord};

#[derive(Debug, Clone)]
pub struct ReplaySegment {
    pub path: PathBuf,
    pub name: String,
}

#[derive(Debug, Default)]
pub struct ReplayStats {
    pub segments_found: usize,
    pub records_replayed: usize,
    pub segments_deleted: usize,
}

pub fn discover_uncommitted_segments(spool_dir: impl AsRef<Path>) -> Result<Vec<ReplaySegment>> {
    let dir = spool_dir.as_ref();
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let mut segments = Vec::new();
    let entries =
        fs::read_dir(dir).with_context(|| format!("read spool directory {}", dir.display()))?;

    for entry in entries {
        let entry = entry.context("read directory entry")?;
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        let Some(file_name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };

        if file_name.ends_with(".sealed") || file_name.ends_with(".open") {
            segments.push(ReplaySegment {
                path: path.clone(),
                name: file_name.to_string(),
            });
        }
    }

    segments.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(segments)
}

pub fn replay_segment(
    segment: &ReplaySegment,
    checkpoint: SpoolCheckpoint,
) -> Result<Vec<SpoolRecord>> {
    let reader = SegmentReader::open(&segment.path);
    reader
        .read_after(checkpoint)
        .with_context(|| format!("replay segment {}", segment.path.display()))
}

pub fn delete_segment(segment: &ReplaySegment) -> Result<()> {
    fs::remove_file(&segment.path)
        .with_context(|| format!("delete segment {}", segment.path.display()))
}

pub fn rename_segment_to_committed(segment: &ReplaySegment) -> Result<PathBuf> {
    let committed_path = segment.path.with_extension("committed");
    fs::rename(&segment.path, &committed_path).with_context(|| {
        format!(
            "rename {} to {}",
            segment.path.display(),
            committed_path.display()
        )
    })?;
    Ok(committed_path)
}

pub fn cleanup_committed_segments(spool_dir: impl AsRef<Path>) -> Result<usize> {
    let dir = spool_dir.as_ref();
    if !dir.exists() {
        return Ok(0);
    }

    let mut deleted = 0;
    let entries =
        fs::read_dir(dir).with_context(|| format!("read spool directory {}", dir.display()))?;

    for entry in entries {
        let entry = entry.context("read directory entry")?;
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        let Some(file_name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };

        if file_name.ends_with(".committed") {
            match fs::remove_file(&path) {
                Ok(()) => {
                    deleted += 1;
                    info!(path = %path.display(), "deleted committed segment");
                }
                Err(err) => {
                    warn!(
                        path = %path.display(),
                        error = %err,
                        "failed to delete committed segment"
                    );
                }
            }
        }
    }

    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SegmentWriter;
    use chrono::{TimeZone, Utc};
    use fwlog_domain::RawLog;

    fn raw(value: &str) -> RawLog {
        RawLog {
            ingest_time: Utc.timestamp_opt(1_778_808_000, 0).unwrap(),
            source_addr: "tcp://127.0.0.1:1514".to_string(),
            raw: value.to_string(),
        }
    }

    #[test]
    fn discover_finds_sealed_and_open_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::create(dir.path(), "segment-a").unwrap();
        writer.append(raw("x")).unwrap();
        writer.seal().unwrap();

        let mut writer = SegmentWriter::create(dir.path(), "segment-b").unwrap();
        writer.append(raw("y")).unwrap();
        drop(writer);

        let segments = discover_uncommitted_segments(dir.path()).unwrap();
        assert_eq!(segments.len(), 2);
        assert!(segments[0].name.contains("segment-a"));
        assert!(segments[1].name.contains("segment-b"));
    }

    #[test]
    fn replay_segment_reads_records_after_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::create(dir.path(), "segment-replay").unwrap();
        writer.append(raw("a")).unwrap();
        writer.append(raw("b")).unwrap();
        writer.append(raw("c")).unwrap();
        let sealed = writer.seal().unwrap();

        let segment = ReplaySegment {
            path: sealed,
            name: "segment-replay.sealed".to_string(),
        };
        let records = replay_segment(
            &segment,
            SpoolCheckpoint {
                committed_offset: 1,
            },
        )
        .unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].raw, "b");
        assert_eq!(records[1].raw, "c");
    }

    #[test]
    fn rename_segment_to_committed_changes_extension() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::create(dir.path(), "segment-commit").unwrap();
        writer.append(raw("x")).unwrap();
        let sealed = writer.seal().unwrap();

        let segment = ReplaySegment {
            path: sealed.clone(),
            name: "segment-commit.sealed".to_string(),
        };
        let committed = rename_segment_to_committed(&segment).unwrap();

        assert!(!sealed.exists());
        assert!(committed.exists());
        assert!(committed.to_string_lossy().ends_with(".committed"));
    }

    #[test]
    fn cleanup_committed_segments_removes_committed_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::create(dir.path(), "segment-cleanup").unwrap();
        writer.append(raw("x")).unwrap();
        let sealed = writer.seal().unwrap();

        let segment = ReplaySegment {
            path: sealed,
            name: "segment-cleanup.sealed".to_string(),
        };
        rename_segment_to_committed(&segment).unwrap();

        let deleted = cleanup_committed_segments(dir.path()).unwrap();
        assert_eq!(deleted, 1);

        let segments = discover_uncommitted_segments(dir.path()).unwrap();
        assert_eq!(segments.len(), 0);
    }
}
