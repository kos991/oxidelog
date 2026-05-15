use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use fwlog_domain::RawLog;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpoolRecord {
    pub offset: u64,
    pub ingest_time: DateTime<Utc>,
    pub source_addr: String,
    pub raw: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SpoolCheckpoint {
    pub committed_offset: u64,
}

pub struct SegmentWriter {
    open_path: PathBuf,
    sealed_path: PathBuf,
    next_offset: u64,
    writer: BufWriter<File>,
}

impl SegmentWriter {
    pub fn create(dir: impl AsRef<Path>, name: &str) -> Result<Self> {
        fs::create_dir_all(dir.as_ref()).context("create spool directory")?;
        let open_path = dir.as_ref().join(format!("{name}.open"));
        let sealed_path = dir.as_ref().join(format!("{name}.sealed"));
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&open_path)
            .with_context(|| format!("open spool segment {}", open_path.display()))?;

        Ok(Self {
            open_path,
            sealed_path,
            next_offset: 1,
            writer: BufWriter::new(file),
        })
    }

    pub fn append(&mut self, raw: RawLog) -> Result<SpoolRecord> {
        let record = SpoolRecord {
            offset: self.next_offset,
            ingest_time: raw.ingest_time,
            source_addr: raw.source_addr,
            raw: raw.raw,
        };
        self.next_offset += 1;
        serde_json::to_writer(&mut self.writer, &record).context("serialize spool record")?;
        self.writer.write_all(b"\n").context("write spool newline")?;
        Ok(record)
    }

    pub fn seal(mut self) -> Result<PathBuf> {
        self.writer.flush().context("flush spool segment")?;
        drop(self.writer);
        fs::rename(&self.open_path, &self.sealed_path).with_context(|| {
            format!(
                "rename {} to {}",
                self.open_path.display(),
                self.sealed_path.display()
            )
        })?;
        Ok(self.sealed_path)
    }
}

pub struct SegmentReader {
    path: PathBuf,
}

impl SegmentReader {
    pub fn open(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn read_after(&self, checkpoint: SpoolCheckpoint) -> Result<Vec<SpoolRecord>> {
        let file = File::open(&self.path)
            .with_context(|| format!("open sealed segment {}", self.path.display()))?;
        let reader = BufReader::new(file);
        let mut records = Vec::new();

        for line in reader.lines() {
            let line = line.context("read spool line")?;
            let record: SpoolRecord = serde_json::from_str(&line).context("parse spool line")?;
            if record.offset > checkpoint.committed_offset {
                records.push(record);
            }
        }

        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn raw(value: &str) -> RawLog {
        RawLog {
            ingest_time: Utc.timestamp_opt(1_778_808_000, 0).unwrap(),
            source_addr: "tcp://127.0.0.1:1514".to_string(),
            raw: value.to_string(),
        }
    }

    #[test]
    fn appending_three_raw_logs_writes_three_jsonl_records() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::create(dir.path(), "segment-20260515-000000-000001").unwrap();

        writer.append(raw("a")).unwrap();
        writer.append(raw("b")).unwrap();
        writer.append(raw("c")).unwrap();
        let sealed = writer.seal().unwrap();

        let records = SegmentReader::open(sealed)
            .read_after(SpoolCheckpoint::default())
            .unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].offset, 1);
        assert_eq!(records[2].raw, "c");
    }

    #[test]
    fn checkpoint_after_line_two_replays_only_line_three() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::create(dir.path(), "segment-20260515-000000-000002").unwrap();

        writer.append(raw("a")).unwrap();
        writer.append(raw("b")).unwrap();
        writer.append(raw("c")).unwrap();
        let sealed = writer.seal().unwrap();

        let records = SegmentReader::open(sealed)
            .read_after(SpoolCheckpoint { committed_offset: 2 })
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].raw, "c");
    }
}
