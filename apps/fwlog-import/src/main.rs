use std::{
    fs::{self, File},
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use flate2::read::GzDecoder;
use fwlog_adapter::ParserEngine;
use fwlog_domain::{CanonicalEvent, RawLog};
use fwlog_storage::DuckDbStore;
use rayon::prelude::*;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    input: Option<PathBuf>,
    #[arg(long)]
    duckdb: PathBuf,
    #[arg(long, default_value_t = 100_000)]
    batch_size: usize,
    #[arg(long)]
    compact_output: Option<PathBuf>,
    #[arg(long)]
    drop_parsed_raw: bool,
    #[arg(long)]
    hot_limit: Option<usize>,
    #[arg(long)]
    fast_hot_limit: Option<usize>,
    #[arg(long)]
    hot_days: Option<u32>,
    #[arg(long)]
    archive_parquet: Option<PathBuf>,
    #[arg(long)]
    archive_slim_parquet: Option<PathBuf>,
    #[arg(long)]
    slim_parquet_input: Option<PathBuf>,
    #[arg(long)]
    slim_parquet_output: Option<PathBuf>,
    #[arg(long)]
    reparse_existing: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let started = Instant::now();
    let store = DuckDbStore::open(&args.duckdb)?;

    if args.reparse_existing {
        let stats = store.event_stats()?;
        let empty_raw_rows = store.empty_raw_rows()?;
        if empty_raw_rows > 0 {
            anyhow::bail!(
                "refuse to reparse: {} events have empty raw payloads; restore frozen/raw source or compact without dropping raw first",
                empty_raw_rows
            );
        }
        let events = store.query_recent(stats.total as usize)?;
        let reparsed: Vec<CanonicalEvent> = events
            .into_par_iter()
            .map(|event| {
                let original_event_id = event.event_id;
                let mut reparsed = ParserEngine::new().parse(RawLog {
                    ingest_time: event.ingest_time,
                    source_addr: event.source_addr,
                    raw: event.raw,
                });
                reparsed.event_id = original_event_id;
                reparsed
            })
            .collect();
        let rows = store.replace_all_events(&reparsed, stats.total)?;
        eprintln!(
            "OxideLog reparse existing finished rows={} elapsed={:.1}s",
            rows,
            started.elapsed().as_secs_f64()
        );
        return Ok(());
    }

    if let (Some(input), Some(output)) = (
        args.slim_parquet_input.as_ref(),
        args.slim_parquet_output.as_ref(),
    ) {
        let archive = store.archive_slim_parquet_from_parquet(input, output)?;
        eprintln!(
            "OxideLog slim parquet finished input={} output={} bytes={} elapsed={:.1}s",
            input.display(),
            archive.path.display(),
            archive.bytes,
            started.elapsed().as_secs_f64()
        );
        return Ok(());
    }

    if let Some(output) = args.compact_output {
        if let Some(parquet) = args.archive_parquet.as_ref() {
            let stats = store.event_stats()?;
            let archive = store.archive_parquet(parquet, stats.total as usize)?;
            eprintln!(
                "OxideLog parquet archive finished output={} rows={} bytes={}",
                archive.path.display(),
                stats.total,
                archive.bytes
            );
        }
        if let Some(parquet) = args.archive_slim_parquet.as_ref() {
            let archive = store.archive_slim_parquet(parquet, None)?;
            eprintln!(
                "OxideLog slim parquet archive finished output={} bytes={}",
                archive.path.display(),
                archive.bytes
            );
        }
        let rows = if let Some(limit) = args.fast_hot_limit {
            store.compact_limit_to(&output, limit, args.drop_parsed_raw)?
        } else if let Some(limit) = args.hot_limit {
            store.compact_hot_to(&output, limit, args.drop_parsed_raw)?
        } else if let Some(days) = args.hot_days {
            store.compact_time_range_to(&output, days, args.drop_parsed_raw)?
        } else {
            store.compact_to(&output, args.drop_parsed_raw)?
        };
        eprintln!(
            "OxideLog compact finished output={} rows={} hot_limit={:?} fast_hot_limit={:?} hot_days={:?} drop_parsed_raw={} elapsed={:.1}s",
            output.display(),
            rows,
            args.hot_limit,
            args.fast_hot_limit,
            args.hot_days,
            args.drop_parsed_raw,
            started.elapsed().as_secs_f64()
        );
        return Ok(());
    }

    let input = args
        .input
        .as_deref()
        .context("--input is required unless --compact-output is used")?;
    let files = collect_files(input)?;
    let parser = ParserEngine::new();
    let mut total_lines = 0_u64;
    let mut total_files = 0_u64;

    for file in files {
        let file_started = Instant::now();
        let before = total_lines;

        let mut reader: Box<dyn BufRead> =
            if file.extension().and_then(|v| v.to_str()) == Some("gz") {
                let file = File::open(&file).with_context(|| format!("open {}", file.display()))?;
                Box::new(BufReader::new(GzDecoder::new(file)))
            } else {
                let file = File::open(&file).with_context(|| format!("open {}", file.display()))?;
                Box::new(BufReader::new(file))
            };

        let mut lines = Vec::with_capacity(args.batch_size);
        let mut buffer = Vec::new();
        loop {
            buffer.clear();
            let bytes = reader.read_until(b'\n', &mut buffer)?;
            if bytes == 0 {
                break;
            }
            while buffer
                .last()
                .is_some_and(|byte| *byte == b'\n' || *byte == b'\r')
            {
                buffer.pop();
            }
            if !buffer.is_empty() {
                lines.push(String::from_utf8_lossy(&buffer).into_owned());
            }
            if lines.len() >= args.batch_size {
                total_lines += flush_lines(&store, &parser, &file, &mut lines)? as u64;
            }
        }
        total_lines += flush_lines(&store, &parser, &file, &mut lines)? as u64;

        total_files += 1;
        eprintln!(
            "[import] file={} lines={} total={} elapsed={:.1}s",
            file.display(),
            total_lines - before,
            total_lines,
            file_started.elapsed().as_secs_f64()
        );
    }

    eprintln!(
        "OxideLog bulk import finished files={} lines={} elapsed={:.1}s",
        total_files,
        total_lines,
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

fn flush_lines(
    store: &DuckDbStore,
    parser: &ParserEngine,
    file: &Path,
    lines: &mut Vec<String>,
) -> Result<usize> {
    if lines.is_empty() {
        return Ok(0);
    }

    let line_count = lines.len();
    let source_addr = format!("file://{}", file.display());
    let events: Vec<_> = std::mem::take(lines)
        .into_par_iter()
        .map(|line| {
            let raw = RawLog {
                ingest_time: Utc::now(),
                source_addr: source_addr.clone(),
                raw: line,
            };
            parser.parse(raw)
        })
        .collect();

    store.append_events(&events)?;
    Ok(line_count)
}

fn collect_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_files_inner(root, &mut files)?;
    files.sort();
    Ok(files
        .into_iter()
        .filter(|path| {
            let name = path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("");
            let len = fs::metadata(path)
                .map(|metadata| metadata.len())
                .unwrap_or(0);
            name != "read me.txt" && len > 0 && !(name.ends_with(".gz") && len <= 20)
        })
        .collect())
}

fn collect_files_inner(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    if path.is_file() {
        files.push(path.to_path_buf());
        return Ok(());
    }
    for entry in fs::read_dir(path).with_context(|| format!("read dir {}", path.display()))? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_files_inner(&path, files)?;
        } else if path.is_file() {
            files.push(path);
        }
    }
    Ok(())
}
