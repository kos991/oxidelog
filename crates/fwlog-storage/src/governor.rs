use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use tracing::{error, info};

use crate::{prune_archive_files, prune_frozen_files, run_lifecycle_to, write_frozen_raw, DuckDbStore};

#[derive(Debug, Clone)]
pub struct GovernorConfig {
    pub archive: ArchiveConfig,
    pub lifecycle: LifecycleConfig,
}

#[derive(Debug, Clone)]
pub struct ArchiveConfig {
    pub enabled: bool,
    pub interval_seconds: u64,
    pub batch_limit: usize,
    pub parquet_retention_days: u64,
    pub frozen_retention_days: u64,
}

#[derive(Debug, Clone)]
pub struct LifecycleConfig {
    pub enabled: bool,
    pub hot_limit: usize,
    pub interval_seconds: u64,
    pub drop_parsed_raw: bool,
}

#[derive(Debug, Clone)]
pub struct GovernorCycleReport {
    pub archive_completed: bool,
    pub archive_events: usize,
    pub archive_parquet_removed: usize,
    pub archive_frozen_removed: usize,
    pub lifecycle_completed: bool,
    pub lifecycle_compacted_rows: usize,
}

/// Unified storage governance coordinator that serializes archive and lifecycle operations
/// to prevent race conditions when accessing DuckDB files.
pub async fn run_storage_governor(
    duckdb_path: PathBuf,
    parquet_dir: PathBuf,
    frozen_dir: PathBuf,
    config: GovernorConfig,
) {
    let archive_interval = Duration::from_secs(config.archive.interval_seconds.max(60));
    let lifecycle_interval = Duration::from_secs(config.lifecycle.interval_seconds.max(60));

    // Use the shorter interval as the base tick rate
    let tick_interval = archive_interval.min(lifecycle_interval);

    let mut archive_ticks = 0u64;
    let mut lifecycle_ticks = 0u64;
    let archive_tick_threshold = (config.archive.interval_seconds / tick_interval.as_secs()).max(1);
    let lifecycle_tick_threshold = (config.lifecycle.interval_seconds / tick_interval.as_secs()).max(1);

    info!(
        archive_enabled = config.archive.enabled,
        lifecycle_enabled = config.lifecycle.enabled,
        tick_interval_secs = tick_interval.as_secs(),
        "storage governor started"
    );

    // Run initial cycle if archive is enabled
    if config.archive.enabled {
        if let Err(err) = run_governance_cycle(
            &duckdb_path,
            &parquet_dir,
            &frozen_dir,
            &config,
            true,
            false,
        ) {
            error!(error = %err, "initial archive cycle failed");
        }
    }

    loop {
        tokio::time::sleep(tick_interval).await;

        archive_ticks += 1;
        lifecycle_ticks += 1;

        let should_archive = config.archive.enabled && archive_ticks >= archive_tick_threshold;
        let should_lifecycle = config.lifecycle.enabled && lifecycle_ticks >= lifecycle_tick_threshold;

        if should_archive || should_lifecycle {
            if let Err(err) = run_governance_cycle(
                &duckdb_path,
                &parquet_dir,
                &frozen_dir,
                &config,
                should_archive,
                should_lifecycle,
            ) {
                error!(error = %err, "governance cycle failed");
            }

            if should_archive {
                archive_ticks = 0;
            }
            if should_lifecycle {
                lifecycle_ticks = 0;
            }
        }
    }
}

fn run_governance_cycle(
    duckdb_path: &Path,
    parquet_dir: &Path,
    frozen_dir: &Path,
    config: &GovernorConfig,
    run_archive: bool,
    run_lifecycle: bool,
) -> Result<GovernorCycleReport> {
    let mut report = GovernorCycleReport {
        archive_completed: false,
        archive_events: 0,
        archive_parquet_removed: 0,
        archive_frozen_removed: 0,
        lifecycle_completed: false,
        lifecycle_compacted_rows: 0,
    };

    // Step 1: Archive (if enabled and scheduled)
    // This must run BEFORE lifecycle to avoid reading from a file being replaced
    if run_archive {
        match run_archive_phase(duckdb_path, parquet_dir, frozen_dir, &config.archive) {
            Ok(archive_report) => {
                report.archive_completed = true;
                report.archive_events = archive_report.events_archived;
                report.archive_parquet_removed = archive_report.parquet_removed;
                report.archive_frozen_removed = archive_report.frozen_removed;
                info!(
                    events = archive_report.events_archived,
                    parquet_removed = archive_report.parquet_removed,
                    frozen_removed = archive_report.frozen_removed,
                    "archive phase completed"
                );
            }
            Err(err) => {
                error!(error = %err, "archive phase failed");
            }
        }
    }

    // Step 2: Lifecycle compaction (if enabled and scheduled)
    // This runs AFTER archive to ensure archive has finished reading
    if run_lifecycle {
        let stamp = archive_stamp();
        let output_path = duckdb_path.with_file_name(format!("oxidelog-hot-{stamp}.duckdb"));
        match run_lifecycle_to(
            duckdb_path,
            &output_path,
            config.lifecycle.hot_limit.max(1),
            config.lifecycle.drop_parsed_raw,
        ) {
            Ok(lifecycle_report) => {
                report.lifecycle_completed = true;
                report.lifecycle_compacted_rows = lifecycle_report.compacted_rows;
                info!(
                    hot_limit = lifecycle_report.hot_limit,
                    compacted_rows = lifecycle_report.compacted_rows,
                    pruned_raw_rows = lifecycle_report.pruned_raw_rows,
                    output_path = %lifecycle_report.output_path.display(),
                    "lifecycle phase completed"
                );
            }
            Err(err) => {
                error!(error = %err, "lifecycle phase failed");
            }
        }
    }

    Ok(report)
}

#[derive(Debug)]
struct ArchivePhaseReport {
    events_archived: usize,
    parquet_removed: usize,
    frozen_removed: usize,
}

fn run_archive_phase(
    duckdb_path: &Path,
    parquet_dir: &Path,
    frozen_dir: &Path,
    config: &ArchiveConfig,
) -> Result<ArchivePhaseReport> {
    let store = DuckDbStore::open(duckdb_path)?;
    let stamp = archive_stamp();
    let parquet = parquet_dir.join(format!("events-{stamp}.parquet"));
    let events = store.query_recent(config.batch_limit.max(1))?;
    let events_count = events.len();
    let parquet_file = store.archive_events_parquet(&parquet, &events)?;

    let raw_lines = events
        .into_iter()
        .map(|event| event.raw)
        .collect::<Vec<_>>();
    let frozen = frozen_dir.join(format!("frozen-{stamp}.raw.zst"));
    let frozen_file = write_frozen_raw(&frozen, &raw_lines)?;
    let frozen_index_path = frozen_file
        .path
        .strip_prefix(frozen_dir)
        .unwrap_or(frozen_file.path.as_path())
        .to_string_lossy()
        .trim_start_matches(|ch| ch == '/' || ch == '\\')
        .to_string();
    let day = chrono::Utc::now().format("%Y-%m-%d").to_string();
    store.upsert_frozen_archive_index_with_times(
        &frozen_index_path,
        &day,
        &format!("frozen://{frozen_index_path}"),
        frozen_file.bytes,
        raw_lines.len() as u64,
        Some(&format!("{day}T00:00:00Z")),
        Some(&format!("{day}T23:59:59Z")),
    )?;

    let parquet_removed = prune_archive_files(
        parquet_dir,
        Duration::from_secs(config.parquet_retention_days.max(1) * 24 * 3600),
    )?;
    let frozen_removed = prune_frozen_files(
        frozen_dir,
        Duration::from_secs(config.frozen_retention_days.max(1) * 24 * 3600),
    )?;

    info!(
        parquet_path = %parquet_file.path.display(),
        frozen_path = %frozen_file.path.display(),
        events = events_count,
        "archive files written"
    );

    Ok(ArchivePhaseReport {
        events_archived: events_count,
        parquet_removed,
        frozen_removed,
    })
}

fn archive_stamp() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static ARCHIVE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let sequence = ARCHIVE_SEQUENCE.fetch_add(1, Ordering::SeqCst);
    format!(
        "{}-{:06}",
        chrono::Utc::now().format("%Y%m%d-%H%M%S%.6f"),
        sequence % 1_000_000
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archive_stamp_is_unique_and_sortable() {
        let stamp1 = archive_stamp();
        let stamp2 = archive_stamp();
        assert_ne!(stamp1, stamp2);
        assert!(stamp1 < stamp2);
    }
}
