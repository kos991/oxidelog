use std::{path::PathBuf, thread, time::Duration};

use anyhow::{Context, Result};
use fwlog_adapter::{LogAdapter, SangforAdapter};
use fwlog_domain::RuntimeMetrics;
use fwlog_ingress::{
    start_tcp_listener_with_metrics, start_udp_listener_with_metrics, UdpDropCounter,
};
use fwlog_spool::SegmentWriter;
use fwlog_storage::{
    run_storage_governor, DuckDbStore, GovernorArchiveConfig, GovernorConfig,
    GovernorLifecycleConfig,
};
use tracing::{error, info};

use crate::{replay::replay_spool_on_startup, ArchiveConfig, Config, LifecycleConfig};

static ARCHIVE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub async fn run(config: Config) -> Result<()> {
    let (tx, rx) = flume::bounded(config.pipeline.ingress_queue);
    let tcp_addr = config.server.tcp_addr.clone();
    let udp_addr = config.server.udp_addr.clone();
    let api_addr = config.server.api_addr.clone();
    let duckdb_path = config.data.duckdb_path.clone();
    let parquet_dir = config.data.parquet_dir.clone();
    let frozen_dir = config.data.frozen_dir.clone();
    let spool_dir = config.data.spool_dir.clone();
    let batch_size = config.pipeline.batch_size.max(1);
    let flush_interval = Duration::from_millis(config.pipeline.flush_interval_ms.max(100));
    let metrics = RuntimeMetrics::default();
    let api_token = config.auth.api_token.clone().filter(|token| !token.is_empty());
    let archive_config = config.archive.clone();
    let lifecycle_config = config.lifecycle.clone();

    let mut api_store = DuckDbStore::open(&duckdb_path)?;
    api_store.insert_batch(&[])?;
    drop(api_store);

    let worker_duckdb_path = duckdb_path.clone();
    let worker_metrics = metrics.clone();
    thread::spawn(move || {
        if let Err(err) = run_worker(
            rx,
            worker_duckdb_path,
            spool_dir,
            batch_size,
            flush_interval,
            worker_metrics.clone(),
        ) {
            worker_metrics.inc_worker_errors();
            error!(error = %err, "pipeline worker stopped");
        }
    });

    // Start unified storage governor (handles both archive and lifecycle)
    if archive_config.enabled || lifecycle_config.enabled {
        let governor_duckdb_path = duckdb_path.clone();
        let governor_parquet_dir = parquet_dir.clone();
        let governor_frozen_dir = frozen_dir.clone();
        tokio::spawn(async move {
            run_storage_governor(
                governor_duckdb_path,
                governor_parquet_dir,
                governor_frozen_dir,
                GovernorConfig {
                    archive: GovernorArchiveConfig {
                        enabled: archive_config.enabled,
                        interval_seconds: archive_config.interval_seconds,
                        batch_limit: archive_config.batch_limit,
                        parquet_retention_days: archive_config.parquet_retention_days,
                        frozen_retention_days: archive_config.frozen_retention_days,
                    },
                    lifecycle: GovernorLifecycleConfig {
                        enabled: lifecycle_config.enabled,
                        hot_limit: lifecycle_config.hot_limit,
                        interval_seconds: lifecycle_config.interval_seconds,
                        drop_parsed_raw: lifecycle_config.drop_parsed_raw,
                    },
                },
            )
            .await;
        });
    }

    // Spool cleanup scheduler
    let cleanup_spool_dir = spool_dir.clone();
    tokio::spawn(async move {
        run_spool_cleanup_scheduler(cleanup_spool_dir).await;
    });

    let _tcp_listener =
        start_tcp_listener_with_metrics(tcp_addr.clone(), tx.clone(), metrics.clone()).await?;
    let _udp_listener = start_udp_listener_with_metrics(
        udp_addr.clone(),
        tx,
        UdpDropCounter::default(),
        metrics.clone(),
    )
    .await?;

    let app = fwlog_api::router_with_options(duckdb_path, parquet_dir, frozen_dir, metrics, api_token);
    let listener = tokio::net::TcpListener::bind(&api_addr)
        .await
        .with_context(|| format!("bind api listener {api_addr}"))?;

    info!(
        "fwlogd listening api={} tcp={} udp={}",
        api_addr, tcp_addr, udp_addr
    );
    axum::serve(listener, app).await.context("serve api")
}

fn run_worker(
    rx: flume::Receiver<fwlog_domain::RawLog>,
    duckdb_path: std::path::PathBuf,
    spool_dir: std::path::PathBuf,
    batch_size: usize,
    flush_interval: Duration,
    metrics: RuntimeMetrics,
) -> Result<()> {
    use rayon::prelude::*;

    let adapter = SangforAdapter;
    let mut store = DuckDbStore::open(&duckdb_path)?;

    // Replay spool on startup for crash recovery
    match replay_spool_on_startup(
        spool_dir.clone(),
        duckdb_path.clone(),
        &adapter,
        &metrics,
    ) {
        Ok(report) => {
            if report.segments_found > 0 {
                info!(
                    segments_found = report.segments_found,
                    segments_replayed = report.segments_replayed,
                    records_replayed = report.records_replayed,
                    events_stored = report.events_stored,
                    segments_failed = report.segments_failed,
                    "spool replay completed"
                );
            }
        }
        Err(err) => {
            error!(error = %err, "spool replay failed, continuing with normal operation");
            metrics.inc_worker_errors();
        }
    }

    let mut spool = SegmentWriter::create(spool_dir, "segment-local")?;
    let mut raw_batch = Vec::with_capacity(batch_size);

    loop {
        match rx.recv_timeout(flush_interval) {
            Ok(raw) => {
                let _record = spool.append(raw.clone()).context("append raw log to spool")?;
                metrics.inc_spool_written();
                raw_batch.push(raw);
                if raw_batch.len() >= batch_size {
                    flush_parallel(&adapter, &mut store, &mut raw_batch, &metrics)?;
                }
            }
            Err(flume::RecvTimeoutError::Timeout) => {
                flush_parallel(&adapter, &mut store, &mut raw_batch, &metrics)?;
            }
            Err(flume::RecvTimeoutError::Disconnected) => {
                flush_parallel(&adapter, &mut store, &mut raw_batch, &metrics)?;
                break;
            }
        }
    }

    Ok(())
}

fn flush_parallel(
    adapter: &SangforAdapter,
    store: &mut DuckDbStore,
    raw_batch: &mut Vec<fwlog_domain::RawLog>,
    metrics: &RuntimeMetrics,
) -> Result<()> {
    use rayon::prelude::*;

    if raw_batch.is_empty() {
        return Ok(());
    }

    let parse_start = std::time::Instant::now();

    // Parallel parsing
    let events: Vec<_> = raw_batch
        .par_iter()
        .map(|raw| adapter.parse(raw.clone()))
        .collect();

    let parse_elapsed = parse_start.elapsed();

    // Sequential write to DuckDB
    let write_start = std::time::Instant::now();
    let inserted = store.insert_batch(&events)?;
    let write_elapsed = write_start.elapsed();

    metrics.add_events_written(inserted);

    info!(
        "flushed batch: parsed={} inserted={} parse_ms={:.1} write_ms={:.1} total_ms={:.1}",
        raw_batch.len(),
        inserted,
        parse_elapsed.as_secs_f64() * 1000.0,
        write_elapsed.as_secs_f64() * 1000.0,
        (parse_elapsed + write_elapsed).as_secs_f64() * 1000.0
    );

    raw_batch.clear();
    Ok(())
}

async fn run_spool_cleanup_scheduler(spool_dir: PathBuf) {
    let interval = Duration::from_secs(3600);
    loop {
        tokio::time::sleep(interval).await;
        match fwlog_spool::cleanup_committed_segments(&spool_dir) {
            Ok(deleted) => {
                if deleted > 0 {
                    info!(deleted, "cleaned up committed spool segments");
                }
            }
            Err(err) => {
                error!(error = %err, "spool cleanup failed");
            }
        }
    }
}
