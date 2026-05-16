use std::sync::atomic::{AtomicU64, Ordering};
use std::{path::PathBuf, thread, time::Duration};

use anyhow::{Context, Result};
use fwlog_adapter::{LogAdapter, SangforAdapter};
use fwlog_domain::RuntimeMetrics;
use fwlog_ingress::{
    start_tcp_listener_with_metrics, start_udp_listener_with_metrics, UdpDropCounter,
};
use fwlog_spool::SegmentWriter;
use fwlog_storage::{prune_archive_files, prune_frozen_files, write_frozen_raw, DuckDbStore};
use tracing::{error, info};

use crate::{ArchiveConfig, Config};

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

    if archive_config.enabled {
        let scheduler_duckdb_path = duckdb_path.clone();
        let scheduler_parquet_dir = parquet_dir.clone();
        let scheduler_frozen_dir = frozen_dir.clone();
        tokio::spawn(async move {
            run_archive_scheduler(
                scheduler_duckdb_path,
                scheduler_parquet_dir,
                scheduler_frozen_dir,
                archive_config,
            )
            .await;
        });
    }

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
    let adapter = SangforAdapter;
    let mut store = DuckDbStore::open(duckdb_path)?;
    let mut spool = SegmentWriter::create(spool_dir, "segment-local")?;
    let mut batch = Vec::with_capacity(batch_size);

    loop {
        match rx.recv_timeout(flush_interval) {
            Ok(raw) => {
                let _record = spool.append(raw.clone()).context("append raw log to spool")?;
                metrics.inc_spool_written();
                batch.push(adapter.parse(raw));
                if batch.len() >= batch_size {
                    flush(&mut store, &mut batch, &metrics)?;
                }
            }
            Err(flume::RecvTimeoutError::Timeout) => {
                flush(&mut store, &mut batch, &metrics)?;
            }
            Err(flume::RecvTimeoutError::Disconnected) => {
                flush(&mut store, &mut batch, &metrics)?;
                break;
            }
        }
    }

    Ok(())
}

fn flush(
    store: &mut DuckDbStore,
    batch: &mut Vec<fwlog_domain::CanonicalEvent>,
    metrics: &RuntimeMetrics,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let inserted = store.insert_batch(batch)?;
    metrics.add_events_stored(inserted as u64);
    metrics.inc_batches_stored();
    info!(inserted, "stored event batch");
    batch.clear();
    Ok(())
}

async fn run_archive_scheduler(
    duckdb_path: PathBuf,
    parquet_dir: PathBuf,
    frozen_dir: PathBuf,
    config: ArchiveConfig,
) {
    let interval = Duration::from_secs(config.interval_seconds.max(60));
    loop {
        tokio::time::sleep(interval).await;
        if let Err(err) = run_archive_cycle(&duckdb_path, &parquet_dir, &frozen_dir, &config) {
            error!(error = %err, "archive cycle failed");
        }
    }
}

fn run_archive_cycle(
    duckdb_path: &PathBuf,
    parquet_dir: &PathBuf,
    frozen_dir: &PathBuf,
    config: &ArchiveConfig,
) -> Result<()> {
    let store = DuckDbStore::open(duckdb_path)?;
    let stamp = archive_stamp();
    let parquet = parquet_dir.join(format!("events-{stamp}.parquet"));
    let events = store.query_recent(config.batch_limit.max(1))?;
    let parquet_file = store.archive_events_parquet(&parquet, &events)?;

    let raw_lines = events.into_iter().map(|event| event.raw).collect::<Vec<_>>();
    let frozen = frozen_dir.join(format!("frozen-{stamp}.raw.zst"));
    let frozen_file = write_frozen_raw(&frozen, &raw_lines)?;

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
        parquet_removed,
        frozen_removed,
        "archive cycle completed"
    );
    Ok(())
}

fn archive_stamp() -> String {
    let sequence = ARCHIVE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!(
        "{}-{:06}",
        chrono::Utc::now().format("%Y%m%d-%H%M%S%.6f"),
        sequence % 1_000_000
    )
}
