use std::{thread, time::Duration};

use anyhow::{Context, Result};
use fwlog_adapter::{LogAdapter, SangforAdapter};
use fwlog_ingress::{start_tcp_listener, start_udp_listener, UdpDropCounter};
use fwlog_spool::SegmentWriter;
use fwlog_storage::DuckDbStore;
use tracing::{error, info};

use crate::Config;

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

    let mut api_store = DuckDbStore::open(&duckdb_path)?;
    api_store.insert_batch(&[])?;
    drop(api_store);

    let worker_duckdb_path = duckdb_path.clone();
    thread::spawn(move || {
        if let Err(err) = run_worker(rx, worker_duckdb_path, spool_dir, batch_size, flush_interval) {
            error!(error = %err, "pipeline worker stopped");
        }
    });

    let _tcp_listener = start_tcp_listener(tcp_addr.clone(), tx.clone()).await?;
    let _udp_listener = start_udp_listener(udp_addr.clone(), tx, UdpDropCounter::default()).await?;

    let app = fwlog_api::router(duckdb_path, parquet_dir, frozen_dir);
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
) -> Result<()> {
    let adapter = SangforAdapter;
    let mut store = DuckDbStore::open(duckdb_path)?;
    let mut spool = SegmentWriter::create(spool_dir, "segment-local")?;
    let mut batch = Vec::with_capacity(batch_size);

    loop {
        match rx.recv_timeout(flush_interval) {
            Ok(raw) => {
                let _record = spool.append(raw.clone()).context("append raw log to spool")?;
                batch.push(adapter.parse(raw));
                if batch.len() >= batch_size {
                    flush(&mut store, &mut batch)?;
                }
            }
            Err(flume::RecvTimeoutError::Timeout) => {
                flush(&mut store, &mut batch)?;
            }
            Err(flume::RecvTimeoutError::Disconnected) => {
                flush(&mut store, &mut batch)?;
                break;
            }
        }
    }

    Ok(())
}

fn flush(store: &mut DuckDbStore, batch: &mut Vec<fwlog_domain::CanonicalEvent>) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let inserted = store.insert_batch(batch)?;
    info!(inserted, "stored event batch");
    batch.clear();
    Ok(())
}
