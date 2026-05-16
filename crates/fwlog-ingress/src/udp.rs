use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use anyhow::{Context, Result};
use flume::Sender;
use fwlog_domain::{RawLog, RuntimeMetrics};
use tokio::{net::UdpSocket, task::JoinHandle};

#[derive(Clone, Default)]
pub struct UdpDropCounter {
    inner: Arc<AtomicU64>,
}

impl UdpDropCounter {
    pub fn increment(&self) {
        self.inner.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.inner.load(Ordering::Relaxed)
    }
}

pub async fn run_udp_listener(
    addr: String,
    sender: Sender<RawLog>,
    dropped: UdpDropCounter,
) -> Result<()> {
    let handle = start_udp_listener(addr, sender, dropped).await?;
    handle.await.context("udp listener task join")?
}

pub async fn start_udp_listener(
    addr: String,
    sender: Sender<RawLog>,
    dropped: UdpDropCounter,
) -> Result<JoinHandle<Result<()>>> {
    start_udp_listener_with_metrics(addr, sender, dropped, RuntimeMetrics::default()).await
}

pub async fn start_udp_listener_with_metrics(
    addr: String,
    sender: Sender<RawLog>,
    dropped: UdpDropCounter,
    metrics: RuntimeMetrics,
) -> Result<JoinHandle<Result<()>>> {
    let socket = UdpSocket::bind(&addr)
        .await
        .with_context(|| format!("bind udp listener {addr}"))?;
    Ok(tokio::spawn(serve_udp_listener(
        socket, sender, dropped, metrics,
    )))
}

async fn serve_udp_listener(
    socket: UdpSocket,
    sender: Sender<RawLog>,
    dropped: UdpDropCounter,
    metrics: RuntimeMetrics,
) -> Result<()> {
    let mut buf = vec![0_u8; 65_535];

    loop {
        let (len, peer) = socket.recv_from(&mut buf).await.context("receive udp datagram")?;
        metrics.inc_udp_received();
        let line = String::from_utf8_lossy(&buf[..len]).to_string();
        let raw = RawLog::new(format!("udp://{peer}"), line);
        if sender.try_send(raw).is_err() {
            dropped.increment();
            metrics.inc_udp_dropped();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn udp_listener_receives_one_datagram() {
        let (tx, rx) = flume::bounded(10);
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        let dropped = UdpDropCounter::default();
        let metrics = RuntimeMetrics::default();

        let handle = tokio::spawn(serve_udp_listener(
            socket,
            tx,
            dropped.clone(),
            metrics.clone(),
        ));
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.send_to(b"hello", addr).await.unwrap();

        let raw = rx.recv_async().await.unwrap();
        handle.abort();

        assert_eq!(raw.raw, "hello");
        assert_eq!(dropped.get(), 0);
        assert_eq!(metrics.snapshot().udp_received, 1);
        assert_eq!(metrics.snapshot().udp_dropped, 0);
    }

    #[test]
    fn udp_drop_counter_increments_when_queue_full_condition_is_reported() {
        let counter = UdpDropCounter::default();
        counter.increment();
        assert_eq!(counter.get(), 1);
    }
}
