use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use serde::Serialize;

#[derive(Debug, Clone, Default)]
pub struct RuntimeMetrics {
    inner: Arc<MetricsInner>,
}

#[derive(Debug, Default)]
struct MetricsInner {
    tcp_received: AtomicU64,
    udp_received: AtomicU64,
    udp_dropped: AtomicU64,
    spool_written: AtomicU64,
    spool_replayed: AtomicU64,
    events_stored: AtomicU64,
    batches_stored: AtomicU64,
    worker_errors: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MetricsSnapshot {
    pub tcp_received: u64,
    pub udp_received: u64,
    pub udp_dropped: u64,
    pub spool_written: u64,
    pub spool_replayed: u64,
    pub events_stored: u64,
    pub batches_stored: u64,
    pub worker_errors: u64,
}

impl RuntimeMetrics {
    pub fn inc_tcp_received(&self) {
        self.inner.tcp_received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_udp_received(&self) {
        self.inner.udp_received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_udp_dropped(&self) {
        self.inner.udp_dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_spool_written(&self) {
        self.inner.spool_written.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_spool_replayed(&self, value: u64) {
        self.inner
            .spool_replayed
            .fetch_add(value, Ordering::Relaxed);
    }

    pub fn add_events_stored(&self, value: u64) {
        self.inner.events_stored.fetch_add(value, Ordering::Relaxed);
    }

    pub fn inc_batches_stored(&self) {
        self.inner.batches_stored.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_worker_errors(&self) {
        self.inner.worker_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            tcp_received: self.inner.tcp_received.load(Ordering::Relaxed),
            udp_received: self.inner.udp_received.load(Ordering::Relaxed),
            udp_dropped: self.inner.udp_dropped.load(Ordering::Relaxed),
            spool_written: self.inner.spool_written.load(Ordering::Relaxed),
            spool_replayed: self.inner.spool_replayed.load(Ordering::Relaxed),
            events_stored: self.inner.events_stored.load(Ordering::Relaxed),
            batches_stored: self.inner.batches_stored.load(Ordering::Relaxed),
            worker_errors: self.inner.worker_errors.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_reports_incremented_counters() {
        let metrics = RuntimeMetrics::default();

        metrics.inc_tcp_received();
        metrics.inc_udp_received();
        metrics.inc_udp_dropped();
        metrics.inc_spool_written();
        metrics.add_events_stored(3);
        metrics.inc_batches_stored();
        metrics.inc_worker_errors();

        assert_eq!(
            metrics.snapshot(),
            MetricsSnapshot {
                tcp_received: 1,
                udp_received: 1,
                udp_dropped: 1,
                spool_written: 1,
                spool_replayed: 0,
                events_stored: 3,
                batches_stored: 1,
                worker_errors: 1
            }
        );
    }
}
