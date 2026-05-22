use anyhow::{Context, Result};
use flume::Sender;
use futures::StreamExt;
use fwlog_domain::{RawLog, RuntimeMetrics};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::codec::{FramedRead, LinesCodec};

pub async fn run_tcp_listener(addr: String, sender: Sender<RawLog>) -> Result<()> {
    let handle = start_tcp_listener(addr, sender).await?;
    handle.await.context("tcp listener task join")?
}

pub async fn start_tcp_listener(
    addr: String,
    sender: Sender<RawLog>,
) -> Result<JoinHandle<Result<()>>> {
    start_tcp_listener_with_metrics(addr, sender, RuntimeMetrics::default()).await
}

pub async fn start_tcp_listener_with_metrics(
    addr: String,
    sender: Sender<RawLog>,
    metrics: RuntimeMetrics,
) -> Result<JoinHandle<Result<()>>> {
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("bind tcp listener {addr}"))?;
    Ok(tokio::spawn(serve_tcp_listener(listener, sender, metrics)))
}

async fn serve_tcp_listener(
    listener: TcpListener,
    sender: Sender<RawLog>,
    metrics: RuntimeMetrics,
) -> Result<()> {
    loop {
        let (stream, peer) = listener.accept().await.context("accept tcp connection")?;
        let sender = sender.clone();
        let metrics = metrics.clone();
        tokio::spawn(async move {
            let mut lines = FramedRead::new(stream, LinesCodec::new());
            while let Some(line) = lines.next().await {
                match line {
                    Ok(line) => {
                        metrics.inc_tcp_received();
                        let raw = RawLog::new(format!("tcp://{peer}"), line);
                        if sender.send_async(raw).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{io::AsyncWriteExt, net::TcpStream};

    #[tokio::test]
    async fn tcp_listener_receives_two_newline_delimited_logs() {
        let (tx, rx) = flume::bounded(10);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let metrics = RuntimeMetrics::default();
        let handle = tokio::spawn(serve_tcp_listener(listener, tx, metrics.clone()));
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"first\nsecond\n").await.unwrap();

        let first = rx.recv_async().await.unwrap();
        let second = rx.recv_async().await.unwrap();
        handle.abort();

        assert_eq!(first.raw, "first");
        assert_eq!(second.raw, "second");
        assert!(first.source_addr.starts_with("tcp://"));
        assert_eq!(metrics.snapshot().tcp_received, 2);
    }
}
