mod pipeline;

use std::{fs, path::PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use serde::Deserialize;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    config: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub data: DataConfig,
    pub pipeline: PipelineConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub api_addr: String,
    pub tcp_addr: String,
    pub udp_addr: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DataConfig {
    pub root: PathBuf,
    pub duckdb_path: PathBuf,
    pub spool_dir: PathBuf,
    pub export_dir: PathBuf,
    pub parquet_dir: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PipelineConfig {
    pub ingress_queue: usize,
    pub batch_size: usize,
    pub flush_interval_ms: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "fwlogd=info,fwlog_api=info".into()),
        )
        .init();

    let args = Args::parse();
    let config = load_config(args.config)?;
    create_data_dirs(&config)?;
    pipeline::run(config).await
}

fn load_config(path: PathBuf) -> Result<Config> {
    let content = fs::read_to_string(&path)
        .with_context(|| format!("read config {}", path.display()))?;
    toml::from_str(&content).context("parse config toml")
}

fn create_data_dirs(config: &Config) -> Result<()> {
    fs::create_dir_all(&config.data.root).context("create data root")?;
    fs::create_dir_all(&config.data.spool_dir).context("create spool dir")?;
    fs::create_dir_all(&config.data.export_dir).context("create export dir")?;
    fs::create_dir_all(&config.data.parquet_dir).context("create parquet dir")?;
    if let Some(parent) = config.data.duckdb_path.parent() {
        fs::create_dir_all(parent).context("create duckdb dir")?;
    }
    Ok(())
}
