mod pipeline;
mod replay;

use std::{fs, path::PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use fwlog_adapter::AdaptiveLearningConfig;
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
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub archive: ArchiveConfig,
    #[serde(default)]
    pub lifecycle: LifecycleConfig,
    #[serde(default)]
    pub adaptive_learning: AdaptiveLearningConfig,
    #[serde(default)]
    pub dual_db: fwlog_storage::DualDbConfig,
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
    pub frozen_dir: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PipelineConfig {
    pub ingress_queue: usize,
    pub batch_size: usize,
    pub flush_interval_ms: u64,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct AuthConfig {
    pub api_token: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ArchiveConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_archive_interval_seconds")]
    pub interval_seconds: u64,
    #[serde(default = "default_archive_limit")]
    pub batch_limit: usize,
    #[serde(default = "default_parquet_retention_days")]
    pub parquet_retention_days: u64,
    #[serde(default = "default_frozen_retention_days")]
    pub frozen_retention_days: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LifecycleConfig {
    #[serde(default = "default_lifecycle_enabled")]
    pub enabled: bool,
    #[serde(default = "default_hot_limit")]
    pub hot_limit: usize,
    #[serde(default = "default_lifecycle_interval_seconds")]
    pub interval_seconds: u64,
    #[serde(default = "default_drop_parsed_raw")]
    pub drop_parsed_raw: bool,
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_seconds: default_archive_interval_seconds(),
            batch_limit: default_archive_limit(),
            parquet_retention_days: default_parquet_retention_days(),
            frozen_retention_days: default_frozen_retention_days(),
        }
    }
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            enabled: default_lifecycle_enabled(),
            hot_limit: default_hot_limit(),
            interval_seconds: default_lifecycle_interval_seconds(),
            drop_parsed_raw: default_drop_parsed_raw(),
        }
    }
}

fn default_archive_interval_seconds() -> u64 {
    86_400
}

fn default_archive_limit() -> usize {
    100_000
}

fn default_parquet_retention_days() -> u64 {
    180
}

fn default_frozen_retention_days() -> u64 {
    365
}

fn default_lifecycle_enabled() -> bool {
    true
}

fn default_hot_limit() -> usize {
    100_000
}

fn default_lifecycle_interval_seconds() -> u64 {
    86_400
}

fn default_drop_parsed_raw() -> bool {
    true
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
    let content =
        fs::read_to_string(&path).with_context(|| format!("read config {}", path.display()))?;
    let mut config: Config = toml::from_str(&content).context("parse config toml")?;
    if let Ok(token) = std::env::var("OXIDELOG_API_TOKEN") {
        if !token.is_empty() {
            config.auth.api_token = Some(token);
        }
    }
    Ok(config)
}

fn create_data_dirs(config: &Config) -> Result<()> {
    fs::create_dir_all(&config.data.root).context("create data root")?;
    fs::create_dir_all(&config.data.spool_dir).context("create spool dir")?;
    fs::create_dir_all(&config.data.export_dir).context("create export dir")?;
    fs::create_dir_all(&config.data.parquet_dir).context("create parquet dir")?;
    fs::create_dir_all(&config.data.frozen_dir).context("create frozen dir")?;
    if let Some(parent) = config.data.duckdb_path.parent() {
        fs::create_dir_all(parent).context("create duckdb dir")?;
    }
    Ok(())
}
