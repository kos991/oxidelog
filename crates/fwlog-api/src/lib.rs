mod handlers;

use std::{path::PathBuf, sync::Arc};

use axum::{
    routing::{get, post, Router},
    Extension,
};

#[derive(Clone)]
pub struct ApiState {
    pub duckdb_path: Arc<PathBuf>,
    pub parquet_dir: Arc<PathBuf>,
}

pub fn router(duckdb_path: PathBuf, parquet_dir: PathBuf) -> Router {
    Router::new()
        .route("/api/health", get(handlers::health))
        .route("/api/events", get(handlers::events))
        .route("/api/events/export.csv", get(handlers::export_csv))
        .route("/api/archive/files", get(handlers::archive_files))
        .route("/api/archive/parquet", post(handlers::archive_parquet))
        .layer(Extension(ApiState {
            duckdb_path: Arc::new(duckdb_path),
            parquet_dir: Arc::new(parquet_dir),
        }))
}
