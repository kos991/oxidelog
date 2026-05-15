mod handlers;

use std::{path::PathBuf, sync::Arc};

use axum::{
    routing::{get, Router},
    Extension,
};

#[derive(Clone)]
pub struct ApiState {
    pub duckdb_path: Arc<PathBuf>,
}

pub fn router(duckdb_path: PathBuf) -> Router {
    Router::new()
        .route("/api/health", get(handlers::health))
        .route("/api/events", get(handlers::events))
        .route("/api/events/export.csv", get(handlers::export_csv))
        .layer(Extension(ApiState {
            duckdb_path: Arc::new(duckdb_path),
        }))
}
