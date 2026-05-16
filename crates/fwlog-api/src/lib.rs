mod handlers;

use std::{path::PathBuf, sync::Arc};

use axum::{
    extract::Request,
    http::{header, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
    Extension, Router,
};
use fwlog_domain::RuntimeMetrics;

#[derive(Clone)]
pub struct ApiState {
    pub duckdb_path: Arc<PathBuf>,
    pub parquet_dir: Arc<PathBuf>,
    pub frozen_dir: Arc<PathBuf>,
    pub metrics: RuntimeMetrics,
    pub auth_enabled: bool,
}

pub fn router(duckdb_path: PathBuf, parquet_dir: PathBuf, frozen_dir: PathBuf) -> Router {
    router_with_options(duckdb_path, parquet_dir, frozen_dir, RuntimeMetrics::default(), None)
}

pub fn router_with_options(
    duckdb_path: PathBuf,
    parquet_dir: PathBuf,
    frozen_dir: PathBuf,
    metrics: RuntimeMetrics,
    api_token: Option<String>,
) -> Router {
    let state = ApiState {
        duckdb_path: Arc::new(duckdb_path),
        parquet_dir: Arc::new(parquet_dir),
        frozen_dir: Arc::new(frozen_dir),
        metrics,
        auth_enabled: api_token.as_deref().is_some_and(|token| !token.is_empty()),
    };

    Router::new()
        .route("/", get(handlers::app))
        .route("/app", get(handlers::app))
        .route("/api/health", get(handlers::health))
        .route("/api/system/status", get(handlers::system_status))
        .route("/api/events", get(handlers::events))
        .route("/api/events/export.csv", get(handlers::export_csv))
        .route("/api/archive/files", get(handlers::archive_files))
        .route("/api/archive/parquet", post(handlers::archive_parquet))
        .route(
            "/api/archive/frozen",
            get(handlers::archive_frozen_files).post(handlers::archive_frozen),
        )
        .route(
            "/api/archive/frozen/restore",
            get(handlers::restore_frozen),
        )
        .layer(middleware::from_fn(move |request, next| {
            require_api_token(request, next, api_token.clone())
        }))
        .layer(Extension(state))
}

async fn require_api_token(
    request: Request,
    next: Next,
    api_token: Option<String>,
) -> Result<Response, StatusCode> {
    let Some(expected) = api_token.filter(|token| !token.is_empty()) else {
        return Ok(next.run(request).await);
    };

    if request.uri().path() == "/" || request.uri().path() == "/app" {
        return Ok(next.run(request).await);
    }

    let authorized = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|token| token == expected);

    if authorized {
        Ok(next.run(request).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}
