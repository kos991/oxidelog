mod handlers;
pub mod search;

use std::{path::PathBuf, sync::Arc};

use axum::{
    extract::Request,
    http::{header, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{get, post, put},
    Extension, Router,
};
use fwlog_domain::RuntimeMetrics;
use fwlog_storage::{
    run_storage_governor, DuckDbStore, GovernorArchiveConfig, GovernorConfig,
    GovernorLifecycleConfig, HybridStorage,
};
use tower_http::services::ServeDir;
use tracing::info;

#[derive(Clone)]
pub struct ApiState {
    pub duckdb_path: Arc<PathBuf>,
    pub parquet_dir: Arc<PathBuf>,
    pub frozen_dir: Arc<PathBuf>,
    pub metrics: RuntimeMetrics,
    pub auth_enabled: bool,
    pub hybrid_storage: Option<Arc<HybridStorage>>,
}

pub fn router(
    duckdb_path: PathBuf,
    parquet_dir: PathBuf,
    frozen_dir: PathBuf,
    hybrid_storage: Option<Arc<HybridStorage>>,
) -> Router {
    router_with_options(
        duckdb_path,
        parquet_dir,
        frozen_dir,
        RuntimeMetrics::default(),
        None,
        hybrid_storage,
    )
}

pub fn router_with_options(
    duckdb_path: PathBuf,
    parquet_dir: PathBuf,
    frozen_dir: PathBuf,
    metrics: RuntimeMetrics,
    api_token: Option<String>,
    hybrid_storage: Option<Arc<HybridStorage>>,
) -> Router {
    let state = ApiState {
        duckdb_path: Arc::new(duckdb_path),
        parquet_dir: Arc::new(parquet_dir),
        frozen_dir: Arc::new(frozen_dir),
        metrics,
        auth_enabled: api_token.as_deref().is_some_and(|token| !token.is_empty()),
        hybrid_storage,
    };
    let web_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../web");

    Router::new()
        .route("/", get(handlers::app))
        .route("/app", get(handlers::app))
        .route("/oxidelog", get(handlers::app))
        .route("/api/health", get(handlers::health))
        .route("/api/system/status", get(handlers::system_status))
        .route("/api/metrics/minutes", get(handlers::minute_metrics))
        .route("/api/metrics/hours", get(handlers::hour_metrics))
        .route("/api/metrics/sources", get(handlers::source_metrics))
        .route("/api/parser/summary", get(handlers::parser_summary))
        .route("/api/parser/profiles", get(handlers::parser_profiles))
        .route(
            "/api/parser/adaptive/rules",
            get(handlers::parser_adaptive_rules),
        )
        .route("/api/parser/diagnostics", get(handlers::parser_diagnostics))
        .route("/api/parser/scopes", get(handlers::parser_scopes))
        .route(
            "/api/devices",
            get(handlers::devices).post(handlers::create_device),
        )
        .route("/api/devices/backfill", post(handlers::backfill_devices))
        .route(
            "/api/devices/:id",
            put(handlers::update_device).delete(handlers::delete_device),
        )
        .route(
            "/api/ip/regions/custom",
            get(handlers::custom_ip_regions).post(handlers::create_custom_ip_region),
        )
        .route(
            "/api/ip/regions/custom/:id",
            put(handlers::update_custom_ip_region).delete(handlers::delete_custom_ip_region),
        )
        .route("/api/events", get(handlers::events))
        .route("/api/search", get(handlers::search))
        .route("/api/search/export.csv", get(handlers::search_export_csv))
        .route(
            "/api/export/jobs",
            get(handlers::export_jobs).post(handlers::create_export_job),
        )
        .route("/api/export/jobs/:id", get(handlers::export_job))
        .route(
            "/api/export/jobs/:id/download",
            get(handlers::download_export_job),
        )
        .route("/api/ip/region", get(handlers::ip_region))
        .route("/api/cold/search", get(handlers::cold_search))
        .route("/api/events/export.csv", get(handlers::export_csv))
        .route("/api/archive/index", get(handlers::archive_index))
        .route("/api/archive/days", get(handlers::archive_days))
        .route(
            "/api/archive/index/rebuild",
            post(handlers::rebuild_archive_index),
        )
        .route("/api/archive/files", get(handlers::archive_files))
        .route("/api/archive/parquet", post(handlers::archive_parquet))
        .route(
            "/api/archive/frozen",
            get(handlers::archive_frozen_files).post(handlers::archive_frozen),
        )
        .route("/api/archive/frozen/restore", get(handlers::restore_frozen))
        .route("/api/admission/cases", get(handlers::admission_cases))
        .route("/api/admission/profiles", get(handlers::admission_profiles))
        .route("/api/storage/health", get(handlers::storage_health))
        .route("/api/storage/stats", get(handlers::storage_stats))
        .fallback_service(ServeDir::new(web_dir).append_index_html_on_directories(true))
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

    if !request.uri().path().starts_with("/api/") {
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
