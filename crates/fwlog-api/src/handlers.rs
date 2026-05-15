use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context};
use axum::{
    extract::{Extension, Query},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use chrono::Utc;
use fwlog_storage::{
    list_archive_files, list_frozen_files, read_frozen_raw, write_frozen_raw, ArchiveFile,
    DuckDbStore, FrozenFile,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::ApiState;

#[derive(Debug, Deserialize)]
pub struct LimitQuery {
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(Debug, Serialize)]
struct ArchiveFileJson {
    path: PathBuf,
    bytes: u64,
}

impl From<ArchiveFile> for ArchiveFileJson {
    fn from(file: ArchiveFile) -> Self {
        Self {
            path: file.path,
            bytes: file.bytes,
        }
    }
}

impl From<FrozenFile> for ArchiveFileJson {
    fn from(file: FrozenFile) -> Self {
        Self {
            path: file.path,
            bytes: file.bytes,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct RestoreQuery {
    path: PathBuf,
}

#[derive(Debug, Serialize)]
struct SystemStatus {
    service: &'static str,
    duckdb_path: PathBuf,
    parquet_dir: PathBuf,
    frozen_dir: PathBuf,
    duckdb_bytes: u64,
    parquet_files: usize,
    parquet_bytes: u64,
    frozen_files: usize,
    frozen_bytes: u64,
}

fn default_limit() -> usize {
    20
}

pub async fn health() -> Json<serde_json::Value> {
    Json(json!({"status":"ok","service":"fwlogd"}))
}

pub async fn system_status(Extension(state): Extension<ApiState>) -> Response {
    let duckdb_bytes = match fs::metadata(&*state.duckdb_path) {
        Ok(metadata) if metadata.is_file() => metadata.len(),
        Ok(_) => 0,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => 0,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };

    let parquet_files = match archive_stats(state.parquet_dir.as_ref().as_path()) {
        Ok(stats) => stats,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };
    let frozen_files = match frozen_stats(state.frozen_dir.as_ref().as_path()) {
        Ok(stats) => stats,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };

    Json(SystemStatus {
        service: "fwlogd",
        duckdb_path: state.duckdb_path.as_ref().clone(),
        parquet_dir: state.parquet_dir.as_ref().clone(),
        frozen_dir: state.frozen_dir.as_ref().clone(),
        duckdb_bytes,
        parquet_files: parquet_files.0,
        parquet_bytes: parquet_files.1,
        frozen_files: frozen_files.0,
        frozen_bytes: frozen_files.1,
    })
    .into_response()
}

pub async fn events(
    Extension(state): Extension<ApiState>,
    Query(query): Query<LimitQuery>,
) -> Response {
    match DuckDbStore::open(&*state.duckdb_path).and_then(|store| store.query_recent(query.limit)) {
        Ok(events) => Json(events).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

pub async fn export_csv(
    Extension(state): Extension<ApiState>,
    Query(query): Query<LimitQuery>,
) -> Response {
    let result = DuckDbStore::open(&*state.duckdb_path).and_then(|store| {
        let events = store.query_recent(query.limit)?;
        let mut writer = csv::Writer::from_writer(Vec::new());
        for event in events {
            writer.serialize(event)?;
        }
        let bytes = writer.into_inner()?;
        Ok(String::from_utf8(bytes)?)
    });

    match result {
        Ok(csv) => (
            [(header::CONTENT_TYPE, "text/csv; charset=utf-8")],
            csv,
        )
            .into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::{Method, Request},
    };
    use fwlog_domain::{CanonicalEvent, ParseStatus, RawLog};
    use tower::ServiceExt;

    #[tokio::test]
    async fn health_events_and_export_routes_work() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();
        let raw = RawLog::new("tcp://127.0.0.1:1514", "raw");
        let mut event = CanonicalEvent::failed(raw, "bad");
        event.event_id = "api-test".to_string();
        event.parse_status = ParseStatus::Failed;
        store.insert_batch(&[event]).unwrap();

        let app = crate::router(
            db_path,
            dir.path().join("parquet"),
            dir.path().join("frozen"),
        );

        let health = app
            .clone()
            .oneshot(Request::builder().uri("/api/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);

        let events = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/events?limit=20")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(events.status(), StatusCode::OK);

        let csv = app
            .oneshot(
                Request::builder()
                    .uri("/api/events/export.csv?limit=20")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(csv.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn archive_routes_create_and_list_parquet_files() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let parquet_dir = dir.path().join("parquet");
        let mut store = DuckDbStore::open(&db_path).unwrap();
        let raw = RawLog::new("tcp://127.0.0.1:1514", "raw");
        let mut event = CanonicalEvent::failed(raw, "bad");
        event.event_id = "archive-test".to_string();
        event.parse_status = ParseStatus::Failed;
        store.insert_batch(&[event]).unwrap();

        let app = crate::router(db_path, parquet_dir, dir.path().join("frozen"));

        let created = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/archive/parquet?limit=20")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::OK);
        let created: serde_json::Value =
            serde_json::from_slice(&to_bytes(created.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert!(created["path"]
            .as_str()
            .unwrap()
            .ends_with(".parquet"));
        assert!(created["bytes"].as_u64().unwrap() > 0);

        let files = app
            .oneshot(
                Request::builder()
                    .uri("/api/archive/files")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(files.status(), StatusCode::OK);
        let files: serde_json::Value =
            serde_json::from_slice(&to_bytes(files.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(files.as_array().unwrap().len(), 1);
        assert_eq!(files[0]["path"], created["path"]);
        assert_eq!(files[0]["bytes"], created["bytes"]);
    }

    #[tokio::test]
    async fn frozen_archive_routes_create_list_restore_and_reject_outside_paths() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let parquet_dir = dir.path().join("parquet");
        let frozen_dir = dir.path().join("frozen");
        let mut store = DuckDbStore::open(&db_path).unwrap();
        let events = (0..5)
            .map(|index| {
                let raw = RawLog::new("tcp://127.0.0.1:1514", format!("raw frozen {index}"));
                let mut event = CanonicalEvent::failed(raw, "bad");
                event.event_id = format!("frozen-test-{index}");
                event.parse_status = ParseStatus::Failed;
                event
            })
            .collect::<Vec<_>>();
        store.insert_batch(&events).unwrap();

        let app = crate::router(db_path, parquet_dir, frozen_dir);

        let created = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/archive/frozen?limit=5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::OK);
        let created: serde_json::Value =
            serde_json::from_slice(&to_bytes(created.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert!(created["path"]
            .as_str()
            .unwrap()
            .ends_with(".raw.zst"));
        assert!(created["bytes"].as_u64().unwrap() > 0);

        let files = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/archive/frozen")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(files.status(), StatusCode::OK);
        let files: serde_json::Value =
            serde_json::from_slice(&to_bytes(files.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(files.as_array().unwrap().len(), 1);
        assert_eq!(files[0]["path"], created["path"]);

        let restore_path = percent_encode(created["path"].as_str().unwrap());
        let restored = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/archive/frozen/restore?path={restore_path}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(restored.status(), StatusCode::OK);
        let restored: Vec<String> =
            serde_json::from_slice(&to_bytes(restored.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(restored.len(), 5);
        assert!(restored.iter().any(|line| line == "raw frozen 0"));

        let outside = percent_encode(dir.path().join("outside.raw.zst").to_str().unwrap());
        let rejected = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/archive/frozen/restore?path={outside}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn system_status_reports_paths_sizes_and_missing_archive_dirs_as_empty() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let parquet_dir = dir.path().join("parquet");
        let frozen_dir = dir.path().join("frozen");
        let nested_parquet_dir = parquet_dir.join("nested");
        let nested_frozen_dir = frozen_dir.join("nested");
        std::fs::create_dir_all(&nested_parquet_dir).unwrap();
        std::fs::create_dir_all(&nested_frozen_dir).unwrap();
        std::fs::write(&db_path, b"duckdb").unwrap();
        std::fs::write(parquet_dir.join("events-a.parquet"), b"root").unwrap();
        std::fs::write(nested_parquet_dir.join("events-b.parquet"), b"nested").unwrap();
        std::fs::write(parquet_dir.join("ignore.txt"), b"ignore").unwrap();
        std::fs::write(frozen_dir.join("frozen-a.raw.zst"), b"raw").unwrap();
        std::fs::write(nested_frozen_dir.join("frozen-b.raw.zst"), b"zstd").unwrap();
        std::fs::write(frozen_dir.join("ignore.zst"), b"ignore").unwrap();

        let app = crate::router(db_path.clone(), parquet_dir.clone(), frozen_dir.clone());

        let status = app
            .oneshot(
                Request::builder()
                    .uri("/api/system/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::OK);
        let status: serde_json::Value =
            serde_json::from_slice(&to_bytes(status.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(status["service"], "fwlogd");
        assert_eq!(status["duckdb_path"], db_path.to_str().unwrap());
        assert_eq!(status["parquet_dir"], parquet_dir.to_str().unwrap());
        assert_eq!(status["frozen_dir"], frozen_dir.to_str().unwrap());
        assert_eq!(status["duckdb_bytes"], 6);
        assert_eq!(status["parquet_files"], 2);
        assert_eq!(status["parquet_bytes"], 10);
        assert_eq!(status["frozen_files"], 2);
        assert_eq!(status["frozen_bytes"], 7);

        let missing_app = crate::router(
            dir.path().join("missing.duckdb"),
            dir.path().join("missing-parquet"),
            dir.path().join("missing-frozen"),
        );

        let missing_status = missing_app
            .oneshot(
                Request::builder()
                    .uri("/api/system/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_status.status(), StatusCode::OK);
        let missing_status: serde_json::Value = serde_json::from_slice(
            &to_bytes(missing_status.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(missing_status["duckdb_bytes"], 0);
        assert_eq!(missing_status["parquet_files"], 0);
        assert_eq!(missing_status["parquet_bytes"], 0);
        assert_eq!(missing_status["frozen_files"], 0);
        assert_eq!(missing_status["frozen_bytes"], 0);
    }

    fn percent_encode(value: &str) -> String {
        value
            .bytes()
            .flat_map(|byte| match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                    vec![byte as char]
                }
                _ => format!("%{byte:02X}").chars().collect(),
            })
            .collect()
    }
}

pub async fn archive_files(Extension(state): Extension<ApiState>) -> Response {
    match list_archive_files(&*state.parquet_dir) {
        Ok(files) => Json(
            files
                .into_iter()
                .map(ArchiveFileJson::from)
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

fn archive_stats(dir: &Path) -> anyhow::Result<(usize, u64)> {
    if !dir.exists() {
        return Ok((0, 0));
    }

    let files = list_archive_files(dir)?;
    Ok((
        files.len(),
        files.into_iter().map(|file| file.bytes).sum(),
    ))
}

fn frozen_stats(dir: &Path) -> anyhow::Result<(usize, u64)> {
    if !dir.exists() {
        return Ok((0, 0));
    }

    let files = list_frozen_files(dir)?;
    Ok((
        files.len(),
        files.into_iter().map(|file| file.bytes).sum(),
    ))
}

pub async fn archive_parquet(
    Extension(state): Extension<ApiState>,
    Query(query): Query<LimitQuery>,
) -> Response {
    let file_name = format!("events-{}.parquet", Utc::now().format("%Y%m%d-%H%M%S"));
    let output_path = state.parquet_dir.join(file_name);

    let result = fs::create_dir_all(&*state.parquet_dir)
        .context("create parquet archive directory")
        .and_then(|_| DuckDbStore::open(&*state.duckdb_path))
        .and_then(|store| store.archive_parquet(&output_path, query.limit));

    match result {
        Ok(file) => Json(ArchiveFileJson::from(file)).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

pub async fn archive_frozen_files(Extension(state): Extension<ApiState>) -> Response {
    match list_frozen_files(&*state.frozen_dir) {
        Ok(files) => Json(
            files
                .into_iter()
                .map(ArchiveFileJson::from)
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

pub async fn archive_frozen(
    Extension(state): Extension<ApiState>,
    Query(query): Query<LimitQuery>,
) -> Response {
    let file_name = format!("frozen-{}.raw.zst", Utc::now().format("%Y%m%d-%H%M%S"));
    let output_path = state.frozen_dir.join(file_name);

    let result = fs::create_dir_all(&*state.frozen_dir)
        .context("create frozen archive directory")
        .and_then(|_| DuckDbStore::open(&*state.duckdb_path))
        .and_then(|store| {
            let raw_lines = store
                .query_recent(query.limit)?
                .into_iter()
                .map(|event| event.raw)
                .collect::<Vec<_>>();
            write_frozen_raw(&output_path, &raw_lines)
        });

    match result {
        Ok(file) => Json(ArchiveFileJson::from(file)).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

pub async fn restore_frozen(
    Extension(state): Extension<ApiState>,
    Query(query): Query<RestoreQuery>,
) -> Response {
    match checked_frozen_path(state.frozen_dir.as_ref().as_path(), &query.path)
        .and_then(read_frozen_raw)
    {
        Ok(lines) => Json(lines).into_response(),
        Err(err) if err.downcast_ref::<OutsideFrozenDir>().is_some() => {
            (StatusCode::BAD_REQUEST, err.to_string()).into_response()
        }
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

#[derive(Debug)]
struct OutsideFrozenDir;

impl std::fmt::Display for OutsideFrozenDir {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("frozen archive path is outside frozen_dir")
    }
}

impl std::error::Error for OutsideFrozenDir {}

fn checked_frozen_path(frozen_dir: &Path, input_path: &Path) -> anyhow::Result<PathBuf> {
    let frozen_dir = frozen_dir
        .canonicalize()
        .with_context(|| format!("canonicalize frozen directory {}", frozen_dir.display()))?;
    let input_path = match input_path.canonicalize() {
        Ok(path) => path,
        Err(err) => {
            let parent = input_path
                .parent()
                .ok_or_else(|| anyhow!("canonicalize frozen archive path {}", input_path.display()))?;
            let parent = parent
                .canonicalize()
                .with_context(|| format!("canonicalize frozen archive parent {}", parent.display()))?;
            let file_name = input_path
                .file_name()
                .ok_or_else(|| anyhow!("canonicalize frozen archive path {}", input_path.display()))?;
            let _ = err;
            parent.join(file_name)
        }
    };

    if input_path.starts_with(&frozen_dir) {
        Ok(input_path)
    } else {
        Err(OutsideFrozenDir.into())
    }
}
