use std::{fs, path::PathBuf};

use anyhow::Context;
use axum::{
    extract::{Extension, Query},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use chrono::Utc;
use fwlog_storage::{list_archive_files, ArchiveFile, DuckDbStore};
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

fn default_limit() -> usize {
    20
}

pub async fn health() -> Json<serde_json::Value> {
    Json(json!({"status":"ok","service":"fwlogd"}))
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

        let app = crate::router(db_path, dir.path().join("parquet"));

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

        let app = crate::router(db_path, parquet_dir);

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
