use axum::{
    extract::{Extension, Query},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use fwlog_storage::DuckDbStore;
use serde::Deserialize;
use serde_json::json;

use crate::ApiState;

#[derive(Debug, Deserialize)]
pub struct LimitQuery {
    #[serde(default = "default_limit")]
    limit: usize,
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
    use axum::{body::Body, http::Request};
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

        let app = crate::router(db_path);

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
}
