use std::{
    fs,
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{anyhow, Context};
use axum::{
    extract::{Extension, Query},
    http::{header, StatusCode},
    response::{Html, IntoResponse, Response},
    Json,
};
use chrono::Utc;
use fwlog_adapter::{LogAdapter, SangforAdapter};
use fwlog_domain::{CanonicalEvent, RawLog};
use fwlog_storage::{
    list_archive_files, list_frozen_files, read_frozen_raw, write_frozen_raw, ArchiveFile,
    DuckDbStore, FrozenFile,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::ApiState;

const APP_HTML: &str = include_str!("../../../web/index.html");
const MAX_QUERY_LIMIT: usize = 100_000;
const MAX_COLD_SEARCH_LIMIT: usize = 10_000;
static ARCHIVE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Deserialize)]
pub struct LimitQuery {
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
pub struct ColdSearchQuery {
    day: Option<String>,
    src_ip: Option<String>,
    dst_ip: Option<String>,
    action: Option<String>,
    keyword: Option<String>,
    #[serde(default)]
    include_failed: bool,
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(Debug, Serialize)]
struct ColdSearchResponse {
    files: usize,
    scanned_lines: u64,
    matched: usize,
    limited: bool,
    events: Vec<CanonicalEvent>,
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
    auth_enabled: bool,
    duckdb_path: PathBuf,
    parquet_dir: PathBuf,
    frozen_dir: PathBuf,
    events_total: u64,
    events_parsed: u64,
    events_failed: u64,
    duckdb_bytes: u64,
    parquet_files: usize,
    parquet_bytes: u64,
    frozen_files: usize,
    frozen_bytes: u64,
    metrics: fwlog_domain::MetricsSnapshot,
}

fn default_limit() -> usize {
    20
}

fn bounded_limit(limit: usize) -> usize {
    limit.clamp(1, MAX_QUERY_LIMIT)
}

fn bounded_cold_limit(limit: usize) -> usize {
    limit.clamp(1, MAX_COLD_SEARCH_LIMIT)
}

fn archive_stamp(prefix: &str, suffix: &str) -> String {
    let sequence = ARCHIVE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!(
        "{prefix}-{}-{:06}.{suffix}",
        Utc::now().format("%Y%m%d-%H%M%S%.6f"),
        sequence % 1_000_000
    )
}

pub async fn app() -> Html<&'static str> {
    Html(APP_HTML)
}

pub async fn health() -> Json<serde_json::Value> {
    Json(json!({"status":"ok","service":"fwlogd"}))
}

pub async fn system_status(Extension(state): Extension<ApiState>) -> Response {
    let duckdb_bytes = match fs::metadata(&*state.duckdb_path) {
        Ok(metadata) if metadata.is_file() => metadata.len(),
        Ok(_) => 0,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => 0,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    };

    let parquet_files = match archive_stats(state.parquet_dir.as_ref().as_path()) {
        Ok(stats) => stats,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    };
    let frozen_files = match frozen_stats(state.frozen_dir.as_ref().as_path()) {
        Ok(stats) => stats,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    };
    let event_stats = match DuckDbStore::open(&*state.duckdb_path)
        .and_then(|store| store.event_stats())
    {
        Ok(stats) => stats,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    };

    Json(SystemStatus {
        service: "fwlogd",
        auth_enabled: state.auth_enabled,
        duckdb_path: state.duckdb_path.as_ref().clone(),
        parquet_dir: state.parquet_dir.as_ref().clone(),
        frozen_dir: state.frozen_dir.as_ref().clone(),
        events_total: event_stats.total,
        events_parsed: event_stats.parsed,
        events_failed: event_stats.failed,
        duckdb_bytes,
        parquet_files: parquet_files.0,
        parquet_bytes: parquet_files.1,
        frozen_files: frozen_files.0,
        frozen_bytes: frozen_files.1,
        metrics: state.metrics.snapshot(),
    })
    .into_response()
}

pub async fn events(
    Extension(state): Extension<ApiState>,
    Query(query): Query<LimitQuery>,
) -> Response {
    match DuckDbStore::open(&*state.duckdb_path)
        .and_then(|store| store.query_recent(bounded_limit(query.limit)))
    {
        Ok(events) => Json(events).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    }
}

pub async fn cold_search(
    Extension(state): Extension<ApiState>,
    Query(query): Query<ColdSearchQuery>,
) -> Response {
    let frozen_dir = state.frozen_dir.as_ref().clone();
    let limit = bounded_cold_limit(query.limit);
    let result = tokio::task::spawn_blocking(move || search_cold_archives(&frozen_dir, query, limit))
        .await
        .map_err(|err| anyhow!("cold search worker failed: {err}"))
        .and_then(|result| result);

    match result {
        Ok(response) => Json(response).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    }
}

pub async fn export_csv(
    Extension(state): Extension<ApiState>,
    Query(query): Query<LimitQuery>,
) -> Response {
    let result = DuckDbStore::open(&*state.duckdb_path).and_then(|store| {
        let events = store.query_recent(bounded_limit(query.limit))?;
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
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    }
}

fn search_cold_archives(
    frozen_dir: &Path,
    query: ColdSearchQuery,
    limit: usize,
) -> anyhow::Result<ColdSearchResponse> {
    let day_filter = query.day.as_deref().and_then(normalize_day_filter);
    let archives = cold_archive_files(frozen_dir, day_filter.as_deref())?;
    let mut scanned_lines = 0_u64;
    let mut events = Vec::new();
    let adapter = SangforAdapter;

    for archive_path in &archives {
        let archive_day_matches = archive_path
            .file_name()
            .and_then(|value| value.to_str())
            .is_some_and(|name| day_filter.as_deref().is_none_or(|day| name.contains(day)));
        let file = File::open(archive_path)
            .with_context(|| format!("open cold archive {}", archive_path.display()))?;
        let decoder = zstd::stream::read::Decoder::new(file)
            .with_context(|| format!("decode cold archive {}", archive_path.display()))?;
        let mut archive = tar::Archive::new(decoder);
        for entry in archive.entries().context("read cold tar entries")? {
            let entry = entry.context("read cold tar entry")?;
            if !entry.header().entry_type().is_file() {
                continue;
            }
            let entry_path = entry.path()?.to_string_lossy().into_owned();
            if !archive_day_matches && !cold_entry_day_matches(&entry_path, day_filter.as_deref()) {
                continue;
            }
            let source_addr = format!("frozen://{entry_path}");
            let mut reader: Box<dyn BufRead> = if entry_path.ends_with(".gz") {
                Box::new(BufReader::new(flate2::read::GzDecoder::new(entry)))
            } else {
                Box::new(BufReader::new(entry))
            };
            if scan_cold_reader(
                &mut *reader,
                &adapter,
                &query,
                limit,
                &source_addr,
                &mut scanned_lines,
                &mut events,
            )? {
                return Ok(ColdSearchResponse {
                    files: archives.len(),
                    scanned_lines,
                    matched: events.len(),
                    limited: true,
                    events,
                });
            }
        }
    }

    Ok(ColdSearchResponse {
        files: archives.len(),
        scanned_lines,
        matched: events.len(),
        limited: false,
        events,
    })
}

fn scan_cold_reader(
    reader: &mut dyn BufRead,
    adapter: &SangforAdapter,
    query: &ColdSearchQuery,
    limit: usize,
    source_addr: &str,
    scanned_lines: &mut u64,
    events: &mut Vec<CanonicalEvent>,
) -> anyhow::Result<bool> {
    let mut buffer = Vec::new();
    loop {
        buffer.clear();
        let bytes = reader.read_until(b'\n', &mut buffer)?;
        if bytes == 0 {
            break;
        }
        while buffer.last().is_some_and(|byte| *byte == b'\n' || *byte == b'\r') {
            buffer.pop();
        }
        if buffer.is_empty() {
            continue;
        }
        *scanned_lines += 1;
        let raw = String::from_utf8_lossy(&buffer).into_owned();
        if let Some(keyword) = query.keyword.as_deref() {
            if !raw.contains(keyword) {
                continue;
            }
        }
        let event = adapter.parse(RawLog {
            ingest_time: Utc::now(),
            source_addr: source_addr.to_string(),
            raw,
        });
        if !query.include_failed && event.parse_status != fwlog_domain::ParseStatus::Parsed {
            continue;
        }
        if !matches_cold_query(&event, query) {
            continue;
        }
        events.push(event);
        if events.len() >= limit {
            return Ok(true);
        }
    }
    Ok(false)
}

fn cold_archive_files(frozen_dir: &Path, day: Option<&str>) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_cold_archive_files(frozen_dir, day, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_cold_archive_files(
    dir: &Path,
    day: Option<&str>,
    files: &mut Vec<PathBuf>,
) -> anyhow::Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir).with_context(|| format!("read frozen directory {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_cold_archive_files(&path, day, files)?;
            continue;
        }
        let name = path.file_name().and_then(|value| value.to_str()).unwrap_or("");
        let day_matches = day.is_none_or(|value| name.contains(value));
        if name.starts_with("raw-import-") && name.ends_with(".tar.zst") && (day_matches || day.is_some()) {
            files.push(path);
        }
    }
    Ok(())
}

fn normalize_day_filter(day: &str) -> Option<String> {
    let digits: String = day.chars().filter(|value| value.is_ascii_digit()).collect();
    if digits.len() == 8 {
        Some(digits)
    } else {
        None
    }
}

fn cold_entry_day_matches(entry_path: &str, day: Option<&str>) -> bool {
    day.is_none_or(|value| {
        entry_path.contains(value)
            || entry_path.contains(&format!("{}-{}-{}", &value[0..4], &value[4..6], &value[6..8]))
    })
}

fn matches_cold_query(event: &CanonicalEvent, query: &ColdSearchQuery) -> bool {
    if let Some(value) = query.src_ip.as_deref() {
        if event.src_ip.as_deref() != Some(value) {
            return false;
        }
    }
    if let Some(value) = query.dst_ip.as_deref() {
        if event.dst_ip.as_deref() != Some(value) {
            return false;
        }
    }
    if let Some(value) = query.action.as_deref() {
        if event.action.as_deref() != Some(value) {
            return false;
        }
    }
    true
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
    async fn root_serves_embedded_chinese_ui() {
        let dir = tempfile::tempdir().unwrap();
        let app = crate::router(
            dir.path().join("oxidelog.duckdb"),
            dir.path().join("parquet"),
            dir.path().join("frozen"),
        );

        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("OxideLog"));
        assert!(body.contains("日志中心"));
    }

    #[tokio::test]
    async fn api_token_protects_api_routes_but_not_ui() {
        let dir = tempfile::tempdir().unwrap();
        let app = crate::router_with_options(
            dir.path().join("oxidelog.duckdb"),
            dir.path().join("parquet"),
            dir.path().join("frozen"),
            fwlog_domain::RuntimeMetrics::default(),
            Some("secret-token".to_string()),
        );

        let ui = app
            .clone()
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(ui.status(), StatusCode::OK);

        let unauthorized = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let authorized = app
            .oneshot(
                Request::builder()
                    .uri("/api/health")
                    .header(header::AUTHORIZATION, "Bearer secret-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(authorized.status(), StatusCode::OK);
    }

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
    async fn cold_search_streams_raw_import_tar_zst_without_extracting() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let parquet_dir = dir.path().join("parquet");
        let frozen_dir = dir.path().join("frozen");
        std::fs::create_dir_all(&frozen_dir).unwrap();
        write_raw_import_tar_zst(
            &frozen_dir.join("raw-import-20260516-test.tar.zst"),
            "Sangfor: src=192.168.0.105 dst=10.4.90.205 sport=21527 dport=2048 proto=TCP action=snat severity=info\n\
             Sangfor: src=192.168.0.200 dst=10.4.90.206 sport=21528 dport=443 proto=TCP action=snat severity=info\n",
        );

        let app = crate::router(db_path, parquet_dir, frozen_dir);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/cold/search?day=20260516&src_ip=192.168.0.105&limit=5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body["files"], 1);
        assert_eq!(body["scanned_lines"], 2);
        assert_eq!(body["matched"], 1);
        assert_eq!(body["events"][0]["src_ip"], "192.168.0.105");
        assert_eq!(body["events"][0]["dst_ip"], "10.4.90.205");
    }

    #[tokio::test]
    async fn cold_search_hides_failed_lines_by_default() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let parquet_dir = dir.path().join("parquet");
        let frozen_dir = dir.path().join("frozen");
        std::fs::create_dir_all(&frozen_dir).unwrap();
        write_raw_import_tar_zst(
            &frozen_dir.join("raw-import-20260516-mixed.tar.zst"),
            "Apr 29 17:19:12 192.168.9.6 825: AP:e05f.b9ea.78be: %LINK-3-UPDOWN\n\
             Sangfor: src=192.168.0.105 dst=10.4.90.205 sport=21527 dport=2048 proto=TCP action=snat severity=info\n",
        );

        let app = crate::router(db_path, parquet_dir, frozen_dir);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/cold/search?day=20260516&limit=5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body["matched"], 1);
        assert_eq!(body["events"][0]["parse_status"], "parsed");
    }

    #[tokio::test]
    async fn cold_search_streams_gzip_members_inside_raw_import_tar_zst() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let parquet_dir = dir.path().join("parquet");
        let frozen_dir = dir.path().join("frozen");
        std::fs::create_dir_all(&frozen_dir).unwrap();
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        std::io::Write::write_all(
            &mut gz,
            b"Sangfor: src=192.168.9.10 dst=10.4.90.205 sport=21527 dport=2048 proto=UDP action=snat severity=info\n",
        )
        .unwrap();
        write_raw_import_tar_zst_member(
            &frozen_dir.join("raw-import-20260516-gz.tar.zst"),
            "logs/sangfor.log-20260516.gz",
            &gz.finish().unwrap(),
        );

        let app = crate::router(db_path, parquet_dir, frozen_dir);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/cold/search?day=20260516&src_ip=192.168.9.10&limit=5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body["matched"], 1);
        assert_eq!(body["events"][0]["protocol"], "UDP");
    }

    #[tokio::test]
    async fn cold_search_day_matches_tar_member_name_not_only_archive_name() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let parquet_dir = dir.path().join("parquet");
        let frozen_dir = dir.path().join("frozen");
        std::fs::create_dir_all(&frozen_dir).unwrap();
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        std::io::Write::write_all(
            &mut gz,
            b"Sangfor: src=10.10.10.1 dst=10.4.90.205 sport=21527 dport=2048 proto=UDP action=snat severity=info\n",
        )
        .unwrap();
        write_raw_import_tar_zst_member(
            &frozen_dir.join("raw-import-20260516-full.tar.zst"),
            "sangfor_fw_log/10.10.10.1_2026-04-24.log-20260425.gz",
            &gz.finish().unwrap(),
        );

        let app = crate::router(db_path, parquet_dir, frozen_dir);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/cold/search?day=2026-04-25&limit=5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body["files"], 1);
        assert_eq!(body["scanned_lines"], 1);
        assert_eq!(body["matched"], 1);
        assert!(body["events"][0]["source_addr"]
            .as_str()
            .unwrap()
            .contains("20260425"));
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
        std::fs::write(parquet_dir.join("events-a.parquet"), b"root").unwrap();
        std::fs::write(nested_parquet_dir.join("events-b.parquet"), b"nested").unwrap();
        std::fs::write(parquet_dir.join("ignore.txt"), b"ignore").unwrap();
        std::fs::write(frozen_dir.join("frozen-a.raw.zst"), b"raw").unwrap();
        std::fs::write(nested_frozen_dir.join("frozen-b.raw.zst"), b"zstd").unwrap();
        std::fs::write(frozen_dir.join("ignore.zst"), b"ignore").unwrap();
        let mut store = DuckDbStore::open(&db_path).unwrap();
        store
            .insert_batch(&[
                {
                    let raw = RawLog::new("tcp://127.0.0.1:1514", "parsed raw");
                    let mut event = CanonicalEvent::failed(raw, "bad");
                    event.event_id = "status-parsed".to_string();
                    event.parse_status = ParseStatus::Parsed;
                    event.parse_error = None;
                    event
                },
                {
                    let raw = RawLog::new("tcp://127.0.0.1:1514", "failed raw");
                    let mut event = CanonicalEvent::failed(raw, "bad");
                    event.event_id = "status-failed".to_string();
                    event
                },
            ])
            .unwrap();

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
        assert_eq!(status["auth_enabled"], false);
        assert_eq!(status["duckdb_path"], db_path.to_str().unwrap());
        assert_eq!(status["parquet_dir"], parquet_dir.to_str().unwrap());
        assert_eq!(status["frozen_dir"], frozen_dir.to_str().unwrap());
        assert_eq!(status["events_total"], 2);
        assert_eq!(status["events_parsed"], 1);
        assert_eq!(status["events_failed"], 1);
        assert!(status["duckdb_bytes"].as_u64().unwrap() > 0);
        assert_eq!(status["parquet_files"], 2);
        assert_eq!(status["parquet_bytes"], 10);
        assert_eq!(status["frozen_files"], 2);
        assert_eq!(status["frozen_bytes"], 7);
        assert_eq!(status["metrics"]["tcp_received"], 0);

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

    fn write_raw_import_tar_zst(path: &Path, content: &str) {
        write_raw_import_tar_zst_member(path, "logs/sangfor.log", content.as_bytes());
    }

    fn write_raw_import_tar_zst_member(path: &Path, member_name: &str, bytes: &[u8]) {
        let file = std::fs::File::create(path).unwrap();
        let encoder = zstd::Encoder::new(file, 3).unwrap();
        let mut builder = tar::Builder::new(encoder);
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder.append_data(&mut header, member_name, bytes).unwrap();
        let encoder = builder.into_inner().unwrap();
        encoder.finish().unwrap();
    }

    #[test]
    fn bounded_limit_clamps_zero_and_huge_values() {
        assert_eq!(bounded_limit(0), 1);
        assert_eq!(bounded_limit(20), 20);
        assert_eq!(bounded_limit(usize::MAX), MAX_QUERY_LIMIT);
    }

    #[test]
    fn archive_stamp_is_unique_within_same_process() {
        let first = archive_stamp("events", "parquet");
        let second = archive_stamp("events", "parquet");

        assert_ne!(first, second);
        assert!(first.ends_with(".parquet"));
        assert!(second.ends_with(".parquet"));
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
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
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
    let file_name = archive_stamp("events", "parquet");
    let output_path = state.parquet_dir.join(file_name);

    let result = fs::create_dir_all(&*state.parquet_dir)
        .context("create parquet archive directory")
        .and_then(|_| DuckDbStore::open(&*state.duckdb_path))
        .and_then(|store| store.archive_parquet(&output_path, bounded_limit(query.limit)));

    match result {
        Ok(file) => Json(ArchiveFileJson::from(file)).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
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
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    }
}

pub async fn archive_frozen(
    Extension(state): Extension<ApiState>,
    Query(query): Query<LimitQuery>,
) -> Response {
    let file_name = archive_stamp("frozen", "raw.zst");
    let output_path = state.frozen_dir.join(file_name);

    let result = fs::create_dir_all(&*state.frozen_dir)
        .context("create frozen archive directory")
        .and_then(|_| DuckDbStore::open(&*state.duckdb_path))
        .and_then(|store| {
            let raw_lines = store
                .query_recent(bounded_limit(query.limit))?
                .into_iter()
                .map(|event| event.raw)
                .collect::<Vec<_>>();
            write_frozen_raw(&output_path, &raw_lines)
        });

    match result {
        Ok(file) => Json(ArchiveFileJson::from(file)).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
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
            (StatusCode::BAD_REQUEST, format!("{err:#}")).into_response()
        }
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
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
