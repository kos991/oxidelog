use std::{
    collections::HashMap,
    fs,
    fs::File,
    io::{BufRead, BufReader},
    net::{IpAddr, Ipv4Addr},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex, OnceLock,
    },
    time::{Duration, SystemTime},
};

use anyhow::{anyhow, Context};
use axum::{
    extract::{Extension, Path as AxumPath, Query},
    http::{header, StatusCode},
    response::{Html, IntoResponse, Response},
    Json,
};
use chrono::Utc;
use duckdb::{params_from_iter, Connection};
use fwlog_adapter::ParserEngine;
use fwlog_domain::{CanonicalEvent, ParseStatus, RawLog};
use fwlog_storage::{
    list_archive_files, list_frozen_files, read_frozen_raw, write_frozen_raw, ArchiveFile,
    DeviceBinding, DuckDbStore, EventQuery, FrozenFile, IpRegionCacheEntry, MinuteMetricQuery,
    SourceMetricQuery,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::ApiState;

const APP_HTML: &str = include_str!("../../../web/index.html");
const MAX_QUERY_LIMIT: usize = 100_000;
const MAX_COLD_SEARCH_LIMIT: usize = 10_000;
const IP2REGION_V4_XDB: &[u8] = include_bytes!("../../../data/ip2region/ip2region_v4.xdb");
static ARCHIVE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static IP2REGION_DATA: OnceLock<Vec<u8>> = OnceLock::new();

#[derive(Debug, Deserialize)]
pub struct LimitQuery {
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
pub struct MinuteMetricsRequest {
    #[serde(default = "default_metric_hours")]
    hours: u32,
    #[serde(default = "default_metric_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
pub struct SourceMetricsRequest {
    #[serde(default = "default_metric_hours")]
    hours: u32,
    #[serde(default = "default_source_metric_limit")]
    limit: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EventsQuery {
    day: Option<String>,
    date_from: Option<String>,
    date_to: Option<String>,
    device: Option<String>,
    device_id: Option<String>,
    src_ip: Option<String>,
    dst_ip: Option<String>,
    protocol: Option<String>,
    action: Option<String>,
    keyword: Option<String>,
    #[serde(default)]
    include_failed: bool,
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ColdSearchQuery {
    day: Option<String>,
    date_from: Option<String>,
    date_to: Option<String>,
    device: Option<String>,
    device_id: Option<String>,
    src_ip: Option<String>,
    dst_ip: Option<String>,
    protocol: Option<String>,
    action: Option<String>,
    keyword: Option<String>,
    #[serde(default)]
    include_failed: bool,
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SearchQuery {
    #[serde(default = "default_search_scope")]
    scope: String,
    day: Option<String>,
    date_from: Option<String>,
    date_to: Option<String>,
    device: Option<String>,
    device_id: Option<String>,
    src_ip: Option<String>,
    dst_ip: Option<String>,
    protocol: Option<String>,
    action: Option<String>,
    keyword: Option<String>,
    #[serde(default)]
    include_failed: bool,
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
pub struct ArchiveIndexQuery {
    day: Option<String>,
    #[serde(default = "default_archive_index_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
pub struct IpRegionQuery {
    ip: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomIpRegion {
    id: String,
    cidr: String,
    name: String,
    #[serde(default)]
    note: String,
    #[serde(default = "default_enabled")]
    enabled: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    deleted: bool,
    created_at: String,
}

#[derive(Debug, Deserialize)]
pub struct CustomIpRegionInput {
    cidr: String,
    name: String,
    #[serde(default)]
    note: String,
    #[serde(default = "default_enabled")]
    enabled: bool,
}

#[derive(Debug, Serialize)]
struct IpRegionResponse {
    ip: String,
    region: Option<String>,
    country: Option<String>,
    province: Option<String>,
    city: Option<String>,
    isp: Option<String>,
    raw: Option<String>,
}

#[derive(Debug, Serialize)]
struct UnifiedSearchRow {
    result_source: String,
    archive_path: Option<PathBuf>,
    device_name: Option<String>,
    geo_region: Option<String>,
    src_geo_region: Option<String>,
    dst_geo_region: Option<String>,
    event: CanonicalEvent,
}

#[derive(Debug, Serialize)]
struct HourMetricResponse {
    bucket_hour: String,
    total: u64,
    parsed: u64,
    partial: u64,
    failed: u64,
    raw_bytes: u64,
}

#[derive(Debug, Serialize)]
struct ParserSummaryResponse {
    reason: String,
    count: u64,
}

const DEFAULT_CUSTOM_IP_REGIONS: &[(&str, &str)] = &[
    ("172.18.0.0/17", "172.18.0.0/17"),
    ("172.28.128.0/19", "172.28.128.0/19"),
    ("2.0.0.0/8", "2.0.0.0/8"),
];

const EXPORT_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_EXPORT_LIMIT: usize = 1_000_000;
static EXPORT_JOBS: OnceLock<Mutex<HashMap<String, ExportJob>>> = OnceLock::new();

#[derive(Debug, Clone, Serialize)]
struct ExportJob {
    job_id: String,
    status: String,
    scope: String,
    format: ExportFormat,
    file_name: String,
    download_url: String,
    rows: usize,
    file_bytes: u64,
    error: Option<String>,
    created_at: String,
    updated_at: String,
    expires_at: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExportJobRequest {
    #[serde(default = "default_search_scope")]
    scope: String,
    day: Option<String>,
    date_from: Option<String>,
    date_to: Option<String>,
    device: Option<String>,
    device_id: Option<String>,
    src_ip: Option<String>,
    dst_ip: Option<String>,
    protocol: Option<String>,
    action: Option<String>,
    keyword: Option<String>,
    #[serde(default)]
    format: ExportFormat,
    #[serde(default, deserialize_with = "deserialize_bool_from_any")]
    include_failed: bool,
    #[serde(
        default = "default_export_limit",
        deserialize_with = "deserialize_usize_from_any"
    )]
    limit: usize,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum ExportFormat {
    Csv,
    Zst,
    Parquet,
}

impl Default for ExportFormat {
    fn default() -> Self {
        Self::Zst
    }
}

impl ExportFormat {
    fn extension(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Zst => "csv.zst",
            Self::Parquet => "parquet",
        }
    }

    fn content_type(self) -> &'static str {
        match self {
            Self::Csv => "text/csv; charset=utf-8",
            Self::Zst => "application/zstd",
            Self::Parquet => "application/vnd.apache.parquet",
        }
    }
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
    events_partial: u64,
    events_failed: u64,
    duckdb_bytes: u64,
    parquet_files: usize,
    parquet_bytes: u64,
    frozen_files: usize,
    frozen_bytes: u64,
    metrics: fwlog_domain::MetricsSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirewallDevice {
    id: String,
    name: String,
    host: String,
    protocol: String,
    port: u16,
    #[serde(default)]
    note: String,
    #[serde(default = "default_enabled")]
    enabled: bool,
    created_at: String,
}

#[derive(Debug, Deserialize)]
pub struct FirewallDeviceInput {
    name: String,
    host: String,
    protocol: String,
    port: u16,
    #[serde(default)]
    note: String,
    #[serde(default = "default_enabled")]
    enabled: bool,
}

fn default_enabled() -> bool {
    true
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn default_limit() -> usize {
    20
}

fn default_export_limit() -> usize {
    1_000_000
}

fn deserialize_usize_from_any<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Value {
        Number(usize),
        Text(String),
    }

    match Value::deserialize(deserializer)? {
        Value::Number(value) => Ok(value),
        Value::Text(value) => value
            .trim()
            .parse::<usize>()
            .map_err(serde::de::Error::custom),
    }
}

fn deserialize_bool_from_any<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Value {
        Bool(bool),
        Text(String),
    }

    match Value::deserialize(deserializer)? {
        Value::Bool(value) => Ok(value),
        Value::Text(value) => match value.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Ok(true),
            "false" | "0" | "no" | "off" | "" => Ok(false),
            other => Err(serde::de::Error::custom(format!("invalid bool {other}"))),
        },
    }
}

fn default_search_scope() -> String {
    "all".to_string()
}

fn default_archive_index_limit() -> usize {
    1000
}

fn default_metric_hours() -> u32 {
    24
}

fn default_metric_limit() -> usize {
    1440
}

fn default_source_metric_limit() -> usize {
    20
}

fn bounded_limit(limit: usize) -> usize {
    limit.clamp(1, MAX_QUERY_LIMIT)
}

fn bounded_cold_limit(limit: usize) -> usize {
    limit.clamp(1, MAX_COLD_SEARCH_LIMIT)
}

fn bounded_export_limit(limit: usize) -> usize {
    limit.clamp(1, MAX_EXPORT_LIMIT)
}

fn event_query_from_params(query: EventsQuery) -> (EventQuery, usize) {
    (
        EventQuery {
            day: query.day,
            date_from: query.date_from,
            date_to: query.date_to,
            device: query.device,
            device_id: query.device_id,
            src_ip: query.src_ip,
            dst_ip: query.dst_ip,
            protocol: query.protocol,
            action: query.action,
            keyword: query.keyword,
            include_failed: query.include_failed,
        },
        bounded_limit(query.limit),
    )
}

fn event_query_from_search_params(query: &SearchQuery) -> EventQuery {
    EventQuery {
        day: query.day.clone(),
        date_from: query.date_from.clone(),
        date_to: query.date_to.clone(),
        device: query.device.clone(),
        device_id: query.device_id.clone(),
        src_ip: query.src_ip.clone(),
        dst_ip: query.dst_ip.clone(),
        protocol: query.protocol.clone(),
        action: query.action.clone(),
        keyword: query.keyword.clone(),
        include_failed: query.include_failed,
    }
}

fn cold_query_from_search_params(query: &SearchQuery, limit: usize) -> ColdSearchQuery {
    ColdSearchQuery {
        day: query.day.clone(),
        date_from: query.date_from.clone(),
        date_to: query.date_to.clone(),
        device: query.device.clone(),
        device_id: query.device_id.clone(),
        src_ip: query.src_ip.clone(),
        dst_ip: query.dst_ip.clone(),
        protocol: query.protocol.clone(),
        action: query.action.clone(),
        keyword: query.keyword.clone(),
        include_failed: query.include_failed,
        limit,
    }
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
    let event_stats = match DuckDbStore::open_read_only(&*state.duckdb_path)
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
        events_partial: event_stats.partial,
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
    Query(query): Query<EventsQuery>,
) -> Response {
    let (event_query, limit) = event_query_from_params(query);
    match DuckDbStore::open_read_only(&*state.duckdb_path)
        .and_then(|store| store.query_events_without_raw(&event_query, limit))
    {
        Ok(events) => Json(events).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    }
}

pub async fn minute_metrics(
    Extension(state): Extension<ApiState>,
    Query(query): Query<MinuteMetricsRequest>,
) -> Response {
    let metric_query = MinuteMetricQuery {
        hours: query.hours,
        limit: query.limit,
    };
    match DuckDbStore::open_read_only(&*state.duckdb_path)
        .and_then(|store| store.query_minute_metrics(&metric_query))
    {
        Ok(points) => Json(points).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    }
}

pub async fn hour_metrics(
    Extension(state): Extension<ApiState>,
    Query(query): Query<MinuteMetricsRequest>,
) -> Response {
    let metric_query = MinuteMetricQuery {
        hours: query.hours,
        limit: query.limit.saturating_mul(60).clamp(1, 24 * 366 * 60),
    };
    match DuckDbStore::open_read_only(&*state.duckdb_path)
        .and_then(|store| store.query_minute_metrics(&metric_query))
    {
        Ok(points) => {
            let mut buckets = std::collections::BTreeMap::<String, HourMetricResponse>::new();
            for point in points {
                let bucket_hour =
                    point.bucket_minute.chars().take(13).collect::<String>() + ":00:00Z";
                let entry = buckets
                    .entry(bucket_hour.clone())
                    .or_insert(HourMetricResponse {
                        bucket_hour,
                        total: 0,
                        parsed: 0,
                        partial: 0,
                        failed: 0,
                        raw_bytes: 0,
                    });
                entry.total += point.total;
                entry.parsed += point.parsed;
                entry.partial += point.partial;
                entry.failed += point.failed;
                entry.raw_bytes += point.raw_bytes;
            }
            let mut rows = buckets.into_values().collect::<Vec<_>>();
            rows.sort_by(|left, right| left.bucket_hour.cmp(&right.bucket_hour));
            if rows.len() > query.limit {
                rows = rows.split_off(rows.len() - query.limit);
            }
            Json(rows).into_response()
        }
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    }
}

pub async fn source_metrics(
    Extension(state): Extension<ApiState>,
    Query(query): Query<SourceMetricsRequest>,
) -> Response {
    let metric_query = SourceMetricQuery {
        hours: query.hours,
        limit: query.limit,
    };
    match DuckDbStore::open_read_only(&*state.duckdb_path)
        .and_then(|store| store.query_source_metrics(&metric_query))
    {
        Ok(points) => Json(points).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    }
}

pub async fn parser_summary(Extension(state): Extension<ApiState>) -> Response {
    match DuckDbStore::open_read_only(&*state.duckdb_path).and_then(|store| {
        let events = store.query_events_without_raw(
            &EventQuery {
                include_failed: true,
                ..EventQuery::default()
            },
            10_000,
        )?;
        let mut counts = std::collections::BTreeMap::<String, u64>::new();
        for event in events {
            if event.parse_status == ParseStatus::Failed {
                let reason = event.parse_error.unwrap_or_else(|| "unknown".to_string());
                *counts.entry(reason).or_default() += 1;
            }
        }
        Ok::<_, anyhow::Error>(
            counts
                .into_iter()
                .map(|(reason, count)| ParserSummaryResponse { reason, count })
                .collect::<Vec<_>>(),
        )
    }) {
        Ok(rows) => Json(rows).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    }
}

pub async fn parser_profiles(Extension(state): Extension<ApiState>) -> Response {
    match DuckDbStore::open_read_only(&*state.duckdb_path)
        .and_then(|store| store.list_parser_profiles())
    {
        Ok(rows) => {
            let total = rows.len();
            Json(json!({ "profiles": rows, "total": total })).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": err.to_string() })),
        )
            .into_response(),
    }
}

pub async fn parser_adaptive_rules(Extension(state): Extension<ApiState>) -> Response {
    match DuckDbStore::open_read_only(&*state.duckdb_path)
        .and_then(|store| store.list_adaptive_field_rules())
    {
        Ok(rows) => {
            let total = rows.len();
            Json(json!({ "rules": rows, "total": total })).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": err.to_string() })),
        )
            .into_response(),
    }
}

pub async fn parser_diagnostics(Extension(state): Extension<ApiState>) -> Response {
    match DuckDbStore::open_read_only(&*state.duckdb_path)
        .and_then(|store| store.list_parser_diagnostics())
    {
        Ok(rows) => {
            let total = rows.len();
            Json(json!({ "diagnostics": rows, "total": total })).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": err.to_string() })),
        )
            .into_response(),
    }
}

pub async fn parser_scopes(Extension(state): Extension<ApiState>) -> Response {
    match DuckDbStore::open_read_only(&*state.duckdb_path)
        .and_then(|store| store.list_parser_scopes())
    {
        Ok(rows) => {
            let total = rows.len();
            Json(json!({ "scopes": rows, "total": total })).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": err.to_string() })),
        )
            .into_response(),
    }
}

pub async fn ip_region(
    Extension(state): Extension<ApiState>,
    Query(query): Query<IpRegionQuery>,
) -> Response {
    let ip = query.ip.trim();
    if ip.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "ip query parameter is required".to_string(),
        )
            .into_response();
    }
    let parsed_ip = match ip.parse::<IpAddr>() {
        Ok(value) => value,
        Err(_) => return (StatusCode::BAD_REQUEST, format!("invalid ip: {ip}")).into_response(),
    };

    match read_custom_ip_regions(custom_ip_regions_path(&state)) {
        Ok(rules) => {
            if let Some(rule) = rules.iter().find(|rule| {
                rule.enabled && !rule.deleted && custom_region_matches(parsed_ip, &rule.cidr)
            }) {
                return Json(IpRegionResponse {
                    ip: ip.to_string(),
                    region: Some(rule.name.clone()),
                    country: None,
                    province: None,
                    city: None,
                    isp: None,
                    raw: Some(rule.cidr.clone()),
                })
                .into_response();
            }
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("read custom ip regions failed: {err:#}"),
            )
                .into_response()
        }
    }

    if !matches!(parsed_ip, IpAddr::V4(_)) {
        return (StatusCode::BAD_REQUEST, format!("invalid ip: {ip}")).into_response();
    }

    let store = match DuckDbStore::open(&*state.duckdb_path) {
        Ok(store) => store,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    };
    match store.get_ip_region_cache(ip) {
        Ok(Some(entry)) => {
            return Json(IpRegionResponse {
                ip: entry.ip,
                region: entry.region,
                country: entry.country,
                province: entry.province,
                city: entry.city,
                isp: entry.isp,
                raw: Some(entry.source),
            })
            .into_response()
        }
        Ok(None) => {}
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    }

    let data = IP2REGION_DATA.get_or_init(|| IP2REGION_V4_XDB.to_vec());
    let raw = match xdb_parse::search_ip(ip, data) {
        Ok(value) => value,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("ip2region lookup failed: {err}"),
            )
                .into_response()
        }
    };

    let response = parse_ip_region(ip, raw);
    let cache_entry = IpRegionCacheEntry {
        ip: response.ip.clone(),
        region: response.region.clone(),
        country: response.country.clone(),
        province: response.province.clone(),
        city: response.city.clone(),
        isp: response.isp.clone(),
        source: response
            .raw
            .clone()
            .unwrap_or_else(|| "ip2region".to_string()),
        updated_at: Utc::now().to_rfc3339(),
    };
    if let Err(err) = store.upsert_ip_region_cache(&cache_entry) {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response();
    }

    Json(response).into_response()
}

pub async fn custom_ip_regions(Extension(state): Extension<ApiState>) -> Response {
    match read_custom_ip_regions(custom_ip_regions_path(&state)) {
        Ok(rules) => Json(
            rules
                .into_iter()
                .filter(|rule| !rule.deleted)
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    }
}

pub async fn create_custom_ip_region(
    Extension(state): Extension<ApiState>,
    Json(input): Json<CustomIpRegionInput>,
) -> Response {
    match create_custom_ip_region_rule(custom_ip_regions_path(&state), input) {
        Ok(rule) => Json(rule).into_response(),
        Err(err) => (StatusCode::BAD_REQUEST, format!("{err:#}")).into_response(),
    }
}

pub async fn update_custom_ip_region(
    Extension(state): Extension<ApiState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<CustomIpRegionInput>,
) -> Response {
    match update_custom_ip_region_rule(custom_ip_regions_path(&state), &id, input) {
        Ok(rule) => Json(rule).into_response(),
        Err(err) => (StatusCode::BAD_REQUEST, format!("{err:#}")).into_response(),
    }
}

pub async fn delete_custom_ip_region(
    Extension(state): Extension<ApiState>,
    AxumPath(id): AxumPath<String>,
) -> Response {
    match delete_custom_ip_region_rule(custom_ip_regions_path(&state), &id) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => (StatusCode::BAD_REQUEST, format!("{err:#}")).into_response(),
    }
}

fn parse_ip_region(ip: &str, raw: &str) -> IpRegionResponse {
    let parts = raw
        .split('|')
        .map(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() || trimmed == "0" {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
        .collect::<Vec<_>>();

    let country = parts.first().and_then(Clone::clone);
    let cleaned = parts.into_iter().flatten().collect::<Vec<_>>();
    let display_parts = cleaned
        .iter()
        .enumerate()
        .filter_map(|(index, value)| {
            let is_last = index + 1 == cleaned.len();
            let looks_like_country_code =
                value.len() == 2 && value.chars().all(|ch| ch.is_ascii_uppercase());
            if is_last && looks_like_country_code && cleaned.len() > 1 {
                None
            } else {
                Some(value.clone())
            }
        })
        .collect::<Vec<_>>();

    IpRegionResponse {
        ip: ip.to_string(),
        region: if display_parts.is_empty() {
            None
        } else {
            Some(display_parts.join(" "))
        },
        country,
        province: display_parts.get(1).cloned(),
        city: display_parts.get(2).cloned(),
        isp: display_parts.last().cloned(),
        raw: Some(raw.to_string()),
    }
}

fn custom_ip_regions_path(state: &ApiState) -> PathBuf {
    state
        .duckdb_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
        .join("custom-ip-regions.json")
}

fn default_custom_ip_regions() -> Vec<CustomIpRegion> {
    DEFAULT_CUSTOM_IP_REGIONS
        .iter()
        .enumerate()
        .map(|(index, (cidr, name))| CustomIpRegion {
            id: format!("builtin-{index}"),
            cidr: (*cidr).to_string(),
            name: (*name).to_string(),
            note: "内置自定义网段".to_string(),
            enabled: true,
            deleted: false,
            created_at: Utc::now().to_rfc3339(),
        })
        .collect()
}

fn read_custom_ip_regions(path: PathBuf) -> anyhow::Result<Vec<CustomIpRegion>> {
    match fs::read_to_string(&path) {
        Ok(content) => {
            let mut rules = serde_json::from_str::<Vec<CustomIpRegion>>(&content)?;
            let existing = rules
                .iter()
                .map(|rule| rule.cidr.clone())
                .collect::<std::collections::HashSet<_>>();
            let existing_ids = rules
                .iter()
                .map(|rule| rule.id.clone())
                .collect::<std::collections::HashSet<_>>();
            rules.extend(
                default_custom_ip_regions().into_iter().filter(|rule| {
                    !existing.contains(&rule.cidr) && !existing_ids.contains(&rule.id)
                }),
            );
            Ok(rules)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(default_custom_ip_regions()),
        Err(err) => Err(err).with_context(|| format!("read custom ip regions {}", path.display())),
    }
}

fn write_custom_ip_regions(path: PathBuf, rules: &[CustomIpRegion]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create custom ip regions dir {}", parent.display()))?;
    }
    let content = serde_json::to_string_pretty(rules)?;
    fs::write(&path, content).with_context(|| format!("write custom ip regions {}", path.display()))
}

fn create_custom_ip_region_rule(
    path: PathBuf,
    input: CustomIpRegionInput,
) -> anyhow::Result<CustomIpRegion> {
    let cidr = input.cidr.trim();
    let name = input.name.trim();
    if cidr.is_empty() {
        return Err(anyhow!("cidr is required"));
    }
    if name.is_empty() {
        return Err(anyhow!("region name is required"));
    }
    parse_ipv4_cidr(cidr)?;

    let mut rules = read_custom_ip_regions(path.clone())?;
    let rule = CustomIpRegion {
        id: format!(
            "ipreg-{}-{}",
            Utc::now().format("%Y%m%d%H%M%S%.6f"),
            ARCHIVE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ),
        cidr: cidr.to_string(),
        name: name.to_string(),
        note: input.note.trim().to_string(),
        enabled: input.enabled,
        deleted: false,
        created_at: Utc::now().to_rfc3339(),
    };
    rules.retain(|existing| existing.cidr != rule.cidr);
    rules.push(rule.clone());
    write_custom_ip_regions(path, &rules)?;
    Ok(rule)
}

fn normalize_custom_ip_region_input(
    input: &CustomIpRegionInput,
) -> anyhow::Result<(String, String, String)> {
    let cidr = input.cidr.trim();
    let name = input.name.trim();
    if cidr.is_empty() {
        return Err(anyhow!("cidr is required"));
    }
    if name.is_empty() {
        return Err(anyhow!("region name is required"));
    }
    parse_ipv4_cidr(cidr)?;
    Ok((
        cidr.to_string(),
        name.to_string(),
        input.note.trim().to_string(),
    ))
}

fn update_custom_ip_region_rule(
    path: PathBuf,
    id: &str,
    input: CustomIpRegionInput,
) -> anyhow::Result<CustomIpRegion> {
    let (cidr, name, note) = normalize_custom_ip_region_input(&input)?;
    let mut rules = read_custom_ip_regions(path.clone())?;
    let Some(index) = rules.iter().position(|rule| rule.id == id && !rule.deleted) else {
        return Err(anyhow!("custom ip region not found: {id}"));
    };
    let created_at = rules[index].created_at.clone();
    rules.retain(|rule| rule.id == id || rule.cidr != cidr);
    let Some(rule) = rules.iter_mut().find(|rule| rule.id == id) else {
        return Err(anyhow!("custom ip region not found: {id}"));
    };
    rule.cidr = cidr;
    rule.name = name;
    rule.note = note;
    rule.enabled = input.enabled;
    rule.deleted = false;
    rule.created_at = created_at;
    let updated = rule.clone();
    write_custom_ip_regions(path, &rules)?;
    Ok(updated)
}

fn delete_custom_ip_region_rule(path: PathBuf, id: &str) -> anyhow::Result<()> {
    let mut rules = read_custom_ip_regions(path.clone())?;
    let Some(index) = rules.iter().position(|rule| rule.id == id && !rule.deleted) else {
        return Err(anyhow!("custom ip region not found: {id}"));
    };
    if rules[index].id.starts_with("builtin-") {
        rules[index].deleted = true;
        rules[index].enabled = false;
    } else {
        rules.remove(index);
    }
    write_custom_ip_regions(path, &rules)
}

fn parse_ipv4_cidr(cidr: &str) -> anyhow::Result<(u32, u32)> {
    let (addr, prefix) = cidr
        .split_once('/')
        .ok_or_else(|| anyhow!("cidr must use address/prefix format"))?;
    let ip = addr
        .parse::<Ipv4Addr>()
        .with_context(|| format!("invalid cidr address: {addr}"))?;
    let prefix = prefix
        .parse::<u32>()
        .with_context(|| format!("invalid cidr prefix: {prefix}"))?;
    if prefix > 32 {
        return Err(anyhow!("cidr prefix must be 0..32"));
    }
    let mask = if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    };
    Ok((u32::from(ip), mask))
}

fn custom_region_matches(ip: IpAddr, cidr: &str) -> bool {
    let IpAddr::V4(ip) = ip else {
        return false;
    };
    parse_ipv4_cidr(cidr)
        .map(|(network, mask)| (u32::from(ip) & mask) == (network & mask))
        .unwrap_or(false)
}

pub async fn devices(Extension(state): Extension<ApiState>) -> Response {
    match read_devices(devices_path(&state)) {
        Ok(devices) => Json(devices).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    }
}

pub async fn create_device(
    Extension(state): Extension<ApiState>,
    Json(input): Json<FirewallDeviceInput>,
) -> Response {
    match create_firewall_device(devices_path(&state), input) {
        Ok(device) => Json(device).into_response(),
        Err(err) => (StatusCode::BAD_REQUEST, format!("{err:#}")).into_response(),
    }
}

pub async fn update_device(
    Extension(state): Extension<ApiState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<FirewallDeviceInput>,
) -> Response {
    match update_firewall_device(devices_path(&state), &id, input) {
        Ok(device) => Json(device).into_response(),
        Err(err) => (StatusCode::BAD_REQUEST, format!("{err:#}")).into_response(),
    }
}

pub async fn delete_device(
    Extension(state): Extension<ApiState>,
    AxumPath(id): AxumPath<String>,
) -> Response {
    match delete_firewall_device(devices_path(&state), &id) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => (StatusCode::BAD_REQUEST, format!("{err:#}")).into_response(),
    }
}

pub async fn backfill_devices(Extension(state): Extension<ApiState>) -> Response {
    let result = read_devices(devices_path(&state)).and_then(|devices| {
        let bindings = devices
            .into_iter()
            .filter(|device| device.enabled)
            .map(|device| DeviceBinding {
                id: device.id,
                protocol: device.protocol,
                host: device.host,
                port: device.port,
            })
            .collect::<Vec<_>>();
        DuckDbStore::open(&*state.duckdb_path)
            .and_then(|store| store.backfill_device_ids(&bindings))
    });
    match result {
        Ok(updated) => Json(json!({ "updated": updated })).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    }
}

fn devices_path(state: &ApiState) -> PathBuf {
    state
        .duckdb_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
        .join("devices.json")
}

fn normalize_device_input(
    input: &FirewallDeviceInput,
) -> anyhow::Result<(String, String, String, u16, String)> {
    let name = input.name.trim();
    let host = input.host.trim();
    let protocol = input.protocol.trim().to_uppercase();
    if name.is_empty() {
        return Err(anyhow!("device name is required"));
    }
    if host.is_empty() {
        return Err(anyhow!("device host is required"));
    }
    if !matches!(protocol.as_str(), "UDP" | "TCP" | "TLS") {
        return Err(anyhow!("device protocol must be UDP, TCP or TLS"));
    }
    if input.port == 0 {
        return Err(anyhow!("device port is required"));
    }
    Ok((
        name.to_string(),
        host.to_string(),
        protocol,
        input.port,
        input.note.trim().to_string(),
    ))
}

fn create_firewall_device(
    path: PathBuf,
    input: FirewallDeviceInput,
) -> anyhow::Result<FirewallDevice> {
    let (name, host, protocol, port, note) = normalize_device_input(&input)?;
    let mut devices = read_devices(path.clone())?;
    let device = FirewallDevice {
        id: format!(
            "dev-{}-{}",
            Utc::now().format("%Y%m%d%H%M%S%.6f"),
            ARCHIVE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ),
        name,
        host,
        protocol,
        port,
        note,
        enabled: input.enabled,
        created_at: Utc::now().to_rfc3339(),
    };
    devices.push(device.clone());
    write_devices(path, &devices)?;
    Ok(device)
}

fn update_firewall_device(
    path: PathBuf,
    id: &str,
    input: FirewallDeviceInput,
) -> anyhow::Result<FirewallDevice> {
    let (name, host, protocol, port, note) = normalize_device_input(&input)?;
    let mut devices = read_devices(path.clone())?;
    let Some(device) = devices.iter_mut().find(|device| device.id == id) else {
        return Err(anyhow!("device not found: {id}"));
    };
    device.name = name;
    device.host = host;
    device.protocol = protocol;
    device.port = port;
    device.note = note;
    device.enabled = input.enabled;
    let updated = device.clone();
    write_devices(path, &devices)?;
    Ok(updated)
}

fn delete_firewall_device(path: PathBuf, id: &str) -> anyhow::Result<()> {
    let mut devices = read_devices(path.clone())?;
    let before = devices.len();
    devices.retain(|device| device.id != id);
    if devices.len() == before {
        return Err(anyhow!("device not found: {id}"));
    }
    write_devices(path, &devices)
}

fn read_devices(path: PathBuf) -> anyhow::Result<Vec<FirewallDevice>> {
    match fs::read_to_string(&path) {
        Ok(content) => Ok(serde_json::from_str(&content)?),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(err) => Err(err).with_context(|| format!("read devices {}", path.display())),
    }
}

fn write_devices(path: PathBuf, devices: &[FirewallDevice]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create devices dir {}", parent.display()))?;
    }
    let content = serde_json::to_string_pretty(devices)?;
    fs::write(&path, content).with_context(|| format!("write devices {}", path.display()))
}

pub async fn cold_search(
    Extension(state): Extension<ApiState>,
    Query(query): Query<ColdSearchQuery>,
) -> Response {
    let frozen_dir = state.frozen_dir.as_ref().clone();
    let parquet_dir = state.parquet_dir.as_ref().clone();
    let duckdb_path = state.duckdb_path.as_ref().clone();
    let limit = bounded_cold_limit(query.limit);
    let result = tokio::task::spawn_blocking(move || {
        search_history_archives(&duckdb_path, &parquet_dir, &frozen_dir, query, limit)
    })
    .await
    .map_err(|err| anyhow!("cold search worker failed: {err}"))
    .and_then(|result| result);

    match result {
        Ok(response) => Json(response).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    }
}

pub async fn search(
    Extension(state): Extension<ApiState>,
    Query(query): Query<SearchQuery>,
) -> Response {
    let limit = bounded_cold_limit(query.limit);
    let scope = query.scope.to_ascii_lowercase();
    if !matches!(scope.as_str(), "hot" | "archive" | "all") {
        return (
            StatusCode::BAD_REQUEST,
            "scope must be hot, archive, or all".to_string(),
        )
            .into_response();
    }

    let mut rows = Vec::new();
    if scope == "hot" || scope == "all" {
        let event_query = event_query_from_search_params(&query);
        match DuckDbStore::open_read_only(&*state.duckdb_path) {
            Ok(store) => match store.query_events_without_raw(&event_query, limit) {
                Ok(events) => rows.extend(
                    events
                        .into_iter()
                        .map(|event| unified_search_row(&state, &store, "hot", None, event)),
                ),
                Err(err) => {
                    return (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response()
                }
            },
            Err(err) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response()
            }
        }
    }

    if rows.len() < limit && (scope == "archive" || scope == "all") {
        let duckdb_path = state.duckdb_path.as_ref().clone();
        let parquet_dir = state.parquet_dir.as_ref().clone();
        let frozen_dir = state.frozen_dir.as_ref().clone();
        let remaining = limit.saturating_sub(rows.len());
        let cold_query = cold_query_from_search_params(&query, remaining);
        let result = tokio::task::spawn_blocking(move || {
            search_history_archives(
                &duckdb_path,
                &parquet_dir,
                &frozen_dir,
                cold_query,
                remaining,
            )
        })
        .await
        .map_err(|err| anyhow!("search worker failed: {err}"))
        .and_then(|result| result);
        match result {
            Ok(response) => match DuckDbStore::open_read_only(&*state.duckdb_path) {
                Ok(store) => rows.extend(
                    response
                        .events
                        .into_iter()
                        .map(|event| unified_search_row(&state, &store, "archive", None, event)),
                ),
                Err(err) => {
                    return (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response()
                }
            },
            Err(err) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response()
            }
        }
    }

    rows.truncate(limit);
    Json(rows).into_response()
}

pub async fn search_export_csv(
    Extension(state): Extension<ApiState>,
    Query(query): Query<SearchQuery>,
) -> Response {
    let limit = bounded_export_limit(query.limit);
    let state_for_worker = state.clone();
    let result = tokio::task::spawn_blocking(move || {
        let events = collect_search_events(&state_for_worker, query, limit)?;
        let mut writer = csv::Writer::from_writer(Vec::new());
        for event in events {
            writer.serialize(event)?;
        }
        let bytes = writer.into_inner()?;
        Ok::<_, anyhow::Error>(String::from_utf8(bytes)?)
    })
    .await
    .map_err(|err| anyhow!("search export worker failed: {err}"))
    .and_then(|result| result);

    match result {
        Ok(csv) => ([(header::CONTENT_TYPE, "text/csv; charset=utf-8")], csv).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    }
}

pub async fn create_export_job(
    Extension(state): Extension<ApiState>,
    Json(input): Json<ExportJobRequest>,
) -> Response {
    if let Err(err) = validate_export_job_request(&input) {
        return (StatusCode::BAD_REQUEST, err.to_string()).into_response();
    }
    let export_dir = export_dir(&state);
    if let Err(err) = cleanup_export_jobs(&export_dir) {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response();
    }
    if let Err(err) = fs::create_dir_all(&export_dir) {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response();
    }

    let now = Utc::now();
    let job_id = format!(
        "export-{}-{}",
        now.format("%Y%m%d%H%M%S%.6f"),
        ARCHIVE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let file_name = format!("{job_id}.{}", input.format.extension());
    let job = ExportJob {
        job_id: job_id.clone(),
        status: "queued".to_string(),
        scope: input.scope.clone(),
        format: input.format,
        file_name: file_name.clone(),
        download_url: format!("/api/export/jobs/{job_id}/download"),
        rows: 0,
        file_bytes: 0,
        error: None,
        created_at: now.to_rfc3339(),
        updated_at: now.to_rfc3339(),
        expires_at: (now + chrono::Duration::seconds(EXPORT_TTL.as_secs() as i64)).to_rfc3339(),
    };
    export_jobs_map()
        .lock()
        .expect("export jobs lock poisoned")
        .insert(job_id.clone(), job.clone());

    let state_for_worker = state.clone();
    tokio::spawn(async move {
        let status_job_id = job_id.clone();
        let result = tokio::task::spawn_blocking(move || {
            run_export_job(&state_for_worker, input, &export_dir, &job_id, &file_name)
        })
        .await
        .map_err(|err| anyhow!("export job worker failed: {err}"))
        .and_then(|result| result);

        match result {
            Ok((rows, file_bytes)) => {
                update_export_job(&status_job_id, "completed", rows, file_bytes, None)
            }
            Err(err) => update_export_job(&status_job_id, "failed", 0, 0, Some(format!("{err:#}"))),
        }
    });

    Json(job).into_response()
}

pub async fn export_jobs(Extension(state): Extension<ApiState>) -> Response {
    let export_dir = export_dir(&state);
    if let Err(err) = cleanup_export_jobs(&export_dir) {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response();
    }
    let mut jobs = export_jobs_map()
        .lock()
        .expect("export jobs lock poisoned")
        .values()
        .cloned()
        .collect::<Vec<_>>();
    jobs.sort_by(|left, right| right.created_at.cmp(&left.created_at));
    Json(jobs).into_response()
}

pub async fn export_job(
    Extension(state): Extension<ApiState>,
    AxumPath(id): AxumPath<String>,
) -> Response {
    let export_dir = export_dir(&state);
    if let Err(err) = cleanup_export_jobs(&export_dir) {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response();
    }
    match export_jobs_map()
        .lock()
        .expect("export jobs lock poisoned")
        .get(&id)
        .cloned()
    {
        Some(job) => Json(job).into_response(),
        None => (StatusCode::NOT_FOUND, "export job not found").into_response(),
    }
}

pub async fn download_export_job(
    Extension(state): Extension<ApiState>,
    AxumPath(id): AxumPath<String>,
) -> Response {
    let export_dir = export_dir(&state);
    if let Err(err) = cleanup_export_jobs(&export_dir) {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response();
    }
    let Some(job) = export_jobs_map()
        .lock()
        .expect("export jobs lock poisoned")
        .get(&id)
        .cloned()
    else {
        return (StatusCode::NOT_FOUND, "export job not found").into_response();
    };
    if job.status != "completed" {
        return (StatusCode::CONFLICT, "export job is not completed").into_response();
    }
    let path = export_dir.join(&job.file_name);
    match fs::read(&path) {
        Ok(bytes) => (
            [
                (header::CONTENT_TYPE, job.format.content_type().to_string()),
                (
                    header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{}\"", job.file_name),
                ),
            ],
            bytes,
        )
            .into_response(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            (StatusCode::NOT_FOUND, "export file not found").into_response()
        }
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    }
}

fn unified_search_row(
    state: &ApiState,
    store: &DuckDbStore,
    result_source: &str,
    archive_path: Option<PathBuf>,
    event: CanonicalEvent,
) -> UnifiedSearchRow {
    let src_geo_region = event
        .src_ip
        .as_deref()
        .and_then(|ip| lookup_ip_region_name(state, store, ip).ok().flatten());
    let dst_geo_region = event
        .dst_ip
        .as_deref()
        .and_then(|ip| lookup_ip_region_name(state, store, ip).ok().flatten());
    let geo_region = src_geo_region.clone().or_else(|| dst_geo_region.clone());
    let archive_path = archive_path.or_else(|| {
        (result_source == "archive")
            .then(|| {
                event
                    .source_addr
                    .strip_prefix("parquet://")
                    .or_else(|| event.source_addr.strip_prefix("frozen://"))
            })
            .flatten()
            .map(PathBuf::from)
    });
    UnifiedSearchRow {
        result_source: result_source.to_string(),
        archive_path,
        device_name: None,
        geo_region,
        src_geo_region,
        dst_geo_region,
        event,
    }
}

fn export_jobs_map() -> &'static Mutex<HashMap<String, ExportJob>> {
    EXPORT_JOBS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn export_dir(state: &ApiState) -> PathBuf {
    state
        .duckdb_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
        .join("exports")
}

fn validate_export_job_request(input: &ExportJobRequest) -> anyhow::Result<()> {
    let scope = input.scope.to_ascii_lowercase();
    if !matches!(scope.as_str(), "hot" | "archive" | "all") {
        return Err(anyhow!("scope must be hot, archive, or all"));
    }
    if let (Some(start), Some(end)) = (
        input.date_from.as_deref().and_then(parse_day),
        input.date_to.as_deref().and_then(parse_day),
    ) {
        if end < start {
            return Err(anyhow!(
                "date_to must be greater than or equal to date_from"
            ));
        }
    }
    Ok(())
}

fn parse_day(value: &str) -> Option<chrono::NaiveDate> {
    normalize_day_filter(value)
        .and_then(|day| chrono::NaiveDate::parse_from_str(&day, "%Y%m%d").ok())
}

fn search_query_from_export_request(input: &ExportJobRequest) -> SearchQuery {
    SearchQuery {
        scope: input.scope.clone(),
        day: input.day.clone(),
        date_from: input.date_from.clone(),
        date_to: input.date_to.clone(),
        device: input.device.clone(),
        device_id: input.device_id.clone(),
        src_ip: input.src_ip.clone(),
        dst_ip: input.dst_ip.clone(),
        protocol: input.protocol.clone(),
        action: input.action.clone(),
        keyword: input.keyword.clone(),
        include_failed: input.include_failed,
        limit: bounded_export_limit(input.limit),
    }
}

fn collect_search_events(
    state: &ApiState,
    query: SearchQuery,
    limit: usize,
) -> anyhow::Result<Vec<CanonicalEvent>> {
    let scope = query.scope.to_ascii_lowercase();
    if !matches!(scope.as_str(), "hot" | "archive" | "all") {
        return Err(anyhow!("scope must be hot, archive, or all"));
    }

    let mut events = Vec::new();
    if scope == "hot" || scope == "all" {
        let event_query = event_query_from_search_params(&query);
        let store = DuckDbStore::open_read_only(&*state.duckdb_path)?;
        let mut hot = store.query_events_without_raw(&event_query, limit)?;
        events.append(&mut hot);
    }

    if events.len() < limit && (scope == "archive" || scope == "all") {
        if let Some(days) = query_date_days(&query)? {
            for day in days {
                if events.len() >= limit {
                    break;
                }
                let remaining = limit.saturating_sub(events.len());
                let mut day_query = query.clone();
                day_query.day = Some(day);
                day_query.date_from = None;
                day_query.date_to = None;
                let cold_query = cold_query_from_search_params(&day_query, remaining);
                let mut cold = search_history_archives(
                    &state.duckdb_path,
                    &state.parquet_dir,
                    &state.frozen_dir,
                    cold_query,
                    remaining,
                )?
                .events;
                events.append(&mut cold);
            }
        } else {
            let remaining = limit.saturating_sub(events.len());
            let cold_query = cold_query_from_search_params(&query, remaining);
            let mut cold = search_history_archives(
                &state.duckdb_path,
                &state.parquet_dir,
                &state.frozen_dir,
                cold_query,
                remaining,
            )?
            .events;
            events.append(&mut cold);
        }
    }

    events.truncate(limit);
    Ok(events)
}

fn query_date_days(query: &SearchQuery) -> anyhow::Result<Option<Vec<String>>> {
    let (Some(start), Some(end)) = (
        query.date_from.as_deref().and_then(parse_day),
        query.date_to.as_deref().and_then(parse_day),
    ) else {
        return Ok(None);
    };
    if end < start {
        return Err(anyhow!(
            "date_to must be greater than or equal to date_from"
        ));
    }
    let span = (end - start).num_days() + 1;
    if span > 366 {
        return Err(anyhow!("date range must not exceed 366 days"));
    }
    let mut days = Vec::new();
    for offset in 0..span {
        let day = start + chrono::Duration::days(offset);
        days.push(day.format("%Y-%m-%d").to_string());
    }
    Ok(Some(days))
}

fn run_export_job(
    state: &ApiState,
    input: ExportJobRequest,
    export_dir: &Path,
    job_id: &str,
    file_name: &str,
) -> anyhow::Result<(usize, u64)> {
    update_export_job(job_id, "running", 0, 0, None);
    fs::create_dir_all(export_dir).context("create export directory")?;
    let path = export_dir.join(file_name);
    let temp_path = export_dir.join(format!("{file_name}.tmp"));
    let query = search_query_from_export_request(&input);
    let limit = bounded_export_limit(input.limit);
    let events = collect_export_events(state, query, limit)?;

    let rows = events.len();
    match input.format {
        ExportFormat::Csv => {
            let file = File::create(&temp_path)
                .with_context(|| format!("create {}", temp_path.display()))?;
            let mut writer = csv::Writer::from_writer(file);
            for event in &events {
                writer.serialize(event).context("write export csv row")?;
            }
            writer.flush().context("finish csv export")?;
        }
        ExportFormat::Zst => {
            let file = File::create(&temp_path)
                .with_context(|| format!("create {}", temp_path.display()))?;
            let encoder =
                zstd::stream::write::Encoder::new(file, 19).context("create zstd encoder")?;
            let mut writer = csv::Writer::from_writer(encoder);
            for event in &events {
                writer.serialize(event).context("write export csv row")?;
            }
            let encoder = writer.into_inner().context("finish csv writer")?;
            encoder.finish().context("finish zstd export")?;
        }
        ExportFormat::Parquet => {
            let store =
                DuckDbStore::open(&*state.duckdb_path).context("open duckdb for parquet export")?;
            store.archive_events_parquet(&temp_path, &events)?;
        }
    }
    fs::rename(&temp_path, &path)
        .with_context(|| format!("rename {} to {}", temp_path.display(), path.display()))?;
    let file_bytes = fs::metadata(&path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    Ok((rows, file_bytes))
}

fn collect_export_events(
    state: &ApiState,
    query: SearchQuery,
    limit: usize,
) -> anyhow::Result<Vec<CanonicalEvent>> {
    let scope = query.scope.to_ascii_lowercase();
    if !matches!(scope.as_str(), "hot" | "archive" | "all") {
        return Err(anyhow!("scope must be hot, archive, or all"));
    }

    let mut events = Vec::new();
    if scope == "hot" || scope == "all" {
        let event_query = event_query_from_search_params(&query);
        let store = DuckDbStore::open_read_only(&*state.duckdb_path)?;
        let mut hot = store.query_events_without_raw(&event_query, limit)?;
        events.append(&mut hot);
    }

    if events.len() < limit && (scope == "archive" || scope == "all") {
        let days = query_date_days(&query)?
            .unwrap_or_else(|| query.day.clone().into_iter().collect::<Vec<_>>());
        for day in days {
            if events.len() >= limit {
                break;
            }
            let remaining = limit.saturating_sub(events.len());
            let mut day_query = query.clone();
            day_query.day = Some(day);
            day_query.date_from = None;
            day_query.date_to = None;
            let cold_query = cold_query_from_search_params(&day_query, remaining);
            let mut cold = search_history_archives_indexed_only(
                &state.duckdb_path,
                &state.parquet_dir,
                &state.frozen_dir,
                &cold_query,
                remaining,
            )?;
            events.append(&mut cold);
        }
    }

    events.truncate(limit);
    Ok(events)
}

fn search_history_archives_indexed_only(
    duckdb_path: &Path,
    parquet_dir: &Path,
    frozen_dir: &Path,
    query: &ColdSearchQuery,
    limit: usize,
) -> anyhow::Result<Vec<CanonicalEvent>> {
    let day_filter = query.day.as_deref().and_then(normalize_day_filter);
    let parquet_files = cold_parquet_files(parquet_dir, day_filter.as_deref())?;
    let mut events = search_parquet_archives(&parquet_files, query, limit)?;
    if events.len() >= limit {
        events.truncate(limit);
        return Ok(events);
    }

    let Some(day) = day_filter.as_deref() else {
        return Ok(events);
    };
    let indexed = DuckDbStore::open_read_only(duckdb_path)
        .and_then(|store| store.list_frozen_archive_index(Some(day), 10_000))
        .unwrap_or_default();
    let device = query.device.as_deref().filter(|value| !value.is_empty());
    let frozen_files = indexed
        .into_iter()
        .filter(|row| {
            device.is_none_or(|value| {
                row.source_addr.contains(value) || row.archive_path.contains(value)
            })
        })
        .map(|row| {
            let path = PathBuf::from(row.archive_path);
            if path.is_absolute() {
                path
            } else {
                frozen_dir.join(path)
            }
        })
        .filter(|path| path.exists())
        .collect::<Vec<_>>();
    if frozen_files.is_empty() {
        return Ok(events);
    }
    let response = search_cold_archives(
        &frozen_files,
        query,
        limit.saturating_sub(events.len()),
        day_filter.as_deref(),
    )?;
    events.extend(response.events);
    events.truncate(limit);
    Ok(events)
}

fn update_export_job(
    job_id: &str,
    status: &str,
    rows: usize,
    file_bytes: u64,
    error: Option<String>,
) {
    if let Some(job) = export_jobs_map()
        .lock()
        .expect("export jobs lock poisoned")
        .get_mut(job_id)
    {
        job.status = status.to_string();
        if rows > 0 || status == "completed" {
            job.rows = rows;
        }
        if file_bytes > 0 || status == "completed" {
            job.file_bytes = file_bytes;
        }
        job.error = error;
        job.updated_at = Utc::now().to_rfc3339();
    }
}

fn cleanup_export_jobs(export_dir: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(export_dir).context("create export directory")?;
    let cutoff = SystemTime::now()
        .checked_sub(EXPORT_TTL)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    for entry in fs::read_dir(export_dir).context("read export directory")? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.modified().unwrap_or(SystemTime::now()) < cutoff {
            let _ = fs::remove_file(entry.path());
        }
    }

    export_jobs_map()
        .lock()
        .expect("export jobs lock poisoned")
        .retain(|_, job| {
            chrono::DateTime::parse_from_rfc3339(&job.expires_at)
                .map(|expires_at| expires_at.with_timezone(&Utc) > Utc::now())
                .unwrap_or(false)
        });
    Ok(())
}

fn lookup_ip_region_name(
    state: &ApiState,
    store: &DuckDbStore,
    ip: &str,
) -> anyhow::Result<Option<String>> {
    let parsed_ip = match ip.trim().parse::<IpAddr>() {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    if let Some(rule) = read_custom_ip_regions(custom_ip_regions_path(state))?
        .into_iter()
        .find(|rule| rule.enabled && !rule.deleted && custom_region_matches(parsed_ip, &rule.cidr))
    {
        return Ok(Some(rule.name));
    }
    let IpAddr::V4(_) = parsed_ip else {
        return Ok(None);
    };
    if !is_public_ipv4(parsed_ip) {
        return Ok(None);
    }
    if let Some(entry) = store.get_ip_region_cache(ip)? {
        return Ok(entry.region);
    }
    let data = IP2REGION_DATA.get_or_init(|| IP2REGION_V4_XDB.to_vec());
    let raw = xdb_parse::search_ip(ip, data)?;
    let response = parse_ip_region(ip, raw);
    store.upsert_ip_region_cache(&IpRegionCacheEntry {
        ip: response.ip,
        region: response.region.clone(),
        country: response.country,
        province: response.province,
        city: response.city,
        isp: response.isp,
        source: response.raw.unwrap_or_else(|| "ip2region".to_string()),
        updated_at: Utc::now().to_rfc3339(),
    })?;
    Ok(response.region)
}

fn is_public_ipv4(ip: IpAddr) -> bool {
    let IpAddr::V4(ip) = ip else {
        return false;
    };
    let [a, b, _, _] = ip.octets();
    if a == 0 || a == 10 || a == 127 || a >= 224 {
        return false;
    }
    if a == 172 && (16..=31).contains(&b) {
        return false;
    }
    if a == 192 && b == 168 {
        return false;
    }
    if a == 169 && b == 254 {
        return false;
    }
    if a == 100 && (64..=127).contains(&b) {
        return false;
    }
    true
}

pub async fn archive_index(
    Extension(state): Extension<ApiState>,
    Query(query): Query<ArchiveIndexQuery>,
) -> Response {
    match DuckDbStore::open_read_only(&*state.duckdb_path)
        .and_then(|store| store.list_frozen_archive_index(query.day.as_deref(), query.limit))
    {
        Ok(rows) => Json(rows).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    }
}

pub async fn archive_days(Extension(state): Extension<ApiState>) -> Response {
    let result = DuckDbStore::open(&*state.duckdb_path).and_then(|store| {
        let mut days = store
            .list_frozen_archive_index(None, 10_000)?
            .into_iter()
            .map(|row| row.day)
            .filter(|day| day != "unknown")
            .collect::<Vec<_>>();
        for file in list_archive_files(&*state.parquet_dir).unwrap_or_default() {
            if let Some(day) = infer_day_from_path(&file.path) {
                days.push(day);
            }
        }
        for file in list_frozen_files(&*state.frozen_dir).unwrap_or_default() {
            if let Some(day) = infer_day_from_path(&file.path) {
                days.push(day);
            }
        }
        days.sort();
        days.dedup();
        Ok(days)
    });
    match result {
        Ok(days) => Json(days).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    }
}

pub async fn rebuild_archive_index(Extension(state): Extension<ApiState>) -> Response {
    let result = DuckDbStore::open_read_only(&*state.duckdb_path).and_then(|store| {
        let files = list_frozen_files(&*state.frozen_dir)?;
        let mut indexed = 0_usize;
        for file in files {
            let metadata = inspect_frozen_archive_for_index(&state.frozen_dir, &file)?;
            store.upsert_frozen_archive_index_with_times(
                &metadata.archive_path,
                &metadata.day,
                &metadata.source_addr,
                metadata.bytes,
                metadata.line_count,
                metadata.first_seen.as_deref(),
                metadata.last_seen.as_deref(),
            )?;
            indexed += 1;
        }
        Ok(indexed)
    });
    match result {
        Ok(indexed) => Json(json!({ "indexed": indexed })).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    }
}

struct FrozenArchiveIndexMetadata {
    archive_path: String,
    day: String,
    source_addr: String,
    bytes: u64,
    line_count: u64,
    first_seen: Option<String>,
    last_seen: Option<String>,
}

fn inspect_frozen_archive_for_index(
    frozen_dir: &Path,
    file: &FrozenFile,
) -> anyhow::Result<FrozenArchiveIndexMetadata> {
    let archive_path = relative_archive_path(frozen_dir, &file.path);
    let mut day = infer_day_from_path(&file.path).unwrap_or_else(|| "unknown".to_string());
    let mut source_addr = format!("frozen://{archive_path}");
    let mut line_count = 0_u64;

    if is_raw_zst_archive(&file.path) {
        line_count = read_frozen_raw(&file.path)?.len() as u64;
    } else if file
        .path
        .file_name()
        .and_then(|value| value.to_str())
        .is_some_and(|name| name.starts_with("raw-import-") && name.ends_with(".tar.zst"))
    {
        let archive_file = File::open(&file.path)
            .with_context(|| format!("open frozen archive {}", file.path.display()))?;
        let decoder = zstd::stream::read::Decoder::new(archive_file)
            .with_context(|| format!("decode frozen archive {}", file.path.display()))?;
        let mut archive = tar::Archive::new(decoder);
        for entry in archive.entries().context("read frozen archive entries")? {
            let entry = entry.context("read frozen archive entry")?;
            if !entry.header().entry_type().is_file() {
                continue;
            }
            let entry_path = entry.path()?.to_string_lossy().into_owned();
            if let Some(entry_day) = infer_day_from_text(&entry_path) {
                day = entry_day;
            }
            if source_addr == format!("frozen://{archive_path}") {
                source_addr = format!("frozen://{entry_path}");
            }
            let mut reader: Box<dyn BufRead> = if entry_path.ends_with(".gz") {
                Box::new(BufReader::new(flate2::read::GzDecoder::new(entry)))
            } else {
                Box::new(BufReader::new(entry))
            };
            line_count += count_lines(&mut *reader)?;
        }
    }

    let (first_seen, last_seen) = if day == "unknown" {
        (None, None)
    } else {
        (
            Some(format!("{day}T00:00:00Z")),
            Some(format!("{day}T23:59:59Z")),
        )
    };

    Ok(FrozenArchiveIndexMetadata {
        archive_path,
        day,
        source_addr,
        bytes: file.bytes,
        line_count,
        first_seen,
        last_seen,
    })
}

fn relative_archive_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .trim_start_matches(|ch| ch == '/' || ch == '\\')
        .to_string()
}

fn count_lines(reader: &mut dyn BufRead) -> anyhow::Result<u64> {
    let mut count = 0_u64;
    let mut buffer = Vec::new();
    loop {
        buffer.clear();
        let bytes = reader.read_until(b'\n', &mut buffer)?;
        if bytes == 0 {
            break;
        }
        count += 1;
    }
    Ok(count)
}

fn search_history_archives(
    duckdb_path: &Path,
    parquet_dir: &Path,
    frozen_dir: &Path,
    query: ColdSearchQuery,
    limit: usize,
) -> anyhow::Result<ColdSearchResponse> {
    let day_filter = query.day.as_deref().and_then(normalize_day_filter);
    let parquet_files = cold_parquet_files(parquet_dir, day_filter.as_deref())?;
    let mut events = search_parquet_archives(&parquet_files, &query, limit)?;
    let mut scanned_lines = 0_u64;
    let mut limited = events.len() >= limit;
    let frozen_files = if limited {
        indexed_or_all_frozen_files(duckdb_path, frozen_dir, &query, day_filter.as_deref())?
    } else {
        let frozen_files =
            indexed_or_all_frozen_files(duckdb_path, frozen_dir, &query, day_filter.as_deref())?;
        let response = search_cold_archives(
            &frozen_files,
            &query,
            limit.saturating_sub(events.len()),
            day_filter.as_deref(),
        )?;
        scanned_lines = response.scanned_lines;
        limited = response.limited;
        events.extend(response.events);
        frozen_files
    };

    Ok(ColdSearchResponse {
        files: parquet_files.len() + frozen_files.len(),
        scanned_lines,
        matched: events.len(),
        limited,
        events,
    })
}

fn indexed_or_all_frozen_files(
    duckdb_path: &Path,
    frozen_dir: &Path,
    query: &ColdSearchQuery,
    day_filter: Option<&str>,
) -> anyhow::Result<Vec<PathBuf>> {
    if let Some(day) = day_filter {
        let source_addr = query.device.as_deref().filter(|value| !value.is_empty());
        let indexed = DuckDbStore::open_read_only(duckdb_path)
            .and_then(|store| store.find_frozen_archives(day, source_addr))
            .unwrap_or_default();
        if !indexed.is_empty() {
            return Ok(indexed
                .into_iter()
                .map(|path| {
                    let path = PathBuf::from(path);
                    if path.is_absolute() {
                        path
                    } else {
                        frozen_dir.join(path)
                    }
                })
                .collect());
        }
    }
    cold_archive_files(frozen_dir, day_filter)
}

fn search_parquet_archives(
    parquet_files: &[PathBuf],
    query: &ColdSearchQuery,
    limit: usize,
) -> anyhow::Result<Vec<CanonicalEvent>> {
    if parquet_files.is_empty() || limit == 0 {
        return Ok(Vec::new());
    }
    let conn = Connection::open_in_memory().context("open parquet search duckdb")?;
    let mut events = Vec::new();
    for path in parquet_files {
        let columns = parquet_columns(&conn, path)?;
        let mut clauses = Vec::new();
        let mut values = Vec::new();
        if let Some(value) = query.device.as_deref().filter(|value| !value.is_empty()) {
            clauses.push("COALESCE(source_addr, '') LIKE ?");
            values.push(format!("%{value}%"));
        }
        if let Some(value) = query.device_id.as_deref().filter(|value| !value.is_empty()) {
            if columns.contains("device_id") {
                clauses.push("device_id = ?");
                values.push(value.to_string());
            } else {
                continue;
            }
        }
        if let Some(value) = query.day.as_deref().and_then(normalize_day_filter) {
            let day = format!("{}-{}-{}", &value[0..4], &value[4..6], &value[6..8]);
            clauses.push("(COALESCE(ingest_time, '') LIKE ? OR COALESCE(event_time, '') LIKE ?)");
            values.push(format!("{day}%"));
            values.push(format!("{day}%"));
        }
        if let Some(value) = query.date_from.as_deref().and_then(normalize_day_filter) {
            let day = format!("{}-{}-{}", &value[0..4], &value[4..6], &value[6..8]);
            clauses.push("substr(COALESCE(event_time, ingest_time), 1, 10) >= ?");
            values.push(day);
        }
        if let Some(value) = query.date_to.as_deref().and_then(normalize_day_filter) {
            let day = format!("{}-{}-{}", &value[0..4], &value[4..6], &value[6..8]);
            clauses.push("substr(COALESCE(event_time, ingest_time), 1, 10) <= ?");
            values.push(day);
        }
        if let Some(value) = query.src_ip.as_deref().filter(|value| !value.is_empty()) {
            clauses.push("src_ip = ?");
            values.push(value.to_string());
        }
        if let Some(value) = query.dst_ip.as_deref().filter(|value| !value.is_empty()) {
            clauses.push("dst_ip = ?");
            values.push(value.to_string());
        }
        if let Some(value) = query.protocol.as_deref().filter(|value| !value.is_empty()) {
            clauses.push("upper(protocol) = ?");
            values.push(
                normalize_query_protocol(Some(value)).unwrap_or_else(|| value.to_ascii_uppercase()),
            );
        }
        if let Some(value) = query.action.as_deref().filter(|value| !value.is_empty()) {
            clauses.push("lower(action) = ?");
            values.push(value.to_ascii_lowercase());
        }
        if let Some(value) = query.keyword.as_deref().filter(|value| !value.is_empty()) {
            if columns.contains("raw") {
                clauses.push("raw LIKE ?");
                values.push(format!("%{value}%"));
            } else {
                continue;
            }
        }
        if !query.include_failed {
            clauses.push("parse_status <> 'failed'");
        }
        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", clauses.join(" AND "))
        };
        let sql_path = path.to_string_lossy().replace('\'', "''");
        let source_expr = if columns.contains("source_addr") {
            format!("COALESCE(source_addr, 'parquet://{}')", sql_path)
        } else {
            format!("'parquet://{}'", sql_path)
        };
        let device_id_expr = if columns.contains("device_id") {
            "device_id".to_string()
        } else {
            "NULL AS device_id".to_string()
        };
        let event_id_expr = if columns.contains("event_id") {
            "COALESCE(event_id, 'parquet-' || row_number() OVER ())".to_string()
        } else {
            "'parquet-' || row_number() OVER ()".to_string()
        };
        let raw_expr = if columns.contains("raw") {
            "COALESCE(raw, '')".to_string()
        } else {
            "''".to_string()
        };
        let parse_error_expr = if columns.contains("parse_error") {
            "COALESCE(parse_error, '')".to_string()
        } else {
            "''".to_string()
        };
        let sql = format!(
            r#"
            SELECT
              event_id,
              ingest_time,
              source_addr,
              device_id,
              event_time,
              vendor,
              product,
              src_ip,
              src_port,
              dst_ip,
              dst_port,
              protocol,
              action,
              severity,
              COALESCE(raw, '') AS raw,
              parse_status,
              parse_error
            FROM (
              SELECT
                {event_id_expr} AS event_id,
                ingest_time,
                {source_expr} AS source_addr,
                {device_id_expr},
                event_time,
                vendor,
                product,
                src_ip,
                src_port,
                dst_ip,
                dst_port,
                protocol,
                action,
                severity,
                {raw_expr} AS raw,
                parse_status,
                {parse_error_expr} AS parse_error
              FROM read_parquet('{}')
            )
            {where_clause}
            ORDER BY ingest_time DESC
            LIMIT {}
            "#,
            sql_path,
            limit.saturating_sub(events.len())
        );
        let mut stmt = conn
            .prepare(&sql)
            .with_context(|| format!("prepare parquet search {}", path.display()))?;
        let rows = stmt.query_map(params_from_iter(values.iter()), parquet_row_to_event)?;
        for row in rows {
            events.push(row?);
            if events.len() >= limit {
                return Ok(events);
            }
        }
    }
    Ok(events)
}

fn parquet_columns(
    conn: &Connection,
    path: &Path,
) -> anyhow::Result<std::collections::HashSet<String>> {
    let sql_path = path.to_string_lossy().replace('\'', "''");
    let sql = format!("DESCRIBE SELECT * FROM read_parquet('{sql_path}')");
    let mut stmt = conn
        .prepare(&sql)
        .with_context(|| format!("describe parquet {}", path.display()))?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let columns = rows
        .collect::<duckdb::Result<Vec<_>>>()?
        .into_iter()
        .map(|value| value.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    Ok(columns)
}

pub async fn export_csv(
    Extension(state): Extension<ApiState>,
    Query(query): Query<EventsQuery>,
) -> Response {
    let (event_query, limit) = event_query_from_params(query);
    let result = DuckDbStore::open_read_only(&*state.duckdb_path).and_then(|store| {
        let events = store.query_events_without_raw(&event_query, limit)?;
        let mut writer = csv::Writer::from_writer(Vec::new());
        for event in events {
            writer.serialize(event)?;
        }
        let bytes = writer.into_inner()?;
        Ok(String::from_utf8(bytes)?)
    });

    match result {
        Ok(csv) => ([(header::CONTENT_TYPE, "text/csv; charset=utf-8")], csv).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    }
}

fn search_cold_archives(
    archives: &[PathBuf],
    query: &ColdSearchQuery,
    limit: usize,
    day_filter: Option<&str>,
) -> anyhow::Result<ColdSearchResponse> {
    let mut scanned_lines = 0_u64;
    let mut events = Vec::new();
    let parser = ParserEngine::new();

    for archive_path in archives {
        if is_raw_zst_archive(archive_path) {
            let file = File::open(archive_path)
                .with_context(|| format!("open cold raw archive {}", archive_path.display()))?;
            let decoder = zstd::stream::read::Decoder::new(file)
                .with_context(|| format!("decode cold raw archive {}", archive_path.display()))?;
            let source_addr = format!("frozen://{}", archive_path.display());
            let mut reader = BufReader::new(decoder);
            if scan_cold_reader(
                &mut reader,
                &parser,
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
            continue;
        }

        let archive_day_matches = archive_path
            .file_name()
            .and_then(|value| value.to_str())
            .is_some_and(|name| day_filter.is_none_or(|day| name.contains(day)));
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
            if !archive_day_matches && !cold_entry_day_matches(&entry_path, day_filter) {
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
                &parser,
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
    parser: &ParserEngine,
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
        while buffer
            .last()
            .is_some_and(|byte| *byte == b'\n' || *byte == b'\r')
        {
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
        let event = parser.parse(RawLog {
            ingest_time: Utc::now(),
            source_addr: source_addr.to_string(),
            raw,
        });
        if !query.include_failed && event.parse_status == fwlog_domain::ParseStatus::Failed {
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

fn cold_parquet_files(parquet_dir: &Path, day: Option<&str>) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_cold_parquet_files(parquet_dir, day, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_cold_parquet_files(
    dir: &Path,
    day: Option<&str>,
    files: &mut Vec<PathBuf>,
) -> anyhow::Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in
        fs::read_dir(dir).with_context(|| format!("read parquet directory {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_cold_parquet_files(&path, day, files)?;
            continue;
        }
        let name = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("");
        if !name.ends_with(".parquet") {
            continue;
        }
        let day_matches = day.is_none_or(|value| {
            name.contains(value)
                || name.contains(&format!(
                    "{}-{}-{}",
                    &value[0..4],
                    &value[4..6],
                    &value[6..8]
                ))
        });
        if day_matches {
            files.push(path);
        }
    }
    Ok(())
}

fn collect_cold_archive_files(
    dir: &Path,
    day: Option<&str>,
    files: &mut Vec<PathBuf>,
) -> anyhow::Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in
        fs::read_dir(dir).with_context(|| format!("read frozen directory {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_cold_archive_files(&path, day, files)?;
            continue;
        }
        let name = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("");
        if is_raw_zst_archive(&path) && archive_path_day_matches(&path, day) {
            files.push(path);
            continue;
        }
        if name.starts_with("raw-import-")
            && name.ends_with(".tar.zst")
            && archive_path_or_tar_member_day_matches(&path, day)?
        {
            files.push(path);
        }
    }
    Ok(())
}

fn is_raw_zst_archive(path: &Path) -> bool {
    path.file_name()
        .and_then(|value| value.to_str())
        .is_some_and(|name| name.ends_with(".raw.zst"))
}

fn archive_path_day_matches(path: &Path, day: Option<&str>) -> bool {
    day.is_none_or(|value| {
        let dashed = format!("{}-{}-{}", &value[0..4], &value[4..6], &value[6..8]);
        path.file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.contains(value) || name.contains(&dashed))
    })
}

fn archive_path_or_tar_member_day_matches(path: &Path, day: Option<&str>) -> anyhow::Result<bool> {
    let Some(day) = day else {
        return Ok(true);
    };
    if archive_path_day_matches(path, Some(day)) {
        return Ok(true);
    }
    let file = File::open(path).with_context(|| format!("open cold archive {}", path.display()))?;
    let decoder = zstd::stream::read::Decoder::new(file)
        .with_context(|| format!("decode cold archive {}", path.display()))?;
    let mut archive = tar::Archive::new(decoder);
    for entry in archive.entries().context("read cold tar entries")? {
        let entry = entry.context("read cold tar entry")?;
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let entry_path = entry.path()?.to_string_lossy().into_owned();
        if cold_entry_day_matches(&entry_path, Some(day)) {
            return Ok(true);
        }
    }
    Ok(false)
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
            || entry_path.contains(&format!(
                "{}-{}-{}",
                &value[0..4],
                &value[4..6],
                &value[6..8]
            ))
    })
}

fn infer_day_from_path(path: &Path) -> Option<String> {
    let text = path.file_name()?.to_string_lossy();
    infer_day_from_text(&text)
}

fn infer_day_from_text(text: &str) -> Option<String> {
    let digits = text
        .chars()
        .filter(|value| value.is_ascii_digit())
        .collect::<String>();
    if digits.len() < 8 {
        return None;
    }
    let day = &digits[..8];
    Some(format!("{}-{}-{}", &day[0..4], &day[4..6], &day[6..8]))
}

fn matches_cold_query(event: &CanonicalEvent, query: &ColdSearchQuery) -> bool {
    if let Some(value) = query.device.as_deref() {
        if !event.source_addr.contains(value) {
            return false;
        }
    }
    if let Some(value) = query.device_id.as_deref() {
        if event.device_id.as_deref() != Some(value) {
            return false;
        }
    }
    if let Some(start) = query.date_from.as_deref().and_then(parse_day) {
        let event_day = event
            .event_time
            .as_ref()
            .unwrap_or(&event.ingest_time)
            .date_naive();
        if event_day < start {
            return false;
        }
    }
    if let Some(end) = query.date_to.as_deref().and_then(parse_day) {
        let event_day = event
            .event_time
            .as_ref()
            .unwrap_or(&event.ingest_time)
            .date_naive();
        if event_day > end {
            return false;
        }
    }
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
    if let Some(value) = query.protocol.as_deref() {
        if normalize_query_protocol(event.protocol.as_deref())
            != normalize_query_protocol(Some(value))
        {
            return false;
        }
    }
    if let Some(value) = query.action.as_deref() {
        let expected_action = value.to_ascii_lowercase();
        if event
            .action
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref()
            != Some(expected_action.as_str())
        {
            return false;
        }
    }
    true
}

fn normalize_query_protocol(value: Option<&str>) -> Option<String> {
    value.map(|text| match text.to_ascii_uppercase().as_str() {
        "17" => "UDP".to_string(),
        "6" => "TCP".to_string(),
        other => other.to_string(),
    })
}

fn parquet_row_to_event(row: &duckdb::Row<'_>) -> duckdb::Result<CanonicalEvent> {
    let ingest_time: String = row.get(1)?;
    let event_time: Option<String> = row.get(4)?;
    let src_port: Option<i64> = row.get(8)?;
    let dst_port: Option<i64> = row.get(10)?;
    let parse_status: String = row.get(15)?;
    let parse_error: Option<String> = row.get(16)?;
    Ok(CanonicalEvent {
        event_id: row.get(0)?,
        ingest_time: chrono::DateTime::parse_from_rfc3339(&ingest_time)
            .map(|value| value.with_timezone(&chrono::Utc))
            .unwrap_or_else(|_| chrono::Utc::now()),
        source_addr: row.get(2)?,
        device_id: row.get(3)?,
        event_time: event_time.and_then(|value| {
            chrono::DateTime::parse_from_rfc3339(&value)
                .map(|value| value.with_timezone(&chrono::Utc))
                .ok()
        }),
        vendor: row.get(5)?,
        product: row.get(6)?,
        src_ip: row.get(7)?,
        src_port: src_port.and_then(|value| u16::try_from(value).ok()),
        dst_ip: row.get(9)?,
        dst_port: dst_port.and_then(|value| u16::try_from(value).ok()),
        protocol: row.get(11)?,
        action: row.get(12)?,
        severity: row.get(13)?,
        raw: row.get(14)?,
        parse_status: parse_status_from_str(&parse_status),
        parse_error,
    })
}

fn parse_status_from_str(value: &str) -> ParseStatus {
    match value {
        "parsed" => ParseStatus::Parsed,
        "partial" => ParseStatus::Partial,
        _ => ParseStatus::Failed,
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

    #[test]
    fn parquet_parse_status_preserves_partial() {
        assert_eq!(parse_status_from_str("parsed"), ParseStatus::Parsed);
        assert_eq!(parse_status_from_str("partial"), ParseStatus::Partial);
        assert_eq!(parse_status_from_str("failed"), ParseStatus::Failed);
    }

    #[tokio::test]
    async fn devices_routes_create_and_list_firewall_devices() {
        let dir = tempfile::tempdir().unwrap();
        let app = crate::router(
            dir.path().join("oxidelog.duckdb"),
            dir.path().join("parquet"),
            dir.path().join("frozen"),
        );

        let empty = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/devices")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(empty.status(), StatusCode::OK);
        let empty: serde_json::Value =
            serde_json::from_slice(&to_bytes(empty.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(empty.as_array().unwrap().len(), 0);

        let created = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/devices")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"name":"出口防火墙","host":"192.168.0.1","protocol":"UDP","port":514,"note":"机房核心出口"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::OK);
        let created: serde_json::Value =
            serde_json::from_slice(&to_bytes(created.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(created["name"], "出口防火墙");
        assert_eq!(created["host"], "192.168.0.1");
        assert_eq!(created["protocol"], "UDP");
        assert_eq!(created["port"], 514);
        assert_eq!(created["enabled"], true);

        let listed = app
            .oneshot(
                Request::builder()
                    .uri("/api/devices")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(listed.status(), StatusCode::OK);
        let listed: serde_json::Value =
            serde_json::from_slice(&to_bytes(listed.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(listed.as_array().unwrap().len(), 1);
        assert_eq!(listed[0]["id"], created["id"]);
        assert_eq!(listed[0]["note"], "机房核心出口");
    }

    #[tokio::test]
    async fn parser_scopes_endpoint_returns_empty_list() {
        let dir = tempfile::tempdir().unwrap();
        let app = crate::router(
            dir.path().join("oxidelog.duckdb"),
            dir.path().join("parquet"),
            dir.path().join("frozen"),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/parser/scopes")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body["scopes"].as_array().unwrap().len(), 0);
        assert_eq!(body["total"], 0);
    }

    #[tokio::test]
    async fn devices_routes_update_disable_and_delete_firewall_devices() {
        let dir = tempfile::tempdir().unwrap();
        let app = crate::router(
            dir.path().join("oxidelog.duckdb"),
            dir.path().join("parquet"),
            dir.path().join("frozen"),
        );

        let created = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/devices")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"name":"出口防火墙","host":"192.168.0.1","protocol":"UDP","port":514}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::OK);
        let created: serde_json::Value =
            serde_json::from_slice(&to_bytes(created.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        let id = created["id"].as_str().unwrap();

        let updated = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri(format!("/api/devices/{id}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"name":"核心防火墙","host":"192.168.0.2","protocol":"TCP","port":1514,"note":"停用测试","enabled":false}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(updated.status(), StatusCode::OK);
        let updated: serde_json::Value =
            serde_json::from_slice(&to_bytes(updated.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(updated["name"], "核心防火墙");
        assert_eq!(updated["host"], "192.168.0.2");
        assert_eq!(updated["protocol"], "TCP");
        assert_eq!(updated["enabled"], false);

        let deleted = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(format!("/api/devices/{id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);

        let listed = app
            .oneshot(
                Request::builder()
                    .uri("/api/devices")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let listed: serde_json::Value =
            serde_json::from_slice(&to_bytes(listed.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(listed.as_array().unwrap().len(), 0);
    }

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
        assert!(body.contains("/umi."));
    }

    #[tokio::test]
    async fn static_frontend_assets_are_served() {
        let dir = tempfile::tempdir().unwrap();
        let app = crate::router(
            dir.path().join("oxidelog.duckdb"),
            dir.path().join("parquet"),
            dir.path().join("frozen"),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/src_global_less_58740cc2.abc178dd.css")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
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
        drop(store);

        let app = crate::router(
            db_path,
            dir.path().join("parquet"),
            dir.path().join("frozen"),
        );

        let health = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/health")
                    .body(Body::empty())
                    .unwrap(),
            )
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
    async fn events_route_filters_in_duckdb_not_only_recent_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();

        let mut old = CanonicalEvent::failed(
            RawLog {
                ingest_time: Utc::now(),
                source_addr: "udp://192.168.0.1:514".to_string(),
                raw: "old hit 2.55.80.6".to_string(),
            },
            "bad",
        );
        old.event_id = "old-hit".to_string();
        old.ingest_time = Utc::now() - chrono::Duration::days(2);
        old.parse_status = ParseStatus::Parsed;
        old.src_ip = Some("2.55.80.6".to_string());
        old.dst_ip = Some("211.93.49.88".to_string());
        old.protocol = Some("UDP".to_string());
        old.action = Some("snat".to_string());
        old.parse_error = None;

        let mut recent = old.clone();
        recent.event_id = "recent-miss".to_string();
        recent.ingest_time = Utc::now();
        recent.src_ip = Some("9.9.9.9".to_string());

        store.insert_batch(&[old, recent]).unwrap();
        drop(store);

        let app = crate::router(
            db_path,
            dir.path().join("parquet"),
            dir.path().join("frozen"),
        );
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/events?src_ip=2.55.80.6&protocol=UDP&action=snat&include_failed=false&limit=20")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let rows: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(rows.as_array().unwrap().len(), 1);
        assert_eq!(rows[0]["event_id"], "old-hit");
    }

    #[tokio::test]
    async fn events_route_defaults_hide_failed_but_keep_partial() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();
        store
            .insert_batch(&[
                {
                    let raw = RawLog::new("tcp://127.0.0.1:1514", "parsed raw");
                    let mut event = CanonicalEvent::failed(raw, "bad");
                    event.event_id = "events-default-parsed".to_string();
                    event.parse_status = ParseStatus::Parsed;
                    event.parse_error = None;
                    event
                },
                {
                    let raw = RawLog::new("tcp://127.0.0.1:1514", "partial raw");
                    let mut event = CanonicalEvent::failed(raw, "partial");
                    event.event_id = "events-default-partial".to_string();
                    event.parse_status = ParseStatus::Partial;
                    event
                },
                {
                    let raw = RawLog::new("tcp://127.0.0.1:1514", "failed raw");
                    let mut event = CanonicalEvent::failed(raw, "failed");
                    event.event_id = "events-default-failed".to_string();
                    event
                },
            ])
            .unwrap();
        drop(store);

        let app = crate::router(
            db_path,
            dir.path().join("parquet"),
            dir.path().join("frozen"),
        );
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/events?limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let rows: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        let ids = rows
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["event_id"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert!(ids.contains(&"events-default-parsed"));
        assert!(ids.contains(&"events-default-partial"));
        assert!(!ids.contains(&"events-default-failed"));
    }

    #[tokio::test]
    async fn unified_search_returns_ip_region_fields() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        std::fs::write(
            dir.path().join("custom-ip-regions.json"),
            r#"[{"id":"custom-1","cidr":"2.0.0.0/8","name":"GovNet","note":"","enabled":true,"deleted":false,"created_at":"2026-05-18T00:00:00Z"}]"#,
        )
        .unwrap();
        let mut store = DuckDbStore::open(&db_path).unwrap();
        let mut event = CanonicalEvent::failed(
            RawLog {
                ingest_time: Utc::now(),
                source_addr: "udp://192.168.0.1:514".to_string(),
                raw: "region hit 2.55.80.6".to_string(),
            },
            "bad",
        );
        event.event_id = "region-hit".to_string();
        event.parse_status = ParseStatus::Parsed;
        event.src_ip = Some("2.55.80.6".to_string());
        event.dst_ip = Some("211.93.49.88".to_string());
        event.protocol = Some("UDP".to_string());
        event.action = Some("snat".to_string());
        event.parse_error = None;
        store.insert_batch(&[event]).unwrap();
        drop(store);

        let app = crate::router(
            db_path,
            dir.path().join("parquet"),
            dir.path().join("frozen"),
        );
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/search?scope=hot&src_ip=2.55.80.6&limit=5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let rows: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(rows.as_array().unwrap().len(), 1);
        assert_eq!(rows[0]["event"]["event_id"], "region-hit");
        assert_eq!(rows[0]["geo_region"], "GovNet");
        assert_eq!(rows[0]["src_geo_region"], "GovNet");
        assert!(rows[0]["dst_geo_region"].is_string() || rows[0]["dst_geo_region"].is_null());
    }

    #[tokio::test]
    async fn hot_search_defaults_hide_failed_but_keep_partial() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();
        store
            .insert_batch(&[
                {
                    let raw = RawLog::new("tcp://127.0.0.1:1514", "parsed raw");
                    let mut event = CanonicalEvent::failed(raw, "bad");
                    event.event_id = "search-default-parsed".to_string();
                    event.parse_status = ParseStatus::Parsed;
                    event.parse_error = None;
                    event
                },
                {
                    let raw = RawLog::new("tcp://127.0.0.1:1514", "partial raw");
                    let mut event = CanonicalEvent::failed(raw, "partial");
                    event.event_id = "search-default-partial".to_string();
                    event.parse_status = ParseStatus::Partial;
                    event
                },
                {
                    let raw = RawLog::new("tcp://127.0.0.1:1514", "failed raw");
                    let mut event = CanonicalEvent::failed(raw, "failed");
                    event.event_id = "search-default-failed".to_string();
                    event
                },
            ])
            .unwrap();
        drop(store);

        let app = crate::router(
            db_path,
            dir.path().join("parquet"),
            dir.path().join("frozen"),
        );
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/search?scope=hot&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let rows: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        let ids = rows
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["event"]["event_id"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert!(ids.contains(&"search-default-parsed"));
        assert!(ids.contains(&"search-default-partial"));
        assert!(!ids.contains(&"search-default-failed"));
    }

    #[tokio::test]
    async fn minute_metrics_route_reads_precomputed_metric_table() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();

        let mut first = CanonicalEvent::failed(
            RawLog {
                ingest_time: chrono::DateTime::parse_from_rfc3339("2026-05-16T08:00:10Z")
                    .unwrap()
                    .with_timezone(&Utc),
                source_addr: "udp://192.168.0.1:514".to_string(),
                raw: "metric one".to_string(),
            },
            "bad",
        );
        first.event_id = "metric-api-one".to_string();
        first.parse_status = ParseStatus::Parsed;
        first.parse_error = None;

        let mut second = first.clone();
        second.event_id = "metric-api-two".to_string();
        second.ingest_time = chrono::DateTime::parse_from_rfc3339("2026-05-16T08:00:50Z")
            .unwrap()
            .with_timezone(&Utc);
        second.parse_status = ParseStatus::Failed;
        second.parse_error = Some("bad".to_string());
        store.insert_batch(&[first, second]).unwrap();
        drop(store);

        let app = crate::router(
            db_path,
            dir.path().join("parquet"),
            dir.path().join("frozen"),
        );
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/metrics/minutes?hours=8784&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let rows: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(rows.as_array().unwrap().len(), 1);
        assert_eq!(rows[0]["bucket_minute"], "2026-05-16T08:00:00Z");
        assert_eq!(rows[0]["total"], 2);
        assert_eq!(rows[0]["parsed"], 1);
        assert_eq!(rows[0]["failed"], 1);
    }

    #[tokio::test]
    async fn source_metrics_route_reads_precomputed_metric_table() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();

        let mut first = CanonicalEvent::failed(
            RawLog {
                ingest_time: chrono::DateTime::parse_from_rfc3339("2026-05-16T08:00:10Z")
                    .unwrap()
                    .with_timezone(&Utc),
                source_addr: "udp://192.168.0.1:514".to_string(),
                raw: "metric one".to_string(),
            },
            "bad",
        );
        first.event_id = "source-api-one".to_string();
        first.parse_status = ParseStatus::Parsed;
        first.parse_error = None;

        let mut second = first.clone();
        second.event_id = "source-api-two".to_string();
        second.source_addr = "udp://192.168.0.2:514".to_string();
        second.raw = "metric two".to_string();

        let mut failed = first.clone();
        failed.event_id = "source-api-failed".to_string();
        failed.parse_status = ParseStatus::Failed;
        failed.parse_error = Some("bad".to_string());
        store.insert_batch(&[first, second, failed]).unwrap();
        drop(store);

        let app = crate::router(
            db_path,
            dir.path().join("parquet"),
            dir.path().join("frozen"),
        );
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/metrics/sources?hours=8784&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let rows: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(rows.as_array().unwrap().len(), 2);
        assert_eq!(rows[0]["source_addr"], "udp://192.168.0.1:514");
        assert_eq!(rows[0]["total"], 2);
        assert_eq!(rows[0]["parsed"], 1);
        assert_eq!(rows[0]["failed"], 1);
        assert_eq!(rows[0]["raw_bytes"], 20);
        assert_eq!(rows[0]["last_seen"], "2026-05-16T08:00:00Z");
        assert_eq!(rows[1]["source_addr"], "udp://192.168.0.2:514");
        assert_eq!(rows[1]["total"], 1);
        assert_eq!(rows[1]["parsed"], 1);
        assert_eq!(rows[1]["failed"], 0);
        assert_eq!(rows[1]["raw_bytes"], 10);
        assert_eq!(rows[1]["last_seen"], "2026-05-16T08:00:00Z");
    }

    #[tokio::test]
    async fn export_csv_uses_same_filters_as_events_route() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();

        let mut hit = CanonicalEvent::failed(
            RawLog {
                ingest_time: Utc::now(),
                source_addr: "udp://192.168.0.1:514".to_string(),
                raw: "export hit 2.55.80.6".to_string(),
            },
            "bad",
        );
        hit.event_id = "export-hit".to_string();
        hit.parse_status = ParseStatus::Parsed;
        hit.src_ip = Some("2.55.80.6".to_string());
        hit.protocol = Some("UDP".to_string());
        hit.action = Some("snat".to_string());
        hit.parse_error = None;

        let mut miss = hit.clone();
        miss.event_id = "export-miss".to_string();
        miss.src_ip = Some("9.9.9.9".to_string());
        store.insert_batch(&[hit, miss]).unwrap();
        drop(store);

        let app = crate::router(
            db_path,
            dir.path().join("parquet"),
            dir.path().join("frozen"),
        );
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/events/export.csv?src_ip=2.55.80.6&protocol=UDP&action=snat&include_failed=false&limit=20")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = String::from_utf8(
            to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert!(body.contains("export-hit"));
        assert!(!body.contains("export-miss"));
    }

    #[tokio::test]
    async fn export_job_accepts_ip_time_range_without_device() {
        let dir = tempfile::tempdir().unwrap();
        let app = crate::router(
            dir.path().join("oxidelog.duckdb"),
            dir.path().join("parquet"),
            dir.path().join("frozen"),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/export/jobs")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"scope":"all","date_from":"2025-05-19","date_to":"2026-05-19","limit":1000000}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn export_job_creates_zstd_csv_download() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let mut store = DuckDbStore::open(&db_path).unwrap();
        let mut event = CanonicalEvent::failed(
            RawLog {
                ingest_time: chrono::DateTime::parse_from_rfc3339("2026-05-16T08:00:10Z")
                    .unwrap()
                    .with_timezone(&Utc),
                source_addr: "udp://192.168.0.1:514".to_string(),
                raw: "export job hit".to_string(),
            },
            "bad",
        );
        event.event_id = "export-job-hit".to_string();
        event.parse_status = ParseStatus::Parsed;
        event.parse_error = None;
        event.device_id = Some("device-a".to_string());
        event.src_ip = Some("2.55.80.6".to_string());
        store.insert_batch(&[event]).unwrap();
        drop(store);

        let app = crate::router(
            db_path,
            dir.path().join("parquet"),
            dir.path().join("frozen"),
        );
        let created = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/export/jobs")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"scope":"hot","device":"192.168.0.1","date_from":"2025-05-19","date_to":"2026-05-19","limit":1000000}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(created.status(), StatusCode::OK);
        let created: serde_json::Value =
            serde_json::from_slice(&to_bytes(created.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        let job_id = created["job_id"].as_str().unwrap();
        assert!(created["download_url"]
            .as_str()
            .unwrap()
            .ends_with("/download"));
        assert!(created["file_name"].as_str().unwrap().ends_with(".csv.zst"));

        let mut status = serde_json::Value::Null;
        for _ in 0..50 {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/api/export/jobs/{job_id}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            status =
                serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                    .unwrap();
            if status["status"] == "completed" {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(status["status"], "completed");
        assert_eq!(status["rows"], 1);
        assert!(status["file_bytes"].as_u64().unwrap() > 0);

        let download = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/export/jobs/{job_id}/download"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(download.status(), StatusCode::OK);
        let body = to_bytes(download.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..4], &[0x28, 0xb5, 0x2f, 0xfd]);
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
        drop(store);

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
        assert!(created["path"].as_str().unwrap().ends_with(".parquet"));
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
        drop(store);

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
        assert!(created["path"].as_str().unwrap().ends_with(".raw.zst"));
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
    async fn cold_search_reads_parquet_archives_before_frozen() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let parquet_dir = dir.path().join("parquet");
        let frozen_dir = dir.path().join("frozen");
        let parquet_path = parquet_dir.join("events-20260516.parquet");
        let store = DuckDbStore::open(&db_path).unwrap();
        let mut event = CanonicalEvent::failed(
            RawLog {
                ingest_time: chrono::DateTime::parse_from_rfc3339("2026-05-16T08:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
                source_addr: "parquet://test".to_string(),
                raw: "parquet hit 172.18.1.1".to_string(),
            },
            "bad",
        );
        event.event_id = "parquet-hit".to_string();
        event.parse_status = ParseStatus::Parsed;
        event.vendor = Some("Sangfor".to_string());
        event.product = Some("Firewall".to_string());
        event.src_ip = Some("172.18.1.1".to_string());
        event.dst_ip = Some("211.93.49.88".to_string());
        event.protocol = Some("UDP".to_string());
        event.action = Some("snat".to_string());
        event.parse_error = None;
        store
            .archive_events_parquet(&parquet_path, &[event])
            .unwrap();
        drop(store);

        let app = crate::router(db_path, parquet_dir, frozen_dir);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/cold/search?day=2026-05-16&src_ip=172.18.1.1&protocol=UDP&action=snat&limit=5")
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
        assert_eq!(body["matched"], 1);
        assert_eq!(body["events"][0]["event_id"], "parquet-hit");
    }

    #[tokio::test]
    async fn cold_search_reads_plain_raw_zst_without_tar_reader() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let parquet_dir = dir.path().join("parquet");
        let frozen_dir = dir.path().join("frozen");
        std::fs::create_dir_all(&frozen_dir).unwrap();
        write_frozen_raw(
            frozen_dir.join("frozen-20260516.raw.zst"),
            &[
                "Sangfor: src=192.168.50.10 dst=10.4.90.205 sport=21527 dport=2048 proto=UDP action=snat severity=info".to_string(),
            ],
        )
        .unwrap();

        let app = crate::router(db_path, parquet_dir, frozen_dir);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/cold/search?day=20260516&src_ip=192.168.50.10&limit=5")
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
        assert_eq!(body["events"][0]["src_ip"], "192.168.50.10");
    }

    #[tokio::test]
    async fn archive_index_rebuild_populates_line_count_time_and_source() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let parquet_dir = dir.path().join("parquet");
        let frozen_dir = dir.path().join("frozen");
        std::fs::create_dir_all(&frozen_dir).unwrap();
        write_raw_import_tar_zst_member(
            &frozen_dir.join("raw-import-20260516-index.tar.zst"),
            "logs/fw-20260516.log",
            b"Sangfor: src=192.168.70.10 dst=10.4.90.205 sport=21527 dport=2048 proto=UDP action=snat severity=info\n",
        );

        let app = crate::router(db_path, parquet_dir, frozen_dir);
        let rebuilt = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/archive/index/rebuild")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rebuilt.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/archive/index?day=2026-05-16")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let rows: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(rows.as_array().unwrap().len(), 1);
        assert_eq!(rows[0]["line_count"], 1);
        assert_eq!(rows[0]["source_addr"], "frozen://logs/fw-20260516.log");
        assert_eq!(rows[0]["first_seen"], "2026-05-16T00:00:00Z");
        assert_eq!(rows[0]["last_seen"], "2026-05-16T23:59:59Z");
    }

    #[tokio::test]
    async fn cold_search_day_filter_does_not_scan_unrelated_archives() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let parquet_dir = dir.path().join("parquet");
        let frozen_dir = dir.path().join("frozen");
        std::fs::create_dir_all(&frozen_dir).unwrap();
        write_raw_import_tar_zst(
            &frozen_dir.join("raw-import-20260516-hit.tar.zst"),
            "Sangfor: src=192.168.60.10 dst=10.4.90.205 sport=21527 dport=2048 proto=UDP action=snat severity=info\n",
        );
        write_raw_import_tar_zst(
            &frozen_dir.join("raw-import-20260517-miss.tar.zst"),
            "Sangfor: src=192.168.60.11 dst=10.4.90.205 sport=21527 dport=2048 proto=UDP action=snat severity=info\n",
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
        assert_eq!(body["files"], 1);
        assert_eq!(body["scanned_lines"], 1);
        assert_eq!(body["matched"], 1);
        assert_eq!(body["events"][0]["src_ip"], "192.168.60.10");
    }

    #[tokio::test]
    async fn archive_search_filters_and_preserves_parquet_device_id() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("oxidelog.duckdb");
        let parquet_dir = dir.path().join("parquet");
        let frozen_dir = dir.path().join("frozen");
        let parquet_path = parquet_dir.join("events-20260516.parquet");
        let store = DuckDbStore::open(&db_path).unwrap();
        let mut hit = CanonicalEvent::failed(
            RawLog {
                ingest_time: chrono::DateTime::parse_from_rfc3339("2026-05-16T08:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
                source_addr: "udp://192.168.0.1:514".to_string(),
                raw: "device id hit".to_string(),
            },
            "bad",
        );
        hit.event_id = "archive-device-hit".to_string();
        hit.device_id = Some("device-a".to_string());
        hit.parse_status = ParseStatus::Parsed;
        hit.src_ip = Some("172.18.2.1".to_string());
        hit.dst_ip = Some("211.93.49.88".to_string());
        hit.protocol = Some("UDP".to_string());
        hit.action = Some("snat".to_string());
        hit.parse_error = None;
        let mut miss = hit.clone();
        miss.event_id = "archive-device-miss".to_string();
        miss.device_id = Some("device-b".to_string());
        miss.src_ip = Some("172.18.2.2".to_string());
        store
            .archive_events_parquet(&parquet_path, &[hit, miss])
            .unwrap();
        drop(store);

        let app = crate::router(db_path, parquet_dir, frozen_dir);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/search?scope=archive&day=2026-05-16&device_id=device-a&limit=5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body.as_array().unwrap().len(), 1);
        assert_eq!(body[0]["event"]["event_id"], "archive-device-hit");
        assert_eq!(body[0]["event"]["device_id"], "device-a");
    }

    #[tokio::test]
    async fn cold_search_hides_failed_lines_but_keeps_partial_by_default() {
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
        assert_eq!(body["matched"], 2);
        let statuses = body["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event["parse_status"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert!(statuses.contains(&"partial"));
        assert!(statuses.contains(&"parsed"));
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
        drop(store);

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
        builder
            .append_data(&mut header, member_name, bytes)
            .unwrap();
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
    Ok((files.len(), files.into_iter().map(|file| file.bytes).sum()))
}

fn frozen_stats(dir: &Path) -> anyhow::Result<(usize, u64)> {
    if !dir.exists() {
        return Ok((0, 0));
    }

    let files = list_frozen_files(dir)?;
    Ok((files.len(), files.into_iter().map(|file| file.bytes).sum()))
}

pub async fn archive_parquet(
    Extension(state): Extension<ApiState>,
    Query(query): Query<LimitQuery>,
) -> Response {
    let file_name = archive_stamp("events", "parquet");
    let output_path = state.parquet_dir.join(file_name);

    let result = fs::create_dir_all(&*state.parquet_dir)
        .context("create parquet archive directory")
        .and_then(|_| DuckDbStore::open_read_only(&*state.duckdb_path))
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

pub async fn admission_cases() -> Response {
    Json(Vec::<serde_json::Value>::new()).into_response()
}

pub async fn admission_profiles() -> Response {
    Json(Vec::<serde_json::Value>::new()).into_response()
}

pub async fn archive_frozen(
    Extension(state): Extension<ApiState>,
    Query(query): Query<LimitQuery>,
) -> Response {
    let file_name = archive_stamp("frozen", "raw.zst");
    let output_path = state.frozen_dir.join(file_name);

    let result = fs::create_dir_all(&*state.frozen_dir)
        .context("create frozen archive directory")
        .and_then(|_| DuckDbStore::open_read_only(&*state.duckdb_path))
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
            let parent = input_path.parent().ok_or_else(|| {
                anyhow!("canonicalize frozen archive path {}", input_path.display())
            })?;
            let parent = parent.canonicalize().with_context(|| {
                format!("canonicalize frozen archive parent {}", parent.display())
            })?;
            let file_name = input_path.file_name().ok_or_else(|| {
                anyhow!("canonicalize frozen archive path {}", input_path.display())
            })?;
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

pub async fn storage_health(Extension(_state): Extension<ApiState>) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "error": "hybrid_storage_not_integrated",
            "message": "HybridStorage not available in ApiState. Integration required."
        })),
    )
        .into_response()
}

pub async fn storage_stats(Extension(_state): Extension<ApiState>) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "error": "hybrid_storage_not_integrated",
            "message": "HybridStorage not available in ApiState. Integration required."
        })),
    )
        .into_response()
}
