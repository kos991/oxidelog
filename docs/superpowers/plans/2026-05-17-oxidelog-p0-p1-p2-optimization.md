# OxideLog P0/P1/P2 Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn OxideLog from an import/demo system into a stable single-node firewall log platform with bounded hot storage, long-term metrics, cheap Frozen retention, and usable historical search.

**Architecture:** Keep the current dedicated pipeline: syslog ingress -> parser -> DuckDB hot event table -> metrics tables -> Frozen raw archives. Do not introduce Logstash, Elasticsearch, or Quickwit in P0/P1; P2 only adds optional search enhancements behind the existing OxideLog API.

**Tech Stack:** Rust, Axum, DuckDB, tar.zst Frozen archives, Ant Design Pro v6, ip2region/custom CIDR.

---

## Priority Summary

**P0: Must finish first**
- Bound the Hot DuckDB size.
- Make metrics the default source for overview charts.
- Make device onboarding and parser observability practical.

**P1: Storage and operational maturity**
- Automate hot/cold/frozen lifecycle.
- Add Frozen archive index so historical lookup does not blindly scan everything.
- Bind events to managed device IDs.

**P2: Search and analysis enhancement**
- Return one unified result model across hot and frozen.
- Add IP attribution cache.
- Add optional external search only after the native flow is stable.

---

## File Map

**Backend storage**
- Modify: `crates/fwlog-storage/src/duckdb.rs`
  - Hot compaction, raw pruning, metrics tables, archive index queries.
- Modify: `crates/fwlog-storage/src/lib.rs`
  - Export new storage types.
- Create: `crates/fwlog-storage/src/lifecycle.rs`
  - Hot retention and compaction orchestration.

**Backend API**
- Modify: `crates/fwlog-api/src/lib.rs`
  - Add lifecycle, parser stats, archive index, unified search routes.
- Modify: `crates/fwlog-api/src/handlers.rs`
  - Route handlers and tests.

**Pipeline**
- Modify: `apps/fwlogd/src/pipeline.rs`
  - Run scheduled lifecycle jobs and update parser/device metrics on ingest.
- Modify: `apps/fwlog-import/src/main.rs`
  - Rebuild metrics/archive index after imports.

**Frontend**
- Modify: `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/services/oxidelog.ts`
  - Add API types/functions.
- Modify: `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/index.tsx`
  - UI for parser stats, lifecycle state, archive index, unified results.
- Modify: `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/style.less`
  - Dense white SOC styling only.

---

## P0-A: Bound Hot DuckDB Storage

**Outcome:** Hot DB stays small and predictable. Parsed rows may drop `raw`; failed rows keep `raw`.

**Files:**
- Modify: `crates/fwlog-storage/src/duckdb.rs`
- Create: `crates/fwlog-storage/src/lifecycle.rs`
- Modify: `crates/fwlog-storage/src/lib.rs`
- Test: `crates/fwlog-storage/src/duckdb.rs`

- [x] **Step 1: Write failing test for parsed raw pruning**

Add this test in `crates/fwlog-storage/src/duckdb.rs`:

```rust
#[test]
fn prune_parsed_raw_keeps_failed_raw_and_preserves_fields() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("oxidelog.duckdb");
    let mut store = DuckDbStore::open(&db_path).unwrap();
    let mut parsed = event("parsed", ParseStatus::Parsed);
    parsed.raw = "large parsed raw".to_string();
    parsed.src_ip = Some("2.55.80.6".to_string());
    let mut failed = event("failed", ParseStatus::Failed);
    failed.raw = "failed raw must stay".to_string();
    store.insert_batch(&[parsed, failed]).unwrap();

    let changed = store.prune_parsed_raw().unwrap();

    assert_eq!(changed, 1);
    let rows = store.query_recent(10).unwrap();
    let parsed = rows.iter().find(|row| row.event_id == "parsed").unwrap();
    let failed = rows.iter().find(|row| row.event_id == "failed").unwrap();
    assert_eq!(parsed.raw, "");
    assert_eq!(parsed.src_ip.as_deref(), Some("2.55.80.6"));
    assert_eq!(failed.raw, "failed raw must stay");
}
```

- [x] **Step 2: Run test and verify failure**

Run on server:

```bash
cd /opt/oxidelog-src
cargo test -p fwlog-storage prune_parsed_raw_keeps_failed_raw_and_preserves_fields
```

Expected: compile failure or method-not-found for `prune_parsed_raw`.

- [x] **Step 3: Implement minimal pruning method**

Add to `impl DuckDbStore` in `crates/fwlog-storage/src/duckdb.rs`:

```rust
pub fn prune_parsed_raw(&self) -> Result<usize> {
    let changed = self
        .conn
        .execute(
            "UPDATE events SET raw = '' WHERE parse_status = 'parsed' AND raw <> ''",
            [],
        )
        .context("prune parsed event raw payloads")?;
    Ok(changed)
}
```

- [x] **Step 4: Verify storage test passes**

Run:

```bash
cd /opt/oxidelog-src
cargo test -p fwlog-storage prune_parsed_raw_keeps_failed_raw_and_preserves_fields
```

Expected: PASS.

- [x] **Step 5: Add hot retention compaction test**

Add:

```rust
#[test]
fn compact_hot_retains_newest_rows_and_prunes_parsed_raw() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("oxidelog.duckdb");
    let compact_path = dir.path().join("hot.duckdb");
    let mut store = DuckDbStore::open(&db_path).unwrap();
    let mut old = event("old", ParseStatus::Parsed);
    old.ingest_time = Utc.timestamp_opt(1_778_808_000, 0).unwrap();
    let mut new = event("new", ParseStatus::Parsed);
    new.ingest_time = Utc.timestamp_opt(1_778_808_060, 0).unwrap();
    store.insert_batch(&[old, new]).unwrap();

    let copied = store.compact_hot_to(&compact_path, 1, true).unwrap();

    assert_eq!(copied, 1);
    let compact = DuckDbStore::open(&compact_path).unwrap();
    let rows = compact.query_recent(10).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].event_id, "new");
    assert_eq!(rows[0].raw, "");
}
```

- [x] **Step 6: Run full storage tests**

Run:

```bash
cd /opt/oxidelog-src
cargo test -p fwlog-storage
```

Expected: all storage tests pass.

---

## P0-B: Extend Long-Term Metrics Tables

**Outcome:** Overview, trend charts, and device cards read small metric tables, not large event tables.

**Files:**
- Modify: `crates/fwlog-storage/src/duckdb.rs`
- Modify: `crates/fwlog-api/src/handlers.rs`
- Modify: `crates/fwlog-api/src/lib.rs`
- Modify: `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/services/oxidelog.ts`
- Modify: `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/index.tsx`

- [x] **Step 1: Add test for hourly metrics**

Add storage test:

```rust
#[test]
fn query_hour_metrics_rolls_up_minute_metrics() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("oxidelog.duckdb");
    let mut store = DuckDbStore::open(&db_path).unwrap();
    let first = parsed_event("h1", 1_778_894_401, "udp://192.168.0.1:514", "2.55.80.6", "8.8.8.8", "UDP", "snat");
    let second = parsed_event("h2", 1_778_894_901, "udp://192.168.0.1:514", "2.55.80.7", "8.8.4.4", "UDP", "snat");
    store.insert_batch(&[first, second]).unwrap();

    let rows = store.query_hour_metrics(24 * 365, 24).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].bucket_hour, "2026-05-16T01:00:00Z");
    assert_eq!(rows[0].total, 2);
}
```

- [x] **Step 2: Implement `HourMetricPoint` and query**

Use minute table as source:

```rust
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct HourMetricPoint {
    pub bucket_hour: String,
    pub total: u64,
    pub parsed: u64,
    pub failed: u64,
    pub raw_bytes: u64,
}

pub fn query_hour_metrics(&self, hours: u32, limit: usize) -> Result<Vec<HourMetricPoint>> {
    let cutoff = (chrono::Utc::now() - chrono::Duration::hours(i64::from(hours.clamp(1, 24 * 366))))
        .to_rfc3339()
        .chars()
        .take(13)
        .collect::<String>();
    let cutoff = format!("{cutoff}:00:00Z");
    let mut stmt = self.conn.prepare(
        r#"
        SELECT *
        FROM (
          SELECT concat(substr(bucket_minute, 1, 13), ':00:00Z') AS bucket_hour,
                 SUM(total_count) AS total,
                 SUM(CASE WHEN parse_status = 'parsed' THEN total_count ELSE 0 END) AS parsed,
                 SUM(CASE WHEN parse_status = 'failed' THEN total_count ELSE 0 END) AS failed,
                 SUM(raw_bytes) AS raw_bytes
          FROM nat_minute_metrics
          WHERE concat(substr(bucket_minute, 1, 13), ':00:00Z') >= ?
          GROUP BY bucket_hour
          ORDER BY bucket_hour DESC
          LIMIT ?
        )
        ORDER BY bucket_hour ASC
        "#,
    )?;
    let rows = stmt.query_map(params![cutoff, limit.clamp(1, 24 * 366) as i64], |row| {
        Ok(HourMetricPoint {
            bucket_hour: row.get(0)?,
            total: row.get::<_, Option<i64>>(1)?.unwrap_or(0).max(0) as u64,
            parsed: row.get::<_, Option<i64>>(2)?.unwrap_or(0).max(0) as u64,
            failed: row.get::<_, Option<i64>>(3)?.unwrap_or(0).max(0) as u64,
            raw_bytes: row.get::<_, Option<i64>>(4)?.unwrap_or(0).max(0) as u64,
        })
    })?;
    rows.collect::<duckdb::Result<Vec<_>>>().context("query hour metrics")
}
```

- [x] **Step 3: Add API route `/api/metrics/hours`**

Add route in `crates/fwlog-api/src/lib.rs`:

```rust
.route("/api/metrics/hours", get(handlers::hour_metrics))
```

Add handler:

```rust
pub async fn hour_metrics(
    Extension(state): Extension<ApiState>,
    Query(query): Query<MinuteMetricsRequest>,
) -> Response {
    match DuckDbStore::open(&*state.duckdb_path)
        .and_then(|store| store.query_hour_metrics(query.hours, query.limit))
    {
        Ok(points) => Json(points).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    }
}
```

- [x] **Step 4: Frontend reads hourly metrics for long range**

In `src/services/oxidelog.ts`:

```ts
export type HourMetricPoint = {
  bucket_hour: string;
  total: number;
  parsed: number;
  failed: number;
  raw_bytes: number;
};

export function fetchHourMetrics(hours = 24 * 365, limit = 365 * 24) {
  const query = new URLSearchParams({ hours: String(hours), limit: String(limit) });
  return getJson<HourMetricPoint[]>(`/api/metrics/hours?${query.toString()}`);
}
```

- [x] **Step 5: Run verification**

Run:

```bash
cd /opt/oxidelog-src
cargo test -p fwlog-storage
cargo test -p fwlog-api
```

Run frontend build from `X:\`:

```powershell
npm.cmd run build
```

---

## P0-C: Parser Observability

**Outcome:** UI shows why parsing fails instead of only showing failed count.

**Files:**
- Modify: `crates/fwlog-storage/src/duckdb.rs`
- Modify: `crates/fwlog-api/src/handlers.rs`
- Modify: `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/index.tsx`

- [x] **Step 1: Add storage test for parse error top reasons**

```rust
#[test]
fn parse_error_summary_groups_failed_reasons() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("oxidelog.duckdb");
    let mut store = DuckDbStore::open(&db_path).unwrap();
    let mut a = event("a", ParseStatus::Failed);
    a.parse_error = Some("missing src_ip".to_string());
    let mut b = event("b", ParseStatus::Failed);
    b.parse_error = Some("missing src_ip".to_string());
    let mut c = event("c", ParseStatus::Failed);
    c.parse_error = Some("unsupported format".to_string());
    store.insert_batch(&[a, b, c]).unwrap();

    let rows = store.parse_error_summary(10).unwrap();

    assert_eq!(rows[0].reason, "missing src_ip");
    assert_eq!(rows[0].count, 2);
    assert_eq!(rows[1].reason, "unsupported format");
    assert_eq!(rows[1].count, 1);
}
```

- [x] **Step 2: Implement summary query**

```rust
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ParseErrorSummary {
    pub reason: String,
    pub count: u64,
}

pub fn parse_error_summary(&self, limit: usize) -> Result<Vec<ParseErrorSummary>> {
    let mut stmt = self.conn.prepare(
        r#"
        SELECT COALESCE(NULLIF(parse_error, ''), 'unknown') AS reason, COUNT(*) AS count
        FROM events
        WHERE parse_status = 'failed'
        GROUP BY reason
        ORDER BY count DESC, reason ASC
        LIMIT ?
        "#,
    )?;
    let rows = stmt.query_map([limit.clamp(1, 100) as i64], |row| {
        Ok(ParseErrorSummary {
            reason: row.get(0)?,
            count: row.get::<_, i64>(1)?.max(0) as u64,
        })
    })?;
    rows.collect::<duckdb::Result<Vec<_>>>().context("query parse error summary")
}
```

- [x] **Step 3: Add API `GET /api/parser/summary`**

Return JSON array from `store.parse_error_summary(20)`.

- [x] **Step 4: UI add card under overview**

Add a compact ProTable titled `瑙ｆ瀽澶辫触 Top 鍘熷洜` with columns `鍘熷洜` and `娆℃暟`.

---

## P1-A: Automated Lifecycle Scheduler

**Outcome:** Daily lifecycle job archives old data, prunes raw, compacts DuckDB, and leaves metrics intact.

**Files:**
- Create: `crates/fwlog-storage/src/lifecycle.rs`
- Modify: `crates/fwlog-storage/src/lib.rs`
- Modify: `apps/fwlogd/src/pipeline.rs`
- Modify: `config/server.toml`

- [x] **Step 1: Define lifecycle config**

Add to `apps/fwlogd/src/main.rs` config:

```rust
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
```

Defaults:

```rust
fn default_lifecycle_enabled() -> bool { true }
fn default_hot_limit() -> usize { 100_000 }
fn default_lifecycle_interval_seconds() -> u64 { 24 * 3600 }
fn default_drop_parsed_raw() -> bool { true }
```

- [x] **Step 2: Implement lifecycle result**

Create `crates/fwlog-storage/src/lifecycle.rs`:

```rust
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct LifecycleReport {
    pub hot_limit: usize,
    pub compacted_rows: usize,
    pub pruned_raw_rows: usize,
    pub output_path: PathBuf,
}
```

- [x] **Step 3: Run lifecycle from pipeline**

In `apps/fwlogd/src/pipeline.rs`, spawn a Tokio task similar to archive scheduler. It calls `compact_hot_to(temp_path, hot_limit, drop_parsed_raw)`, then atomically swaps the DB file only after compact succeeds.

- [x] **Step 4: Verification**

Run:

```bash
cd /opt/oxidelog-src
cargo test -p fwlog-storage
cargo test -p fwlog-api
cargo build --release -p fwlogd
```

---

## P1-B: Frozen Archive Index

**Outcome:** Historical search finds candidate tar.zst files by date/device before streaming raw content.

**Files:**
- Modify: `crates/fwlog-storage/src/duckdb.rs`
- Modify: `crates/fwlog-api/src/handlers.rs`
- Modify: `apps/fwlog-import/src/main.rs`

- [x] **Step 1: Add `frozen_archive_index` table**

Schema:

```sql
CREATE TABLE IF NOT EXISTS frozen_archive_index (
  archive_path TEXT PRIMARY KEY,
  day TEXT NOT NULL,
  source_addr TEXT NOT NULL,
  bytes BIGINT NOT NULL,
  line_count BIGINT NOT NULL,
  first_seen TEXT,
  last_seen TEXT,
  indexed_at TEXT NOT NULL
);
```

- [x] **Step 2: Add test for indexed file lookup**

```rust
#[test]
fn archive_index_filters_by_day_before_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("oxidelog.duckdb");
    let store = DuckDbStore::open(&db_path).unwrap();
    store.upsert_frozen_archive_index("raw-import-20260425-a.tar.zst", "2026-04-25", "udp://192.168.0.1:514", 100, 10).unwrap();
    store.upsert_frozen_archive_index("raw-import-20260426-a.tar.zst", "2026-04-26", "udp://192.168.0.1:514", 100, 10).unwrap();

    let files = store.find_frozen_archives("2026-04-25", None).unwrap();

    assert_eq!(files, vec!["raw-import-20260425-a.tar.zst".to_string()]);
}
```

- [x] **Step 3: API exposes archive index**

Add:

```text
GET /api/archive/index?day=2026-04-25
POST /api/archive/index/rebuild
```

- [x] **Step 4: Cold search uses index first**

In `search_history_archives`, if `day` is present and index has rows, use indexed paths. If index is empty, fall back to existing filesystem scan.

---

## P1-C: Device ID Binding

**Outcome:** Events store `device_id`, so changing device name updates UI everywhere.

**Files:**
- Modify: `crates/fwlog-storage/src/duckdb.rs`
- Modify: `crates/fwlog-api/src/handlers.rs`
- Modify: `apps/fwlogd/src/pipeline.rs`

- [x] **Step 1: Add nullable `device_id` column migration**

Migration:

```sql
ALTER TABLE events ADD COLUMN device_id TEXT;
```

Guard it with `pragma_table_info('events')` like existing `source_addr` migration.

- [x] **Step 2: Add query support**

Extend `EventQuery`:

```rust
pub device_id: Option<String>,
```

Filter:

```rust
if let Some(value) = query.device_id.as_deref().filter(|value| !value.is_empty()) {
    clauses.push("device_id = ?");
    values.push(value.to_string());
}
```

- [x] **Step 3: Pipeline resolves device ID**

Before insert, match `source_addr` host/port against `devices.json`. If matched, set `event.device_id = Some(device.id)`.

- [x] **Step 4: Backfill endpoint**

Add:

```text
POST /api/devices/backfill
```

It updates existing events based on current device table.

---

## P2-A: Unified Search Result Model

**Outcome:** Hot, Cold, and Frozen results render in one table with consistent metadata.

**Files:**
- Modify: `crates/fwlog-api/src/handlers.rs`
- Modify: `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/services/oxidelog.ts`
- Modify: `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/index.tsx`

- [x] **Step 1: Define API response**

```rust
#[derive(Debug, Serialize)]
struct UnifiedSearchRow {
    result_source: String,
    archive_path: Option<PathBuf>,
    device_name: Option<String>,
    geo_region: Option<String>,
    event: CanonicalEvent,
}
```

- [x] **Step 2: Add route**

```text
GET /api/search?scope=hot|archive|all&day=2026-04-25&src_ip=2.55.80.6
```

- [x] **Step 3: Frontend uses only unified result rows**

Replace separate hot/cold merge logic in `index.tsx` with API rows from `/api/search`.

---

## P2-B: IP Region Cache

**Outcome:** Result table does not call ip2region repeatedly for the same public IP.

**Files:**
- Modify: `crates/fwlog-storage/src/duckdb.rs`
- Modify: `crates/fwlog-api/src/handlers.rs`

- [x] **Step 1: Add cache table**

```sql
CREATE TABLE IF NOT EXISTS ip_region_cache (
  ip TEXT PRIMARY KEY,
  region TEXT,
  country TEXT,
  province TEXT,
  city TEXT,
  isp TEXT,
  source TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
```

- [x] **Step 2: Lookup flow**

Order:

```text
custom CIDR -> ip_region_cache -> ip2region xdb -> insert cache -> return
```

- [x] **Step 3: Add tests**

Check that second lookup returns cached value even if xdb fallback is not called.

---

## P2-C: Optional Full-Text Search Adapter

**Outcome:** Keep native OxideLog search as default; allow later Quickwit-style backend without changing UI.

**Files:**
- Create: `crates/fwlog-api/src/search.rs`
- Modify: `crates/fwlog-api/src/handlers.rs`
- Modify: `config/server.toml`

- [x] **Step 1: Define trait**

```rust
pub trait SearchBackend {
    fn search(&self, query: &EventQuery, limit: usize) -> anyhow::Result<Vec<CanonicalEvent>>;
}
```

- [x] **Step 2: Implement native backend**

Native backend calls:

```rust
DuckDbStore::open(path)?.query_events(query, limit)
```

- [x] **Step 3: Leave Quickwit disabled by default**

Config:

```toml
[search]
backend = "native"
```

Do not add Quickwit dependency until native hot/frozen lifecycle is stable.

---

## Execution Order

1. P0-A Hot DuckDB raw pruning and compaction.
2. P0-B Hour/device metrics and UI metric-only charts.
3. P0-C Parser failure observability.
4. P1-B Frozen archive index.
5. P1-A Lifecycle scheduler.
6. P1-C Device ID binding and backfill.
7. P2-B IP region cache.
8. P2-A Unified search API.
9. P2-C Optional external search adapter.

---

## Deployment Checklist

- [x] Build frontend from `X:\`:

```powershell
npm.cmd run build
```

- [x] Copy frontend dist locally:

```powershell
Copy-Item -Path X:\dist\* -Destination .\web -Recurse -Force
```

- [x] Copy frontend dist to server:

```powershell
scp -r X:\dist\* root@192.168.0.142:/opt/oxidelog-src/web/
```

- [x] Build and restart backend:

```bash
cd /opt/oxidelog-src
cargo test -p fwlog-storage
cargo test -p fwlog-api
cargo build --release -p fwlogd
install -m 0755 target/release/fwlogd /opt/oxidelog/bin/fwlogd
systemctl restart oxidelog.service
systemctl is-active oxidelog.service
```

- [x] Smoke test:

```powershell
Invoke-RestMethod http://192.168.0.142:18080/api/health
Invoke-RestMethod "http://192.168.0.142:18080/api/metrics/minutes?hours=24&limit=5"
Invoke-RestMethod http://192.168.0.142:18080/api/devices
```

Expected: health is `ok`, metrics returns JSON array, devices returns JSON array.

---

## Self-Review

- P0 covers bounded hot storage, long-term metrics, onboarding visibility, and parser failure visibility.
- P1 covers lifecycle automation, Frozen index, and stable device identity.
- P2 covers unified search, IP cache, and optional external search without forcing Quickwit/Logstash.
- No task requires the user to make architectural decisions during execution.
- All backend behavior changes include test-first steps.
