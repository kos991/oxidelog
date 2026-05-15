# OxideLog V3 Local Goal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a local one-command OxideLog V3 goal that proves the core pipeline works: sample firewall logs are ingested, spooled, parsed, stored in DuckDB, queried through an API, and exported to CSV.

**Architecture:** Start with a Rust workspace and a narrow vertical slice. The first runnable goal avoids UI and archive complexity, but keeps module boundaries compatible with the full V3 architecture: ingress, spool, adapter, storage, API, and daemon orchestration.

**Tech Stack:** Rust stable, Tokio, Axum, DuckDB, Regex, Serde, Clap, Tracing, PowerShell goal script, local filesystem data directories.

---

## Locked Decisions

These choices are fixed for implementation so the user is not asked to judge every branch later.

- Default local TCP port: `1514`, because Windows/Linux/macOS often require elevated privileges for port `514`.
- Default local UDP port: `1515`, for the same reason.
- Default API port: `8080`.
- Default data directory: `./data`.
- First goal scope: local vertical slice only.
- First goal excludes frontend, Parquet archive, and Frozen archive, but reserves their module paths and configuration keys.
- First storage engine: DuckDB hot store.
- First parser target: Sangfor-style text logs plus failed-event fallback.
- Failed parse policy: always store event with `parse_status = "failed"` and original `raw`.
- Replay policy: each spool line receives deterministic `event_id = SHA256(raw + ingest_time_nanos + source_addr)`.
- One-click command: `.\scripts\goal.ps1`.

## File Map

Create these files:

- `Cargo.toml` - Rust workspace definition.
- `apps/fwlogd/Cargo.toml` - daemon crate dependencies.
- `apps/fwlogd/src/main.rs` - CLI, tracing, configuration load, task orchestration.
- `apps/fwlogd/src/pipeline.rs` - connects ingress, spool, parser, storage, and API.
- `crates/fwlog-domain/Cargo.toml` - domain crate dependencies.
- `crates/fwlog-domain/src/lib.rs` - public exports.
- `crates/fwlog-domain/src/event.rs` - `CanonicalEvent`, `ParseStatus`, `Protocol`, `Action`.
- `crates/fwlog-domain/src/raw.rs` - `RawLog` and source metadata.
- `crates/fwlog-adapter/Cargo.toml` - parser crate dependencies.
- `crates/fwlog-adapter/src/lib.rs` - public parser API.
- `crates/fwlog-adapter/src/sangfor.rs` - Sangfor parser implementation.
- `crates/fwlog-spool/Cargo.toml` - spool crate dependencies.
- `crates/fwlog-spool/src/lib.rs` - public spool API.
- `crates/fwlog-spool/src/segment.rs` - segment writer, reader, and line format.
- `crates/fwlog-storage/Cargo.toml` - storage crate dependencies.
- `crates/fwlog-storage/src/lib.rs` - public storage API.
- `crates/fwlog-storage/src/duckdb.rs` - DuckDB schema, insert, query, export.
- `crates/fwlog-ingress/Cargo.toml` - ingress crate dependencies.
- `crates/fwlog-ingress/src/lib.rs` - public ingress API.
- `crates/fwlog-ingress/src/tcp.rs` - TCP line listener.
- `crates/fwlog-ingress/src/udp.rs` - UDP datagram listener.
- `crates/fwlog-api/Cargo.toml` - API crate dependencies.
- `crates/fwlog-api/src/lib.rs` - Axum router.
- `crates/fwlog-api/src/handlers.rs` - health, query, export handlers.
- `config/local.toml` - local ports and data directory defaults.
- `samples/sangfor.log` - deterministic sample log input.
- `scripts/goal.ps1` - one-command local build, test, run, ingest, query, export.
- `README.md` - exact user-facing commands.

## Goal Definition

`.\scripts\goal.ps1` must perform these actions without further user input:

1. Verify `cargo` exists.
2. Create `data/spool`, `data/duckdb`, and `data/export`.
3. Run `cargo test --workspace`.
4. Run `cargo build --workspace`.
5. Start `fwlogd` in the background with `config/local.toml`.
6. Wait until `GET http://127.0.0.1:8080/api/health` returns `200`.
7. Send every line from `samples/sangfor.log` to TCP `127.0.0.1:1514`.
8. Query `GET http://127.0.0.1:8080/api/events?limit=20`.
9. Export `GET http://127.0.0.1:8080/api/events/export.csv` to `data/export/events.csv`.
10. Print a final success block containing ingested count, parsed count, failed count, and export path.
11. Stop the background daemon process.

Expected final output shape:

```text
OxideLog V3 local goal passed
API: http://127.0.0.1:8080
Ingested: 5
Parsed: 4
Failed: 1
Export: data/export/events.csv
```

## Task 1: Workspace Scaffold

**Files:**
- Create: `Cargo.toml`
- Create: all crate directories listed in File Map

- [ ] **Step 1: Create workspace manifest**

Create `Cargo.toml`:

```toml
[workspace]
resolver = "2"
members = [
  "apps/fwlogd",
  "crates/fwlog-domain",
  "crates/fwlog-adapter",
  "crates/fwlog-spool",
  "crates/fwlog-storage",
  "crates/fwlog-ingress",
  "crates/fwlog-api",
]

[workspace.package]
edition = "2021"
license = "MIT"
version = "0.1.0"

[workspace.dependencies]
anyhow = "1"
axum = "0.7"
chrono = { version = "0.4", features = ["serde"] }
clap = { version = "4", features = ["derive"] }
flume = "0.11"
futures = "0.3"
regex = "1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
sha2 = "0.10"
tempfile = "3"
tokio = { version = "1", features = ["full"] }
tokio-util = { version = "0.7", features = ["codec"] }
tower-http = { version = "0.5", features = ["cors", "trace"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
uuid = { version = "1", features = ["v4", "serde"] }
```

- [ ] **Step 2: Add minimal crate manifests**

Each crate must have a `Cargo.toml` with `edition.workspace = true`. Internal dependencies must use `{ path = "../crate-name" }`.

- [ ] **Step 3: Verify scaffold**

Run:

```powershell
cargo metadata --no-deps
```

Expected: command exits with code `0` and lists all workspace members.

## Task 2: Domain Model

**Files:**
- Create: `crates/fwlog-domain/src/event.rs`
- Create: `crates/fwlog-domain/src/raw.rs`
- Create: `crates/fwlog-domain/src/lib.rs`

- [ ] **Step 1: Write model tests**

Create unit tests in `event.rs` that verify:

- parsed events serialize `parse_status` as `"parsed"`.
- failed events preserve `raw`.
- `event_id` is stable when the same raw/source/timestamp are provided.

- [ ] **Step 2: Implement event model**

Required public API:

```rust
pub enum ParseStatus {
    Parsed,
    Failed,
}

pub struct CanonicalEvent {
    pub event_id: String,
    pub ingest_time: chrono::DateTime<chrono::Utc>,
    pub event_time: Option<chrono::DateTime<chrono::Utc>>,
    pub vendor: Option<String>,
    pub product: Option<String>,
    pub src_ip: Option<String>,
    pub src_port: Option<u16>,
    pub dst_ip: Option<String>,
    pub dst_port: Option<u16>,
    pub protocol: Option<String>,
    pub action: Option<String>,
    pub severity: Option<String>,
    pub raw: String,
    pub parse_status: ParseStatus,
    pub parse_error: Option<String>,
}
```

- [ ] **Step 3: Run tests**

Run:

```powershell
cargo test -p fwlog-domain
```

Expected: all domain tests pass.

## Task 3: Sangfor Adapter

**Files:**
- Create: `crates/fwlog-adapter/src/lib.rs`
- Create: `crates/fwlog-adapter/src/sangfor.rs`
- Modify: `crates/fwlog-adapter/Cargo.toml`
- Create: `samples/sangfor.log`

- [ ] **Step 1: Add sample logs**

Create `samples/sangfor.log` with five lines:

```text
<134>May 15 10:00:01 fw Sangfor: src=192.168.1.10 dst=8.8.8.8 sport=51514 dport=53 proto=UDP action=allow severity=info
<134>May 15 10:00:02 fw Sangfor: src=192.168.1.20 dst=1.1.1.1 sport=44321 dport=443 proto=TCP action=deny severity=high
Sangfor: src=10.0.0.5 dst=172.16.0.10 sport=12345 dport=80 proto=TCP action=allow severity=medium
date=2026-05-15 src=10.10.10.10 dst=10.10.20.20 sport=60000 dport=22 proto=TCP action=deny severity=critical
this is not a valid firewall log
```

- [ ] **Step 2: Write parser tests**

Tests must assert:

- first four lines return `ParseStatus::Parsed`.
- invalid fifth line returns `ParseStatus::Failed`.
- failed event contains `parse_error`.
- syslog prefix does not prevent field extraction.

- [ ] **Step 3: Implement parser**

Required public API:

```rust
pub trait LogAdapter {
    fn parse(&self, raw: fwlog_domain::RawLog) -> fwlog_domain::CanonicalEvent;
}

pub struct SangforAdapter;
```

Implementation rules:

- Use `OnceLock<Regex>` for compiled regex.
- Extract `src`, `dst`, `sport`, `dport`, `proto`, `action`, and `severity`.
- Set `vendor = "Sangfor"`.
- Set `product = "Firewall"` when parsed.
- Store raw text unchanged.
- Return failed event when required fields `src`, `dst`, and `action` are missing.

- [ ] **Step 4: Run tests**

Run:

```powershell
cargo test -p fwlog-adapter
```

Expected: adapter tests pass.

## Task 4: Spool Segment

**Files:**
- Create: `crates/fwlog-spool/src/lib.rs`
- Create: `crates/fwlog-spool/src/segment.rs`

- [ ] **Step 1: Write spool tests**

Tests must verify:

- appending three raw logs writes three JSONL records.
- reopening a sealed segment can read all records.
- checkpoint after line `2` replays only line `3`.

- [ ] **Step 2: Implement spool format**

Use JSON Lines:

```json
{"offset":1,"ingest_time":"2026-05-15T00:00:00Z","source_addr":"tcp://127.0.0.1:1514","raw":"..."}
```

Required public API:

```rust
pub struct SegmentWriter;
pub struct SegmentReader;
pub struct SpoolRecord;
pub struct SpoolCheckpoint;
```

Rules:

- Segment file extension while writing: `.open`.
- Segment file extension after sealing: `.sealed`.
- Segment name format: `segment-YYYYMMDD-HHMMSS-NNNNNN`.
- `seal()` must flush and rename the file.

- [ ] **Step 3: Run tests**

Run:

```powershell
cargo test -p fwlog-spool
```

Expected: spool tests pass.

## Task 5: DuckDB Hot Storage

**Files:**
- Create: `crates/fwlog-storage/src/lib.rs`
- Create: `crates/fwlog-storage/src/duckdb.rs`

- [ ] **Step 1: Add DuckDB dependency**

Use the Rust DuckDB crate with bundled support if available in the current registry. If bundled support is unavailable on the host, document the required native DuckDB install in `README.md` and keep the code API unchanged.

- [ ] **Step 2: Write storage tests**

Tests must verify:

- database initializes schema.
- batch insert stores parsed and failed events.
- query by `limit = 10` returns newest events first.
- CSV export writes a header and all selected rows.

- [ ] **Step 3: Implement schema**

Create table:

```sql
CREATE TABLE IF NOT EXISTS events (
  event_id TEXT PRIMARY KEY,
  ingest_time TIMESTAMP NOT NULL,
  event_time TIMESTAMP,
  vendor TEXT,
  product TEXT,
  src_ip TEXT,
  src_port INTEGER,
  dst_ip TEXT,
  dst_port INTEGER,
  protocol TEXT,
  action TEXT,
  severity TEXT,
  raw TEXT NOT NULL,
  parse_status TEXT NOT NULL,
  parse_error TEXT
);
```

Required public API:

```rust
pub struct DuckDbStore;

impl DuckDbStore {
    pub fn open(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self>;
    pub fn insert_batch(&self, events: &[CanonicalEvent]) -> anyhow::Result<usize>;
    pub fn query_recent(&self, limit: usize) -> anyhow::Result<Vec<CanonicalEvent>>;
    pub fn export_csv(&self, path: impl AsRef<std::path::Path>, limit: usize) -> anyhow::Result<usize>;
}
```

- [ ] **Step 4: Run tests**

Run:

```powershell
cargo test -p fwlog-storage
```

Expected: storage tests pass.

## Task 6: Ingress

**Files:**
- Create: `crates/fwlog-ingress/src/lib.rs`
- Create: `crates/fwlog-ingress/src/tcp.rs`
- Create: `crates/fwlog-ingress/src/udp.rs`

- [ ] **Step 1: Write ingress tests**

Tests must verify:

- TCP listener receives two newline-delimited logs from one connection.
- UDP listener receives one datagram as one raw log.
- queue full condition increments a dropped counter for UDP.

- [ ] **Step 2: Implement TCP listener**

Rules:

- Use `tokio_util::codec::LinesCodec`.
- Do not parse in ingress.
- Send `RawLog` into a bounded channel.
- Preserve `source_addr`.

- [ ] **Step 3: Implement UDP listener**

Rules:

- Read datagrams into a fixed buffer of `65535`.
- If queue is full, drop datagram and increment `udp_dropped_total`.
- Preserve `source_addr`.

- [ ] **Step 4: Run tests**

Run:

```powershell
cargo test -p fwlog-ingress
```

Expected: ingress tests pass.

## Task 7: API

**Files:**
- Create: `crates/fwlog-api/src/lib.rs`
- Create: `crates/fwlog-api/src/handlers.rs`

- [ ] **Step 1: Write API tests**

Tests must verify:

- `GET /api/health` returns `200`.
- `GET /api/events?limit=20` returns JSON array.
- `GET /api/events/export.csv` returns CSV body with header.

- [ ] **Step 2: Implement routes**

Routes:

```text
GET /api/health
GET /api/events?limit=20
GET /api/events/export.csv?limit=1000
```

Health response:

```json
{"status":"ok","service":"fwlogd"}
```

- [ ] **Step 3: Run tests**

Run:

```powershell
cargo test -p fwlog-api
```

Expected: API tests pass.

## Task 8: Daemon Pipeline

**Files:**
- Create: `apps/fwlogd/src/main.rs`
- Create: `apps/fwlogd/src/pipeline.rs`
- Create: `config/local.toml`

- [ ] **Step 1: Create local config**

Create `config/local.toml`:

```toml
[server]
api_addr = "127.0.0.1:8080"
tcp_addr = "127.0.0.1:1514"
udp_addr = "127.0.0.1:1515"

[data]
root = "data"
duckdb_path = "data/duckdb/oxidelog.duckdb"
spool_dir = "data/spool"
export_dir = "data/export"

[pipeline]
ingress_queue = 100000
batch_size = 10000
flush_interval_ms = 1000
```

- [ ] **Step 2: Implement CLI**

Required command:

```powershell
cargo run -p fwlogd -- --config config/local.toml
```

CLI flags:

```text
--config <path>
```

- [ ] **Step 3: Implement pipeline**

Runtime behavior:

- create data directories.
- open DuckDB.
- start TCP ingress.
- start UDP ingress.
- start worker that writes raw logs to spool.
- parse raw logs with `SangforAdapter`.
- insert parsed/failed events into DuckDB.
- start API server.

- [ ] **Step 4: Run daemon manually**

Run:

```powershell
cargo run -p fwlogd -- --config config/local.toml
```

Expected logs include:

```text
fwlogd listening api=127.0.0.1:8080 tcp=127.0.0.1:1514 udp=127.0.0.1:1515
```

## Task 9: One-Click Goal Script

**Files:**
- Create: `scripts/goal.ps1`
- Modify: `README.md`

- [ ] **Step 1: Implement `scripts/goal.ps1`**

The script must:

- set `$ErrorActionPreference = "Stop"`.
- run from repo root.
- create data directories.
- run workspace tests.
- build the daemon.
- start daemon with `Start-Process -WindowStyle Hidden`.
- poll `/api/health` for up to 30 seconds.
- send sample logs over TCP.
- call `/api/events`.
- call `/api/events/export.csv`.
- stop daemon in `finally`.

- [ ] **Step 2: Add README command**

Create `README.md` with:

```markdown
# OxideLog V3

Run the local goal:

```powershell
.\scripts\goal.ps1
```

The goal builds, tests, starts the local daemon, ingests sample Sangfor logs, queries the API, exports CSV, and stops the daemon.
```

- [ ] **Step 3: Run one-click goal**

Run:

```powershell
.\scripts\goal.ps1
```

Expected:

```text
OxideLog V3 local goal passed
```

## Task 10: Implementation Quality Gate

**Files:**
- Modify only files created by previous tasks.

- [ ] **Step 1: Format**

Run:

```powershell
cargo fmt --all
```

Expected: exits `0`.

- [ ] **Step 2: Lint**

Run:

```powershell
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: exits `0`.

- [ ] **Step 3: Test**

Run:

```powershell
cargo test --workspace
```

Expected: exits `0`.

- [ ] **Step 4: Goal**

Run:

```powershell
.\scripts\goal.ps1
```

Expected: exits `0` and prints final success block.

## Post-Goal Roadmap

After the local goal passes, proceed without further architecture decisions using these default phases:

### Goal 2: Archive

- Add Parquet writer under `crates/fwlog-storage/src/parquet.rs`.
- Add Frozen raw Zstd archive under `crates/fwlog-storage/src/frozen.rs`.
- Add daily archive command: `fwlogd archive --config config/local.toml`.
- Add archive status API: `GET /api/archive/files`.

### Goal 3: Frontend

- Add Ant Design Pro web app under `web/`.
- Implement Chinese SOC pages:
  - event search
  - event detail
  - system status
  - archive files
- Default frontend command: `npm run dev`.

### Goal 4: Production Hardening

- Add service install scripts for Windows.
- Add disk waterline enforcement.
- Add parser metrics.
- Add UDP drop metrics.
- Add replay checkpoint compaction.

## Self-Review

- Spec coverage: local ingest, spool, parser, failed fallback, DuckDB hot store, API query, CSV export, and one-click verification are covered.
- Deferred coverage: Parquet, Frozen, and frontend are intentionally moved to post-goal phases with fixed defaults.
- Placeholder scan: no `TBD`, no open-ended user decisions, no unspecified commands.
- Type consistency: `CanonicalEvent`, `RawLog`, `SangforAdapter`, `DuckDbStore`, and route paths are consistent across tasks.
