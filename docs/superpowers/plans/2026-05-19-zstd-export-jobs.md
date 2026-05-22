# Zstd Export Jobs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build large log export jobs that generate zstd-compressed CSV files on the server, require a firewall device for one-year exports, and auto-clean files after 24 hours.

**Architecture:** Small exports can keep the existing direct CSV path. Large/date-range exports create a server-side job under `data/exports`, run in the background, write `csv.zst`, expose job status and download endpoints, and prune expired files on job creation/listing. The frontend chooses job export for one-year or long-range exports and polls status until downloadable.

**Tech Stack:** Rust axum API, DuckDB-backed search/export code, zstd compression, Ant Design Pro frontend, Jest/Rust tests.

---

### Task 1: Backend Export Job API

**Files:**
- Modify: `crates/fwlog-api/src/handlers.rs`
- Modify: `crates/fwlog-api/src/lib.rs`

- [ ] Add failing tests for `POST /api/export/jobs` rejecting one-year jobs without `device` or `device_id`.
- [ ] Add tests for successful job creation returning `job_id`, `status`, `download_url`, and `.csv.zst` naming.
- [ ] Implement in-memory job metadata plus disk output under `data/exports` or a path derived from the configured DuckDB directory.
- [ ] Add `GET /api/export/jobs`, `GET /api/export/jobs/:id`, and `GET /api/export/jobs/:id/download`.

### Task 2: Zstd CSV Writer and Cleanup

**Files:**
- Modify: `crates/fwlog-api/src/handlers.rs`

- [ ] Add failing tests that generated files start with zstd magic bytes and expired files older than 24 hours are removed.
- [ ] Implement background job execution with `csv::Writer` writing to `zstd::stream::write::Encoder`.
- [ ] Track status as `queued/running/completed/failed/expired`, row count, file size, error message, and timestamps.
- [ ] Run cleanup before creating/listing jobs and delete files older than 24 hours.

### Task 3: Frontend Services and UI

**Files:**
- Modify: `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/services/oxidelog.ts`
- Modify: `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/index.tsx`

- [ ] Add service methods for creating/listing/export job status and download URL.
- [ ] Frontend detects one-year or >31 day range and uses export job API instead of direct CSV.
- [ ] Enforce `device`/`device_id` selection before year export.
- [ ] Add a compact export-job panel showing status, row count, compressed size, and download action.

### Task 4: Verification and Deployment

**Files:**
- Build output: `web/`

- [ ] Run focused Rust API tests.
- [ ] Run focused frontend tests/build.
- [ ] Build frontend through `O:` path if needed.
- [ ] Copy dist to `web/`, deploy to server, rebuild/restart `oxidelog.service`, and verify `/api/health` plus export job endpoints.
