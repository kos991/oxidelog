# OxideLog V3

Run the local goal:

```powershell
.\scripts\goal.ps1
```

The goal builds, tests, starts the local daemon, ingests sample Sangfor logs, queries the API, exports CSV, and stops the daemon.
It also writes Parquet and frozen Zstd raw archives, lists them, and verifies frozen restore.

Deploy as a Linux systemd service:

```powershell
.\scripts\deploy-linux.ps1
```

The deployment script clones or updates `main` into `/opt/oxidelog-src` by default, then runs `scripts/install-linux-service.sh` on the server. The server must have `git`, `cargo`, `systemd`, and SSH access to the repository. The default deploy user is `root` because the installer writes to `/opt/oxidelog`, `/etc/oxidelog`, `/var/lib/oxidelog`, and `/etc/systemd/system`.

Install directly on a Linux server from the repository root:

```bash
sudo bash scripts/install-linux-service.sh
```

Check service status and logs:

```bash
sudo systemctl status oxidelog.service --no-pager
sudo journalctl -u oxidelog.service -f
```

Run a one-click production smoke verification against the deployed API:

```powershell
.\scripts\smoke-production.ps1
```

```bash
bash scripts/smoke-production.sh
```

The production smoke defaults to API `http://192.168.0.142:18080` and TCP ingest `192.168.0.142:1514`. It checks `/api/health`, `/api/system/status`, sample TCP ingest, `/api/events`, CSV export, Parquet archive/list, and Frozen archive/list/restore. JSON, CSV, and ingested sample artifacts are written under `smoke-production-output/<timestamp>/`.

Import historical Sangfor firewall logs in bulk:

```powershell
.\scripts\import-history.ps1 -LocalInput "D:\项目工程\OxideLog\sangfor_fw_log"
```

```bash
sudo bash scripts/import-history-linux.sh /opt/sangfor_fw_log /var/lib/oxidelog/duckdb/oxidelog.duckdb 500000
```

The import script stops `oxidelog.service`, uses the high-throughput `fwlog-import` binary, and starts the service again when the import finishes. Use TCP/UDP syslog only for live firewall traffic; do not replay large historical files through TCP.

Compact the hot DuckDB file after a bulk historical import:

```powershell
.\scripts\compact-hot.ps1
```

By default this archives slim structured fields to Parquet, keeps the latest 1,000,000 rows in DuckDB for UI/API hot queries, moves the raw import directory into a Zstd tar archive under frozen storage, and drops raw text from parsed hot rows so the query database stays small.

If `[auth].api_token` is set in the server config, or `OXIDELOG_API_TOKEN` is set in `/etc/oxidelog/oxidelog.env`, pass the same token with `-ApiToken`, `--api-token`, or the `OXIDELOG_API_TOKEN` environment variable. The token is sent as `Authorization: Bearer <token>` and is not printed by the smoke scripts.

Override targets or skip TCP ingest when needed:

```powershell
.\scripts\smoke-production.ps1 -ApiHost 10.0.0.12 -ApiPort 18080 -TcpHost 10.0.0.12 -TcpPort 1514
.\scripts\smoke-production.ps1 -ApiToken "change-me"
.\scripts\smoke-production.ps1 -NoIngest
```

```bash
bash scripts/smoke-production.sh --api-host 10.0.0.12 --api-port 18080 --tcp-host 10.0.0.12 --tcp-port 1514
bash scripts/smoke-production.sh --api-token "change-me"
bash scripts/smoke-production.sh --no-ingest
```

Uninstall the service, installed binary, and installed config while keeping `/var/lib/oxidelog` data intact:

```bash
sudo bash scripts/uninstall-linux-service.sh
```

Default local endpoints:

- API: `http://127.0.0.1:18080`
- UI: `http://127.0.0.1:18080/`
- TCP syslog input: `127.0.0.1:1514`
- UDP syslog input: `127.0.0.1:1515`

Production hardening knobs:

- `[auth].api_token`: optional API Bearer token. Empty means API routes are open. `/` and `/app` remain reachable so operators can enter the token in the UI.
- `OXIDELOG_API_TOKEN`: environment override for the API token. The systemd installer creates `/etc/oxidelog/oxidelog.env` with this variable so secrets do not have to live in the repository config.
- `[archive].enabled`: enables periodic Parquet and Frozen archive cycles.
- `[archive].interval_seconds`: archive cycle interval. The daemon enforces a minimum of 60 seconds.
- `[archive].batch_limit`: maximum recent events included in each periodic archive file.
- `[archive].parquet_retention_days` and `[archive].frozen_retention_days`: remove expired archive files during each archive cycle.

Archive API:

- `GET /api/system/status` returns service identity, auth state, configured DuckDB/archive paths, event counts, runtime metrics, DuckDB size, and Parquet/frozen archive file counts and byte totals as JSON.
- `POST /api/archive/parquet?limit=1000` writes `data/parquet/events-YYYYMMDD-HHMMSS.parquet` and returns the archive file metadata as JSON.
- `GET /api/archive/files` lists parquet archive files as JSON.
- `POST /api/archive/frozen?limit=1000` writes `data/frozen/frozen-YYYYMMDD-HHMMSS.raw.zst` from recent event raw fields and returns the frozen file metadata as JSON.
- `GET /api/archive/frozen` lists frozen `.raw.zst` archive files as JSON.
- `GET /api/archive/frozen/restore?path=<path>` reads a frozen archive under `data/frozen` and returns restored raw lines as JSON.

Server-facing config is available at `config/server.toml` and binds API/TCP/UDP to `0.0.0.0`.
