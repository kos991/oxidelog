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

Uninstall the service, installed binary, and installed config while keeping `/var/lib/oxidelog` data intact:

```bash
sudo bash scripts/uninstall-linux-service.sh
```

Default local endpoints:

- API: `http://127.0.0.1:18080`
- TCP syslog input: `127.0.0.1:1514`
- UDP syslog input: `127.0.0.1:1515`

Archive API:

- `GET /api/system/status` returns service identity, configured DuckDB/archive paths, DuckDB size, and Parquet/frozen archive file counts and byte totals as JSON.
- `POST /api/archive/parquet?limit=1000` writes `data/parquet/events-YYYYMMDD-HHMMSS.parquet` and returns the archive file metadata as JSON.
- `GET /api/archive/files` lists parquet archive files as JSON.
- `POST /api/archive/frozen?limit=1000` writes `data/frozen/frozen-YYYYMMDD-HHMMSS.raw.zst` from recent event raw fields and returns the frozen file metadata as JSON.
- `GET /api/archive/frozen` lists frozen `.raw.zst` archive files as JSON.
- `GET /api/archive/frozen/restore?path=<path>` reads a frozen archive under `data/frozen` and returns restored raw lines as JSON.

Server-facing config is available at `config/server.toml` and binds API/TCP/UDP to `0.0.0.0`.
