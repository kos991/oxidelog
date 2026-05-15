# OxideLog V3

Run the local goal:

```powershell
.\scripts\goal.ps1
```

The goal builds, tests, starts the local daemon, ingests sample Sangfor logs, queries the API, exports CSV, and stops the daemon.

Deploy and verify on a Linux server:

```powershell
.\scripts\deploy-linux.ps1
```

The deployment script clones `main` by default and runs the same goal on the server. The server must have `git`, `cargo`, `curl`, `python3`, and SSH access to the repository.

Default local endpoints:

- API: `http://127.0.0.1:18080`
- TCP syslog input: `127.0.0.1:1514`
- UDP syslog input: `127.0.0.1:1515`

Archive API:

- `POST /api/archive/parquet?limit=1000` writes `data/parquet/events-YYYYMMDD-HHMMSS.parquet` and returns the archive file metadata as JSON.
- `GET /api/archive/files` lists parquet archive files as JSON.

Server-facing config is available at `config/server.toml` and binds API/TCP/UDP to `0.0.0.0`.
