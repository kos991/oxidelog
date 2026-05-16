#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [ "$(id -u)" -ne 0 ]; then
  echo "install-linux-service.sh must be run as root." >&2
  exit 1
fi

if [ -f "$HOME/.cargo/env" ]; then
  # Prefer rustup-managed Cargo when installed for the invoking root user.
  # shellcheck disable=SC1090
  . "$HOME/.cargo/env"
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo not found in PATH. Install Rust with rustup before installing OxideLog." >&2
  exit 1
fi

if [ ! -f config/server.toml ]; then
  echo "config/server.toml not found. Run this script from the OxideLog repository root." >&2
  exit 1
fi

cargo build --release -p fwlogd

install -d /opt/oxidelog/bin
install -m 0755 target/release/fwlogd /opt/oxidelog/bin/fwlogd

install -d /etc/oxidelog
if [ ! -f /etc/oxidelog/oxidelog.env ]; then
  cat >/etc/oxidelog/oxidelog.env <<'ENV'
# Optional API protection. Set a non-empty value and restart oxidelog.service.
OXIDELOG_API_TOKEN=
ENV
  chmod 0600 /etc/oxidelog/oxidelog.env
fi
install -d \
  /var/lib/oxidelog/spool \
  /var/lib/oxidelog/duckdb \
  /var/lib/oxidelog/export \
  /var/lib/oxidelog/parquet \
  /var/lib/oxidelog/frozen

tmp_config="$(mktemp)"
sed \
  -e 's#^\([[:space:]]*root[[:space:]]*=[[:space:]]*\).*#\1"/var/lib/oxidelog"#' \
  -e 's#^\([[:space:]]*duckdb_path[[:space:]]*=[[:space:]]*\).*#\1"/var/lib/oxidelog/duckdb/oxidelog.duckdb"#' \
  -e 's#^\([[:space:]]*spool_dir[[:space:]]*=[[:space:]]*\).*#\1"/var/lib/oxidelog/spool"#' \
  -e 's#^\([[:space:]]*export_dir[[:space:]]*=[[:space:]]*\).*#\1"/var/lib/oxidelog/export"#' \
  -e 's#^\([[:space:]]*parquet_dir[[:space:]]*=[[:space:]]*\).*#\1"/var/lib/oxidelog/parquet"#' \
  -e 's#^\([[:space:]]*frozen_dir[[:space:]]*=[[:space:]]*\).*#\1"/var/lib/oxidelog/frozen"#' \
  config/server.toml >"$tmp_config"
install -m 0644 "$tmp_config" /etc/oxidelog/config.toml
rm -f "$tmp_config"

cat >/etc/systemd/system/oxidelog.service <<'UNIT'
[Unit]
Description=OxideLog V3 service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/var/lib/oxidelog
Environment=HOME=/var/lib/oxidelog
EnvironmentFile=-/etc/oxidelog/oxidelog.env
ExecStart=/opt/oxidelog/bin/fwlogd --config /etc/oxidelog/config.toml
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable oxidelog.service
systemctl restart oxidelog.service
systemctl status oxidelog.service --no-pager
