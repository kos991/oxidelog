#!/usr/bin/env bash
set -euo pipefail

DUCKDB="${1:-/var/lib/oxidelog/duckdb/oxidelog.duckdb}"
SERVICE="${OXIDELOG_SERVICE:-oxidelog.service}"
IMPORT_BIN="${OXIDELOG_IMPORT_BIN:-/opt/oxidelog/bin/fwlog-import}"
RAW_INPUT="${OXIDELOG_RAW_INPUT:-/opt/sangfor_fw_log}"
FROZEN_DIR="${OXIDELOG_FROZEN_DIR:-/var/lib/oxidelog/frozen}"
STAMP="$(date +%Y%m%d-%H%M%S)"
COMPACT="${DUCKDB%.duckdb}.compact-${STAMP}.duckdb"
BACKUP="${DUCKDB%.duckdb}.backup-${STAMP}.duckdb"

if [ "$(id -u)" -ne 0 ]; then
  echo "compact-hot-linux.sh must be run as root." >&2
  exit 1
fi

if [ ! -x "$IMPORT_BIN" ]; then
  echo "fwlog-import not found at $IMPORT_BIN" >&2
  exit 1
fi

install -d "$FROZEN_DIR"
if [ -e "$RAW_INPUT" ]; then
  tar -C "$(dirname "$RAW_INPUT")" -I 'zstd -3 -T0' -cf "$FROZEN_DIR/raw-import-${STAMP}.tar.zst" "$(basename "$RAW_INPUT")"
fi

was_active=false
if systemctl is-active --quiet "$SERVICE"; then
  was_active=true
  systemctl stop "$SERVICE"
fi

start_service() {
  if [ "$was_active" = true ]; then
    systemctl start "$SERVICE"
  fi
}
trap start_service EXIT

"$IMPORT_BIN" --duckdb "$DUCKDB" --compact-output "$COMPACT" --drop-parsed-raw
mv "$DUCKDB" "$BACKUP"
mv "$COMPACT" "$DUCKDB"

trap - EXIT
start_service

du -h "$DUCKDB" "$BACKUP"
systemctl status "$SERVICE" --no-pager
