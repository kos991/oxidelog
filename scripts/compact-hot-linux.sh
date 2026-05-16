#!/usr/bin/env bash
set -euo pipefail

DUCKDB="${1:-/var/lib/oxidelog/duckdb/oxidelog.duckdb}"
HOT_LIMIT="${2:-100000}"
SERVICE="${OXIDELOG_SERVICE:-oxidelog.service}"
IMPORT_BIN="${OXIDELOG_IMPORT_BIN:-/opt/oxidelog/bin/fwlog-import}"
RAW_INPUT="${OXIDELOG_RAW_INPUT:-/opt/sangfor_fw_log}"
FROZEN_DIR="${OXIDELOG_FROZEN_DIR:-/var/lib/oxidelog/frozen}"
PARQUET_DIR="${OXIDELOG_PARQUET_DIR:-/var/lib/oxidelog/parquet}"
KEEP_BACKUP="${OXIDELOG_KEEP_BACKUP:-false}"
SKIP_PARQUET="${OXIDELOG_SKIP_PARQUET:-true}"
STAMP="$(date +%Y%m%d-%H%M%S)"
COMPACT="${DUCKDB%.duckdb}.compact-${STAMP}.duckdb"
BACKUP="${DUCKDB%.duckdb}.backup-${STAMP}.duckdb"
PARQUET="$PARQUET_DIR/all-events-${STAMP}.parquet"

if [ "$(id -u)" -ne 0 ]; then
  echo "compact-hot-linux.sh must be run as root." >&2
  exit 1
fi

if [ ! -x "$IMPORT_BIN" ]; then
  echo "fwlog-import not found at $IMPORT_BIN" >&2
  exit 1
fi

install -d "$FROZEN_DIR" "$PARQUET_DIR"
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

import_args=(
  --duckdb "$DUCKDB"
  --compact-output "$COMPACT"
  --fast-hot-limit "$HOT_LIMIT"
  --drop-parsed-raw
)
if [ "$SKIP_PARQUET" != true ]; then
  import_args+=(--archive-slim-parquet "$PARQUET")
fi

"$IMPORT_BIN" "${import_args[@]}"
mv "$DUCKDB" "$BACKUP"
mv "$COMPACT" "$DUCKDB"

trap - EXIT
start_service

if [ "$KEEP_BACKUP" != true ]; then
  rm -f "$BACKUP"
fi

du -h "$DUCKDB"
if [ -f "$PARQUET" ]; then
  du -h "$PARQUET"
fi
systemctl status "$SERVICE" --no-pager
