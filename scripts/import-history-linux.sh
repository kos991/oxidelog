#!/usr/bin/env bash
set -euo pipefail

INPUT="${1:-/opt/sangfor_fw_log}"
DUCKDB="${2:-/var/lib/oxidelog/duckdb/oxidelog.duckdb}"
BATCH_SIZE="${3:-500000}"
SERVICE="${OXIDELOG_SERVICE:-oxidelog.service}"
IMPORT_BIN="${OXIDELOG_IMPORT_BIN:-/opt/oxidelog/bin/fwlog-import}"
HOT_LIMIT="${OXIDELOG_HOT_LIMIT:-100000}"

if [ "$(id -u)" -ne 0 ]; then
  echo "import-history-linux.sh must be run as root." >&2
  exit 1
fi

if [ ! -e "$INPUT" ]; then
  echo "input path not found: $INPUT" >&2
  exit 1
fi

if [ ! -x "$IMPORT_BIN" ]; then
  if [ -f Cargo.toml ]; then
    cargo build --release -p fwlog-import
    IMPORT_BIN="target/release/fwlog-import"
  else
    echo "fwlog-import not found at $IMPORT_BIN and no Cargo.toml is available to build it." >&2
    exit 1
  fi
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

echo "Starting bulk import from $INPUT..."
"$IMPORT_BIN" --input "$INPUT" --duckdb "$DUCKDB" --batch-size "$BATCH_SIZE"

echo "Import complete. Starting database compaction (keeping latest $HOT_LIMIT rows)..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
bash "$SCRIPT_DIR/compact-hot-linux.sh" "$DUCKDB" "$HOT_LIMIT"

trap - EXIT
start_service
systemctl status "$SERVICE" --no-pager
