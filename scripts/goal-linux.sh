#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [ -f "$HOME/.cargo/env" ]; then
  # Prefer rustup-managed stable Rust over old distro cargo packages.
  # shellcheck disable=SC1090
  . "$HOME/.cargo/env"
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo not found in PATH. Install Rust with rustup before running this goal." >&2
  exit 1
fi

rm -rf data
mkdir -p data/spool data/duckdb data/export data/parquet data/frozen

cargo test --workspace
cargo build --workspace

./target/debug/fwlogd --config config/local.toml >data/fwlogd.out.log 2>data/fwlogd.err.log &
pid=$!

cleanup() {
  if kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid" || true
    wait "$pid" || true
  fi
}
trap cleanup EXIT

healthy=0
for _ in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:18080/api/health >/dev/null 2>&1; then
    healthy=1
    break
  fi
  sleep 1
done

if [ "$healthy" -ne 1 ]; then
  echo "fwlogd did not become healthy within 30 seconds. See data/fwlogd.out.log and data/fwlogd.err.log" >&2
  exit 1
fi

exec 3<>/dev/tcp/127.0.0.1/1514
while IFS= read -r line; do
  printf '%s\n' "$line" >&3
done < samples/sangfor.log
exec 3>&-

sleep 2

curl -fsS "http://127.0.0.1:18080/api/events?limit=20" -o data/export/events.json
curl -fsS "http://127.0.0.1:18080/api/events/export.csv?limit=20" -o data/export/events.csv
curl -fsS -X POST "http://127.0.0.1:18080/api/archive/parquet?limit=20" -o data/export/archive.json
curl -fsS "http://127.0.0.1:18080/api/archive/files" -o data/export/archive-files.json
curl -fsS -X POST "http://127.0.0.1:18080/api/archive/frozen?limit=20" -o data/export/frozen.json
curl -fsS "http://127.0.0.1:18080/api/archive/frozen" -o data/export/frozen-files.json
frozen_path=$(python3 - <<'PY'
import json
from urllib.parse import quote
with open("data/export/frozen.json", "r", encoding="utf-8") as f:
    frozen = json.load(f)
print(quote(frozen["path"], safe=""))
PY
)
curl -fsS "http://127.0.0.1:18080/api/archive/frozen/restore?path=$frozen_path" -o data/export/frozen-restored.json

ingested=$(python3 - <<'PY'
import json
with open("data/export/events.json", "r", encoding="utf-8") as f:
    events = json.load(f)
print(len(events))
PY
)
parsed=$(python3 - <<'PY'
import json
with open("data/export/events.json", "r", encoding="utf-8") as f:
    events = json.load(f)
print(sum(1 for e in events if e.get("parse_status") == "parsed"))
PY
)
failed=$(python3 - <<'PY'
import json
with open("data/export/events.json", "r", encoding="utf-8") as f:
    events = json.load(f)
print(sum(1 for e in events if e.get("parse_status") == "failed"))
PY
)
archive_files=$(python3 - <<'PY'
import json
with open("data/export/archive-files.json", "r", encoding="utf-8") as f:
    files = json.load(f)
print(len(files))
PY
)
frozen_files=$(python3 - <<'PY'
import json
with open("data/export/frozen-files.json", "r", encoding="utf-8") as f:
    files = json.load(f)
print(len(files))
PY
)
restored_lines=$(python3 - <<'PY'
import json
with open("data/export/frozen-restored.json", "r", encoding="utf-8") as f:
    lines = json.load(f)
print(len(lines))
PY
)

if [ "$ingested" -lt 5 ] || [ "$parsed" -lt 4 ] || [ "$failed" -lt 1 ]; then
  echo "unexpected goal counts: ingested=$ingested parsed=$parsed failed=$failed" >&2
  exit 1
fi
if [ "$archive_files" -lt 1 ]; then
  echo "expected at least one archive file, got $archive_files" >&2
  exit 1
fi
if [ "$frozen_files" -lt 1 ]; then
  echo "expected at least one frozen archive file, got $frozen_files" >&2
  exit 1
fi
if [ "$restored_lines" -lt 5 ]; then
  echo "expected at least five restored frozen lines, got $restored_lines" >&2
  exit 1
fi

echo "OxideLog V3 local goal passed"
echo "API: http://127.0.0.1:18080"
echo "Ingested: $ingested"
echo "Parsed: $parsed"
echo "Failed: $failed"
echo "Export: data/export/events.csv"
echo "Archives: $archive_files"
echo "Frozen archives: $frozen_files"
echo "Restored frozen lines: $restored_lines"
