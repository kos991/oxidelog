#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo not found in PATH. Install Rust with rustup before running this goal." >&2
  exit 1
fi

rm -rf data
mkdir -p data/spool data/duckdb data/export

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
  if curl -fsS http://127.0.0.1:8080/api/health >/dev/null; then
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

curl -fsS "http://127.0.0.1:8080/api/events?limit=20" -o data/export/events.json
curl -fsS "http://127.0.0.1:8080/api/events/export.csv?limit=20" -o data/export/events.csv

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

if [ "$ingested" -lt 5 ] || [ "$parsed" -lt 4 ] || [ "$failed" -lt 1 ]; then
  echo "unexpected goal counts: ingested=$ingested parsed=$parsed failed=$failed" >&2
  exit 1
fi

echo "OxideLog V3 local goal passed"
echo "API: http://127.0.0.1:8080"
echo "Ingested: $ingested"
echo "Parsed: $parsed"
echo "Failed: $failed"
echo "Export: data/export/events.csv"
