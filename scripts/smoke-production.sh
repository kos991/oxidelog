#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

api_host="192.168.0.142"
api_port="18080"
tcp_host="192.168.0.142"
tcp_port="1514"
limit="50"
wait_seconds="3"
output_root="smoke-production-output"
ingest="1"
api_token="${OXIDELOG_API_TOKEN:-}"

usage() {
  cat <<'EOF'
Usage: scripts/smoke-production.sh [options]

Options:
  --api-host HOST       API host (default: 192.168.0.142)
  --api-port PORT       API port (default: 18080)
  --tcp-host HOST       TCP ingest host (default: 192.168.0.142)
  --tcp-port PORT       TCP ingest port (default: 1514)
  --limit N             API event/archive limit (default: 50)
  --wait-seconds N      Seconds to wait after TCP ingest (default: 3)
  --output-root DIR     Artifact root directory (default: smoke-production-output)
  --api-token TOKEN     Bearer token for protected API routes (or OXIDELOG_API_TOKEN)
  --no-ingest           Skip sample TCP ingest
  -h, --help            Show this help
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --api-host) api_host="$2"; shift 2 ;;
    --api-port) api_port="$2"; shift 2 ;;
    --tcp-host) tcp_host="$2"; shift 2 ;;
    --tcp-port) tcp_port="$2"; shift 2 ;;
    --limit) limit="$2"; shift 2 ;;
    --wait-seconds) wait_seconds="$2"; shift 2 ;;
    --output-root) output_root="$2"; shift 2 ;;
    --api-token) api_token="$2"; shift 2 ;;
    --no-ingest) ingest="0"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if ! command -v curl >/dev/null 2>&1; then
  echo "curl not found in PATH" >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not found in PATH" >&2
  exit 1
fi

base_url="http://${api_host}:${api_port}"
stamp="$(date +%Y%m%d-%H%M%S)"
output_dir="${output_root}/${stamp}"
mkdir -p "$output_dir"
curl_config=""
cleanup() {
  if [ -n "$curl_config" ] && [ -f "$curl_config" ]; then
    rm -f "$curl_config"
  fi
}
trap cleanup EXIT

if [ -n "$api_token" ]; then
  curl_config="$(mktemp)"
  chmod 0600 "$curl_config"
  printf 'header = "Authorization: Bearer %s"\n' "$api_token" > "$curl_config"
fi

step() {
  echo "[smoke] $*"
}

get_json() {
  local path="$1"
  local outfile="$2"
  step "GET ${base_url}${path}"
  if [ -n "$api_token" ]; then
    curl -fsS --max-time 15 --config "$curl_config" "${base_url}${path}" -o "$outfile"
  else
    curl -fsS --max-time 15 "${base_url}${path}" -o "$outfile"
  fi
}

post_json() {
  local path="$1"
  local outfile="$2"
  step "POST ${base_url}${path}"
  if [ -n "$api_token" ]; then
    curl -fsS --max-time 15 --config "$curl_config" -X POST "${base_url}${path}" -o "$outfile"
  else
    curl -fsS --max-time 15 -X POST "${base_url}${path}" -o "$outfile"
  fi
}

json_value() {
  local file="$1"
  local expression="$2"
  python3 - "$file" "$expression" <<'PY'
import json
import sys
with open(sys.argv[1], "r", encoding="utf-8") as f:
    data = json.load(f)
value = eval(sys.argv[2], {}, {"data": data})
print(value)
PY
}

url_quote_json_value() {
  local file="$1"
  local expression="$2"
  python3 - "$file" "$expression" <<'PY'
import json
import sys
from urllib.parse import quote
with open(sys.argv[1], "r", encoding="utf-8") as f:
    data = json.load(f)
print(quote(str(eval(sys.argv[2], {}, {"data": data})), safe=""))
PY
}

write_sample_lines() {
  local outfile="$1"
  if [ -f samples/sangfor.log ]; then
    sed '/^[[:space:]]*$/d' samples/sangfor.log > "$outfile"
    return
  fi

  local now
  now="$(date -Iseconds)"
  cat > "$outfile" <<EOF
<134>1 $now smoke-production oxidelog - - - allow src=10.10.0.1 dst=10.10.0.2 action=allow
<134>1 $now smoke-production oxidelog - - - deny src=10.10.0.3 dst=10.10.0.4 action=deny
<134>1 $now smoke-production oxidelog - - - alert src=10.10.0.5 dst=10.10.0.6 action=alert
<134>1 $now smoke-production oxidelog - - - pass src=10.10.0.7 dst=10.10.0.8 action=pass
smoke-production malformed raw line
EOF
}

send_tcp_lines() {
  local sample_file="$1"
  step "TCP ingest $(wc -l < "$sample_file") lines to ${tcp_host}:${tcp_port}"
  exec 3<>"/dev/tcp/${tcp_host}/${tcp_port}"
  while IFS= read -r line; do
    printf '%s\n' "$line" >&3
  done < "$sample_file"
  exec 3>&-
}

step "artifact directory: $output_dir"

get_json "/api/health" "$output_dir/health.json"
health_status="$(json_value "$output_dir/health.json" 'data.get("status", "")')"
if [ "$health_status" != "ok" ]; then
  echo "unexpected health status: $health_status" >&2
  exit 1
fi

get_json "/api/system/status" "$output_dir/system-status.json"
status_service="$(json_value "$output_dir/system-status.json" 'data.get("service", "")')"
if [ "$status_service" != "fwlogd" ]; then
  echo "unexpected system status service: $status_service" >&2
  exit 1
fi

sample_count="0"
if [ "$ingest" = "1" ]; then
  sample_file="$output_dir/ingested-sample.log"
  write_sample_lines "$sample_file"
  sample_count="$(wc -l < "$sample_file" | tr -d '[:space:]')"
  if [ "$sample_count" -lt 1 ]; then
    echo "no sample log lines available" >&2
    exit 1
  fi
  send_tcp_lines "$sample_file"
  sleep "$wait_seconds"
else
  step "TCP ingest skipped"
fi

get_json "/api/events?limit=${limit}" "$output_dir/events.json"
event_count="$(json_value "$output_dir/events.json" 'len(data)')"
if [ "$event_count" -lt 1 ]; then
  echo "expected at least one event from /api/events" >&2
  exit 1
fi
if [ "$ingest" = "1" ] && [ "$event_count" -lt "$sample_count" ] && [ "$event_count" -lt "$limit" ]; then
  echo "expected events after ingest, got $event_count for $sample_count sample lines" >&2
  exit 1
fi

step "GET ${base_url}/api/events/export.csv?limit=${limit}"
if [ -n "$api_token" ]; then
  curl -fsS --max-time 15 --config "$curl_config" "${base_url}/api/events/export.csv?limit=${limit}" -o "$output_dir/events.csv"
else
  curl -fsS --max-time 15 "${base_url}/api/events/export.csv?limit=${limit}" -o "$output_dir/events.csv"
fi
if ! grep -Eq "event_id|raw|parse_status" "$output_dir/events.csv"; then
  echo "CSV export did not include expected event columns" >&2
  exit 1
fi

post_json "/api/archive/parquet?limit=${limit}" "$output_dir/parquet-created.json"
parquet_path="$(json_value "$output_dir/parquet-created.json" 'data.get("path", "")')"
case "$parquet_path" in
  *.parquet) ;;
  *) echo "parquet archive path was not a .parquet file: $parquet_path" >&2; exit 1 ;;
esac

get_json "/api/archive/files" "$output_dir/parquet-files.json"
parquet_count="$(json_value "$output_dir/parquet-files.json" 'len(data)')"
if [ "$parquet_count" -lt 1 ]; then
  echo "expected at least one parquet archive file" >&2
  exit 1
fi

post_json "/api/archive/frozen?limit=${limit}" "$output_dir/frozen-created.json"
frozen_path="$(json_value "$output_dir/frozen-created.json" 'data.get("path", "")')"
case "$frozen_path" in
  *.raw.zst) ;;
  *) echo "frozen archive path was not a .raw.zst file: $frozen_path" >&2; exit 1 ;;
esac

get_json "/api/archive/frozen" "$output_dir/frozen-files.json"
frozen_count="$(json_value "$output_dir/frozen-files.json" 'len(data)')"
if [ "$frozen_count" -lt 1 ]; then
  echo "expected at least one frozen archive file" >&2
  exit 1
fi

restore_path="$(url_quote_json_value "$output_dir/frozen-created.json" 'data.get("path", "")')"
get_json "/api/archive/frozen/restore?path=${restore_path}" "$output_dir/frozen-restored.json"
restored_count="$(json_value "$output_dir/frozen-restored.json" 'len(data)')"
if [ "$restored_count" -lt 1 ]; then
  echo "expected restored frozen archive lines" >&2
  exit 1
fi

echo "OxideLog production smoke passed"
echo "API: $base_url"
if [ -n "$api_token" ]; then
  echo "Auth header: enabled"
else
  echo "Auth header: not set"
fi
if [ "$ingest" = "1" ]; then
  echo "TCP ingest: ${tcp_host}:${tcp_port} (${sample_count} lines)"
else
  echo "TCP ingest: skipped"
fi
echo "Events checked: $event_count"
echo "CSV: $output_dir/events.csv"
echo "Parquet archives listed: $parquet_count"
echo "Frozen archives listed: $frozen_count"
echo "Restored frozen lines: $restored_count"
echo "Artifacts: $output_dir"
