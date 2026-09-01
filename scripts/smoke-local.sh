#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

address=${LATTICE_SMOKE_ADDR:-127.0.0.1:18080}
base_url=${LATTICE_SMOKE_URL:-http://$address}
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/lattice-smoke.XXXXXX")
server_pid=""

cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT INT TERM

GOCACHE=${GOCACHE:-/tmp/lattice-gocache} \
LATTICE_ADDR="$address" \
LATTICE_DATA_DIR="$work_dir/data" \
go run ./cmd >"$work_dir/server.log" 2>&1 &
server_pid=$!

ready=0
for _ in $(seq 1 300); do
  if curl -fsS "$base_url/readyz" >/dev/null; then
    ready=1
    break
  fi
  sleep 0.2
done
if [[ "$ready" -ne 1 ]]; then
  cat "$work_dir/server.log"
  exit 1
fi

curl -fsS "$base_url/docs" | grep -q 'LATTICE Swagger UI'
curl -fsS "$base_url/playground" | grep -q 'LATTICE Playground'
curl -fsS -X POST "$base_url/v1/documents" \
  -H 'content-type: application/json' \
  -d '{"id":"smoke-1","title":"Raft leader","body":"consensus and storage","tags":["smoke"]}' \
  | grep -q 'smoke-1'
curl -fsS "$base_url/v1/search?q=consensus&size=1" | grep -q 'smoke-1'
curl -fsS -X POST "$base_url/v1/admin/snapshot" \
  -H 'content-type: application/json' \
  -d '{"path":"'"$work_dir"'/snapshot.json"}' >/dev/null
curl -fsS -X POST "$base_url/v1/debug/shards/0" \
  -H 'content-type: application/json' -d '{"available":false}' >/dev/null
curl -fsS "$base_url/v1/search?q=consensus&deadline=150ms" | grep -q '"complete":false'
curl -fsS -X POST "$base_url/v1/debug/shards/0" \
  -H 'content-type: application/json' -d '{"available":true}' >/dev/null
curl -fsS -X POST "$base_url/v1/admin/restore" \
  -H 'content-type: application/json' \
  -d '{"path":"'"$work_dir"'/snapshot.json"}' | grep -q '"documents":1'

echo "local LATTICE smoke test passed"
