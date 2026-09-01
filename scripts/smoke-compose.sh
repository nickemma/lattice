#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
compose_file=${LATTICE_COMPOSE_FILE:-"$repo_dir/deploy/docker-compose.yaml"}
base_url=${LATTICE_E2E_URL:-http://localhost:8080}
indexer_metrics_url=${LATTICE_INDEXER_METRICS_URL:-http://localhost:19091}

wait_for_ready() {
  for _ in $(seq 1 60); do
    if curl -fsS "$base_url/readyz" >/tmp/lattice-ready.json; then
      return 0
    fi
    sleep 1
  done
  cat /tmp/lattice-ready.json 2>/dev/null || true
  return 1
}

poll_for_id() {
  local query=$1
  local id=$2
  local deadline=${3:-2s}
  local body=''
  for _ in $(seq 1 60); do
    body=$(curl -fsS -G "$base_url/v1/search" \
      --data-urlencode "q=$query" \
      --data-urlencode 'size=20' \
      --data-urlencode "deadline=$deadline")
    if grep -q "\"id\":\"$id\"" <<<"$body"; then
      printf '%s\n' "$body"
      return 0
    fi
    sleep 1
  done
  printf '%s\n' "$body"
  return 1
}

echo 'waiting for LATTICE readiness'
wait_for_ready
echo 'ensuring the Compose indexer is running'
docker compose -f "$compose_file" up -d lattice-indexer >/dev/null
for _ in $(seq 1 60); do
  if curl -fsS "$indexer_metrics_url/metrics" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS "$indexer_metrics_url/metrics" >/dev/null
curl -fsS "$base_url/docs" | grep -q 'Swagger UI'
curl -fsS "$base_url/playground" | grep -q 'LATTICE Playground'

document_id="compose-smoke-$(date -u +%s)"
echo "publishing $document_id"
curl -fsS -X POST "$base_url/v1/documents" \
  -H 'content-type: application/json' \
  -d "{\"id\":\"$document_id\",\"title\":\"Compose smoke $document_id\",\"body\":\"Kafka OpenSearch end to end verification\",\"tags\":[\"compose\",\"smoke\"]}" \
  | grep -q '"accepted":true'
poll_for_id "$document_id" "$document_id"

echo 'testing Kafka replay across an indexer restart'
docker compose -f "$compose_file" stop lattice-indexer >/dev/null
replay_id="compose-replay-$(date -u +%s)"
curl -fsS -X POST "$base_url/v1/documents" \
  -H 'content-type: application/json' \
  -d "{\"id\":\"$replay_id\",\"title\":\"Replay smoke $replay_id\",\"body\":\"Buffered Kafka message after consumer restart\",\"tags\":[\"replay\"]}" \
  | grep -q '"accepted":true'
docker compose -f "$compose_file" up -d lattice-indexer >/dev/null
poll_for_id "$replay_id" "$replay_id"

echo 'testing alias-based reindex and immediate read visibility'
curl -fsS -X POST "$base_url/v1/admin/reindex" | grep -q 'lattice-documents-read'
poll_for_id "$replay_id" "$replay_id"

echo 'testing native OpenSearch snapshot and restore'
source_index=$(curl -fsS 'http://127.0.0.1:19200/_cat/aliases?h=alias,index' \
  | awk '$1 == "lattice-documents-read" {print $2}')
test -n "$source_index"
snapshot_name="compose-smoke-$(date -u +%s)"
curl -fsS -X POST "$base_url/v1/admin/snapshot" \
  -H 'content-type: application/json' \
  -d "{\"repository\":\"lattice-local\",\"snapshot\":\"$snapshot_name\"}" \
  | grep -q '"repository":"lattice-local"'
restore_target="lattice-restore-smoke-$(date -u +%s)"
restore_started=$(date +%s%3N)
curl -fsS -X POST "$base_url/v1/admin/restore" \
  -H 'content-type: application/json' \
  -d "{\"repository\":\"lattice-local\",\"snapshot\":\"$snapshot_name\",\"source_index\":\"$source_index\",\"target_index\":\"$restore_target\"}" \
  | grep -q '"repository":"lattice-local"'
restore_elapsed=$(( $(date +%s%3N) - restore_started ))
echo "snapshot restore completed in ${restore_elapsed}ms"
poll_for_id "$replay_id" "$replay_id" 10s

curl -fsS "$base_url/metrics" | grep -q 'lattice_query_requests_total'
curl -fsS "$indexer_metrics_url/metrics" | grep -q 'lattice_ingest_lag_seconds'
curl -fsS http://127.0.0.1:19200/lattice-documents-read/_count | grep -q '"count"'
echo 'compose smoke passed'
