#!/usr/bin/env bash
# Partial-results experiment on a real OpenSearch cluster.
#
# This is the experiment the README's headline claim needs and the four recorded
# benchmark runs never performed. Those runs measured deadline expiry: every
# incomplete response also carried an error, because coverage and errors were
# the same signal. The shard-unavailable path was never exercised.
#
# Here a data node is genuinely stopped while the API stays up, and the
# experiment reads _shards.successful against _shards.total in the response.
# The fault is induced in the cluster, not injected in-process; the in-process
# comparison is scripts/partial-results-local.sh.
#
# Scenarios, all against the same corpus and deadline:
#
#   baseline           three data nodes, every shard answers
#   shard-unavailable  one data node stopped, its shard has no replica
#   recovered          the node is back and the cluster has re-formed
#
# Pass condition, and the measurement the paper needs:
#
#   shard-unavailable  incomplete_responses > 0 AND api_error_responses == 0
#
# Usage:
#   docker compose -f deploy/docker-compose.multinode.yaml up -d --build
#   bash scripts/partial-results-opensearch.sh [output-directory]
#   docker compose -f deploy/docker-compose.multinode.yaml down -v
set -euo pipefail

cd "$(dirname "$0")/.."
OUT_DIR="${1:-docs/experiments}"
COMPOSE_FILE="deploy/docker-compose.multinode.yaml"
BASE="${LATTICE_EXPERIMENT_URL:-http://127.0.0.1:18080}"
OS_URL="${LATTICE_EXPERIMENT_OPENSEARCH_URL:-http://127.0.0.1:19200}"
INDEX="${LATTICE_EXPERIMENT_INDEX:-lattice-documents}"
DOCS="${LATTICE_EXPERIMENT_DOCS:-50000}"
QUERIES="${LATTICE_EXPERIMENT_QUERIES:-2000}"
RUNS="${LATTICE_EXPERIMENT_RUNS:-5}"
CONCURRENCY="${LATTICE_EXPERIMENT_CONCURRENCY:-8,32,128}"
DEADLINE="${LATTICE_EXPERIMENT_DEADLINE:-200ms}"
WARMUP="${LATTICE_EXPERIMENT_WARMUP:-10s}"
# The node to stop. It must not be the elected cluster manager's only quorum
# member; with three nodes, stopping one leaves a quorum of two.
VICTIM="${LATTICE_EXPERIMENT_VICTIM:-opensearch-3}"
export GOCACHE="${LATTICE_GOCACHE:-/tmp/lattice-gocache}"

WORK="$(mktemp -d)"
COMMIT="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
mkdir -p "${OUT_DIR}"

restore_victim() {
  docker compose -f "${COMPOSE_FILE}" start "${VICTIM}" >/dev/null 2>&1 || true
}
cleanup() {
  restore_victim
  rm -rf "${WORK}"
}
trap cleanup EXIT

require_up() {
  if ! curl -fsS "${BASE}/healthz" >/dev/null 2>&1; then
    echo "LATTICE is not answering at ${BASE}." >&2
    echo "Start the cluster first: docker compose -f ${COMPOSE_FILE} up -d --build" >&2
    exit 1
  fi
}

echo "==> checking the cluster"
require_up
curl -fsS "${OS_URL}/_cluster/health?wait_for_status=green&timeout=120s" >/dev/null

echo "==> confirming the index has no replicas"
REPLICAS="$(curl -fsS "${OS_URL}/${INDEX}/_settings" | python3 -c 'import json,sys; print(next(iter(json.load(sys.stdin).values()))["settings"]["index"]["number_of_replicas"])')"
if [[ "${REPLICAS}" != "0" ]]; then
  echo "index ${INDEX} has ${REPLICAS} replica(s)." >&2
  echo "With a replica available OpenSearch routes around the stopped node and coverage stays complete," >&2
  echo "which measures the wrong thing. Recreate the index with OPENSEARCH_REPLICAS=0." >&2
  exit 1
fi

echo "==> building latticectl and latticebench"
go build -o "${WORK}/latticectl" ./cmd/latticectl
go build -o "${WORK}/latticebench" ./cmd/latticebench

echo "==> seeding ${DOCS} documents"
LATTICE_URL="${BASE}" "${WORK}/latticectl" seed -count "${DOCS}" -concurrency 32 >/dev/null
curl -fsS -X POST "${OS_URL}/${INDEX}/_refresh" >/dev/null
curl -fsS "${OS_URL}/_cluster/health?wait_for_status=green&timeout=120s" >/dev/null

run_bench() { # scenario, fault-source, note, output
  "${WORK}/latticebench" \
    -url "${BASE}" \
    -q "common=raft" \
    -q "rare=sstables" \
    -q "phrase=distributed consensus leader election" \
    -concurrency "${CONCURRENCY}" \
    -queries "${QUERIES}" \
    -runs "${RUNS}" \
    -warmup "${WARMUP}" \
    -deadline "${DEADLINE}" \
    -backend opensearch \
    -opensearch-url "${OS_URL}" \
    -opensearch-index "${INDEX}" \
    -commit "${COMMIT}" \
    -scenario "$1" \
    -fault-source "$2" \
    -note "$3" \
    -output "$4" >/dev/null
}

record_shards() { # label
  curl -fsS "${OS_URL}/${INDEX}/_search?size=0" \
    | python3 -c "import json,sys; s=json.load(sys.stdin)['_shards']; print('    $1 _shards total=%s successful=%s' % (s['total'], s['successful']))"
}

echo "==> scenario 1/3: baseline (all data nodes up)"
record_shards baseline
run_bench baseline none \
  "Three-node OpenSearch, 3 primary shards, 0 replicas, all nodes up. Reference distribution." \
  "${OUT_DIR}/opensearch-baseline.json"

echo "==> scenario 2/3: stopping ${VICTIM}"
docker compose -f "${COMPOSE_FILE}" stop "${VICTIM}" >/dev/null
# Wait for the cluster to notice, then confirm the API is still serving. A red
# cluster with a live API is exactly the state being measured.
for _ in $(seq 1 60); do
  status="$(curl -fsS "${OS_URL}/_cluster/health" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])' 2>/dev/null || echo unknown)"
  if [[ "${status}" == "red" || "${status}" == "yellow" ]]; then break; fi
  sleep 2
done
echo "    cluster status: ${status}"
require_up
record_shards shard-unavailable
run_bench shard-unavailable induced-in-cluster \
  "Data node ${VICTIM} stopped with docker compose stop. One primary shard has no copy in the cluster. The API stayed up throughout." \
  "${OUT_DIR}/opensearch-shard-unavailable.json"

echo "==> scenario 3/3: restarting ${VICTIM} and re-measuring"
restore_victim
curl -fsS "${OS_URL}/_cluster/health?wait_for_status=green&timeout=300s" >/dev/null
record_shards recovered
run_bench recovered none \
  "Data node ${VICTIM} restarted and the cluster returned to green. Confirms the baseline is reproducible after the fault." \
  "${OUT_DIR}/opensearch-recovered.json"

echo "==> verifying the availability path was actually exercised"
python3 scripts/verify_partial_results.py "${OUT_DIR}" \
  opensearch-baseline.json opensearch-shard-unavailable.json opensearch-recovered.json

echo "==> artifacts written to ${OUT_DIR}/"
