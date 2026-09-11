#!/usr/bin/env bash
# Partial-results experiment on the in-process backend.
#
# This is the controlled comparison for the OpenSearch experiment in
# scripts/partial-results-opensearch.sh. It answers one question: when part of
# the index stops answering, does the system return usable partial results and
# say so, and can it distinguish that from a deadline expiring?
#
# Three scenarios run against the same corpus and the same deadline:
#
#   baseline           all three shards answer
#   shard-unavailable  one shard is removed from the fan-out (no error occurs)
#   deadline           one shard is stalled past the deadline (an error occurs)
#
# The faults are injected in-process through /v1/debug/shards. They are
# experiment controls, not observed node failures, and every artifact this
# script writes records that under environment.fault_source.
#
# The expected result, and the thing that could not be measured before coverage
# and errors were separated:
#
#   shard-unavailable  incomplete_responses > 0 AND api_error_responses == 0
#   deadline           incomplete_responses > 0 AND api_error_responses > 0
#
# Usage: bash scripts/partial-results-local.sh [output-directory]
set -euo pipefail

cd "$(dirname "$0")/.."
OUT_DIR="${1:-docs/experiments}"
PORT="${LATTICE_EXPERIMENT_PORT:-18080}"
BASE="http://127.0.0.1:${PORT}"
DOCS="${LATTICE_EXPERIMENT_DOCS:-20000}"
QUERIES="${LATTICE_EXPERIMENT_QUERIES:-2000}"
RUNS="${LATTICE_EXPERIMENT_RUNS:-5}"
CONCURRENCY="${LATTICE_EXPERIMENT_CONCURRENCY:-8,32,128}"
DEADLINE="${LATTICE_EXPERIMENT_DEADLINE:-200ms}"
STALL_MS="${LATTICE_EXPERIMENT_STALL_MS:-5000}"
WARMUP="${LATTICE_EXPERIMENT_WARMUP:-3s}"
export GOCACHE="${LATTICE_GOCACHE:-/tmp/lattice-gocache}"

WORK="$(mktemp -d)"
DATA_DIR="${WORK}/data"
COMMIT="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
mkdir -p "${OUT_DIR}" "${DATA_DIR}"

cleanup() {
  if [[ -n "${API_PID:-}" ]] && kill -0 "${API_PID}" 2>/dev/null; then
    kill "${API_PID}" 2>/dev/null || true
    wait "${API_PID}" 2>/dev/null || true
  fi
  rm -rf "${WORK}"
}
trap cleanup EXIT

echo "==> building"
go build -o "${WORK}/lattice" ./cmd
go build -o "${WORK}/latticectl" ./cmd/latticectl
go build -o "${WORK}/latticebench" ./cmd/latticebench

echo "==> starting API on ${BASE} (in-process backend, 3 shards)"
LATTICE_ADDR=":${PORT}" LATTICE_DATA_DIR="${DATA_DIR}" "${WORK}/lattice" >"${WORK}/lattice.log" 2>&1 &
API_PID=$!
for _ in $(seq 1 60); do
  if curl -fsS "${BASE}/healthz" >/dev/null 2>&1; then break; fi
  sleep 0.5
done
curl -fsS "${BASE}/healthz" >/dev/null

echo "==> seeding ${DOCS} documents"
LATTICE_URL="${BASE}" "${WORK}/latticectl" seed -count "${DOCS}" -concurrency 32 >/dev/null

shard_state() { # shard, json body
  curl -fsS -X POST "${BASE}/v1/debug/shards/$1" \
    -H 'content-type: application/json' -d "$2" >/dev/null
}

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
    -backend in-memory \
    -commit "${COMMIT}" \
    -scenario "$1" \
    -fault-source "$2" \
    -note "$3" \
    -output "$4" >/dev/null
}

echo "==> scenario 1/3: baseline (all shards answering)"
shard_state 1 '{"available":true,"delay_ms":0}'
run_bench baseline none \
  "In-process backend, 3 shards, all answering. Reference distribution for the two fault scenarios." \
  "${OUT_DIR}/local-baseline.json"

echo "==> scenario 2/3: shard-unavailable (availability-driven incompleteness)"
shard_state 1 '{"available":false,"delay_ms":0}'
run_bench shard-unavailable injected-in-process \
  "Shard 1 removed from the fan-out via /v1/debug/shards. Nothing errors; coverage must fall on its own." \
  "${OUT_DIR}/local-shard-unavailable.json"

echo "==> scenario 3/3: deadline (deadline-driven incompleteness)"
shard_state 1 "{\"available\":true,\"delay_ms\":${STALL_MS}}"
run_bench deadline injected-in-process \
  "Shard 1 stalled ${STALL_MS}ms, past the ${DEADLINE} budget. Coverage falls and the response carries a deadline error." \
  "${OUT_DIR}/local-deadline.json"

shard_state 1 '{"available":true,"delay_ms":0}'

echo "==> verifying the availability path was actually exercised"
python3 scripts/verify_partial_results.py "${OUT_DIR}" \
  local-baseline.json local-shard-unavailable.json

echo "==> verifying the deadline path does not look like it"
python3 - "${OUT_DIR}" <<'DEADLINE_CHECK'
import json, os, sys

# The availability check above proves an unavailable shard reports no error.
# This proves the other failure class is not the same event wearing the same
# label: a stalled shard must be incomplete AND degraded AND reported as a
# deadline. If both scenarios looked alike, coverage would be back to carrying
# one signal instead of two.
out = sys.argv[1]
with open(os.path.join(out, "local-deadline.json")) as handle:
    report = json.load(handle)

incomplete = degraded = errors = 0
reasons = {}
for config in report["configurations"]:
    for run in config["runs"]:
        incomplete += run["incomplete_responses"]
        degraded += run["degraded_responses"]
        errors += run["api_error_responses"]
        for reason, count in (run.get("coverage_reasons") or {}).items():
            reasons[reason] = reasons.get(reason, 0) + count

print(f"deadline scenario: incomplete={incomplete} degraded={degraded} errors={errors} reasons={reasons}")

failures = []
if incomplete == 0:
    failures.append("a shard stalled past the deadline produced no incomplete responses")
if degraded == 0 or errors == 0:
    failures.append("a lost deadline is an error and must be reported as one")
if reasons.get("deadline", 0) == 0:
    failures.append(f"expected a deadline reason, got {reasons}")

if failures:
    print("FAILED:")
    for failure in failures:
        print("  - " + failure)
    sys.exit(1)
print("OK: deadline-driven incompleteness is degraded and labelled the deadline reason.")
DEADLINE_CHECK

echo "==> artifacts written to ${OUT_DIR}/"
