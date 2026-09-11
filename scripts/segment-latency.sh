#!/usr/bin/env bash
# Segment count against tail latency.
#
# The four earlier benchmark runs contain a real finding that nobody designed:
# across the 1M-document corpus, p99 moved 191ms -> 95ms -> 49ms as segments
# were merged, while p50 barely moved (16.7 -> 24.7 -> 18.0). Segment count was
# dominating tail latency and almost nothing else. That was an artifact of three
# incidental runs, not an experiment: two points, no repeats, no attribution to
# a branch, and no statement of what the merge cost.
#
# This script turns it into a designed experiment. It force-merges the index
# down through a series of target segment counts, and at each target it records:
#
#   - the actual segment count from the _segments API, not the requested one
#   - p50/p95/p99 over repeated runs, with an IQR
#   - the BM25 and kNN branch timings separately, so the sensitivity can be
#     attributed to one branch rather than to "the query"
#   - the wall-clock cost of the merge that produced that segment count
#
# The cost column is not optional. "You can cut p99 4x" is marketing; "you can
# cut p99 4x for N minutes of merge that also doubles disk usage while it runs"
# is a result.
#
# Force-merging is expensive and one-way. Do not point this at anything you
# care about.
#
# Usage:
#   docker compose -f deploy/docker-compose.yaml up -d
#   # load a declared corpus first; this script does not seed
#   bash scripts/segment-latency.sh [output-directory]
set -euo pipefail

cd "$(dirname "$0")/.."
OUT_DIR="${1:-docs/experiments}"
BASE="${LATTICE_EXPERIMENT_URL:-http://127.0.0.1:8080}"
OS_URL="${LATTICE_EXPERIMENT_OPENSEARCH_URL:-http://127.0.0.1:19200}"
INDEX="${LATTICE_EXPERIMENT_INDEX:-lattice-documents}"
QUERIES="${LATTICE_EXPERIMENT_QUERIES:-2000}"
RUNS="${LATTICE_EXPERIMENT_RUNS:-5}"
CONCURRENCY="${LATTICE_EXPERIMENT_CONCURRENCY:-32}"
DEADLINE="${LATTICE_EXPERIMENT_DEADLINE:-200ms}"
WARMUP="${LATTICE_EXPERIMENT_WARMUP:-10s}"
# Descending merge targets. Each is a point on the curve; four or more points
# are needed before "p99 against segment count" is a plot rather than an anecdote.
TARGETS="${LATTICE_EXPERIMENT_SEGMENT_TARGETS:-0 16 8 4 2 1}"
export GOCACHE="${LATTICE_GOCACHE:-/tmp/lattice-gocache}"

WORK="$(mktemp -d)"
COMMIT="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
mkdir -p "${OUT_DIR}"
trap 'rm -rf "${WORK}"' EXIT

if ! curl -fsS "${BASE}/healthz" >/dev/null 2>&1; then
  echo "LATTICE is not answering at ${BASE}." >&2
  exit 1
fi

DOCUMENTS="$(curl -fsS "${BASE}/readyz" | python3 -c 'import json,sys; print(json.load(sys.stdin)["dependencies"]["opensearch"]["documents"])')"
if [[ "${DOCUMENTS}" -lt 1 ]]; then
  echo "The index is empty. Load a declared corpus before measuring." >&2
  exit 1
fi
echo "==> corpus: ${DOCUMENTS} documents in ${INDEX}"

go build -o "${WORK}/latticebench" ./cmd/latticebench

segments() {
  curl -fsS "${OS_URL}/${INDEX}/_segments" | python3 -c '
import json, sys
data = json.load(sys.stdin)["indices"]
total = 0
for index in data.values():
    for shard in index["shards"].values():
        for copy in shard:
            if copy["routing"]["primary"]:
                total += copy["num_search_segments"]
print(total)'
}

INDEX_FILE="${OUT_DIR}/segment-latency-index.json"
echo "[]" > "${INDEX_FILE}"

for target in ${TARGETS}; do
  merge_seconds=0
  if [[ "${target}" == "0" ]]; then
    echo "==> point: as-is, no merge"
    label="as-is"
  else
    echo "==> point: force-merge to max_num_segments=${target}"
    label="forcemerge-${target}"
    merge_started="$(date +%s.%N)"
    curl -fsS -X POST "${OS_URL}/${INDEX}/_forcemerge?max_num_segments=${target}" >/dev/null
    merge_seconds="$(python3 -c "import sys; print(round(float(sys.argv[1]) - float(sys.argv[2]), 3))" "$(date +%s.%N)" "${merge_started}")"
    curl -fsS -X POST "${OS_URL}/${INDEX}/_refresh" >/dev/null
    curl -fsS "${OS_URL}/_cluster/health?wait_for_status=yellow&timeout=300s" >/dev/null
  fi

  actual="$(segments)"
  echo "    segments now: ${actual} (merge took ${merge_seconds}s)"

  output="${OUT_DIR}/segment-latency-${label}.json"
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
    -scenario "segments-${actual}" \
    -fault-source none \
    -note "Segment-count point: ${actual} primary segments over ${DOCUMENTS} documents. The merge that produced this point took ${merge_seconds}s." \
    -output "${output}" >/dev/null

  python3 - "${INDEX_FILE}" "${output}" "${actual}" "${merge_seconds}" <<'PY'
import json, sys

index_file, report_file, segments, merge_seconds = sys.argv[1:5]
with open(report_file) as handle:
    report = json.load(handle)
with open(index_file) as handle:
    points = json.load(handle)

for config in report["configurations"]:
    aggregate = config["aggregate"]
    points.append({
        "segments": int(segments),
        "merge_seconds": float(merge_seconds),
        "query_class": config["query_class"],
        "concurrency": config["concurrency"],
        "runs": len(config["runs"]),
        "p50_ms": aggregate["p50_ms"]["median"],
        "p99_ms": aggregate["p99_ms"]["median"],
        "p99_iqr_ms": aggregate["p99_ms"]["iqr"],
        "bm25_p99_ms": aggregate["bm25_p99_ms"]["median"],
        "vector_p99_ms": aggregate["vector_p99_ms"]["median"],
        "report": report_file,
    })

with open(index_file, "w") as handle:
    json.dump(points, handle, indent=2)
    handle.write("\n")
PY
done

echo
echo "==> curve"
python3 - "${INDEX_FILE}" <<'PY'
import json, sys

with open(sys.argv[1]) as handle:
    points = json.load(handle)

print(f"{'segments':>9} {'class':<8} {'p50':>8} {'p99':>8} {'p99 IQR':>8} {'bm25 p99':>9} {'knn p99':>8} {'merge s':>9}")
for point in sorted(points, key=lambda p: (-p["segments"], p["query_class"])):
    print(f"{point['segments']:>9} {point['query_class']:<8} {point['p50_ms']:>8.2f} {point['p99_ms']:>8.2f} "
          f"{point['p99_iqr_ms']:>8.2f} {point['bm25_p99_ms']:>9.2f} {point['vector_p99_ms']:>8.2f} {point['merge_seconds']:>9.1f}")

distinct = {point["segments"] for point in points}
if len(distinct) < 4:
    print(f"\nWARNING: only {len(distinct)} distinct segment counts were reached.")
    print("A p99-against-segments plot needs at least four points before it is a curve.")
PY

echo "==> artifacts written to ${OUT_DIR}/"
