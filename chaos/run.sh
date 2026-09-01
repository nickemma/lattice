#!/usr/bin/env bash
set -euo pipefail

namespace=${1:-lattice}
local_port=${LATTICE_CHAOS_PORT:-18080}
api_key_args=()
if [[ -n "${LATTICE_API_KEY:-}" ]]; then
  api_key_args=(-H "X-API-Key: ${LATTICE_API_KEY}")
fi

cleanup() {
  if [[ -n "${port_forward_pid:-}" ]]; then
    kill "${port_forward_pid}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

kubectl -n "${namespace}" port-forward svc/lattice-query "${local_port}:8080" >/tmp/lattice-chaos-port-forward.log 2>&1 &
port_forward_pid=$!
until curl -fsS "${api_key_args[@]}" "http://127.0.0.1:${local_port}/readyz" >/dev/null; do
  sleep 1
done

echo "baseline search"
curl -fsS "${api_key_args[@]}" "http://127.0.0.1:${local_port}/v1/search?q=consensus&deadline=2s" >/tmp/lattice-chaos-baseline.json

echo "query pod restart"
kubectl -n "${namespace}" delete pod -l app=lattice-query --wait=false
until curl -fsS "${api_key_args[@]}" "http://127.0.0.1:${local_port}/readyz" >/dev/null; do
  sleep 1
done
curl -fsS "${api_key_args[@]}" "http://127.0.0.1:${local_port}/v1/search?q=consensus&deadline=2s" >/tmp/lattice-chaos-after-query-restart.json

id="chaos-$(date -u +%Y%m%dT%H%M%SZ)"
echo "indexer restart and replay check for ${id}"
kubectl -n "${namespace}" delete pod -l app=lattice-indexer --wait=false
curl -fsS "${api_key_args[@]}" -X POST "http://127.0.0.1:${local_port}/v1/documents" \
  -H 'content-type: application/json' \
  -d "{\"id\":\"${id}\",\"title\":\"chaos replay\",\"body\":\"Kafka offset replay after an indexer restart\"}" >/tmp/lattice-chaos-publish.json

deadline=$((SECONDS + 120))
while (( SECONDS < deadline )); do
  if curl -fsS "${api_key_args[@]}" "http://127.0.0.1:${local_port}/v1/search?q=${id}&deadline=2s" | grep -q "${id}"; then
    echo "chaos checks passed"
    exit 0
  fi
  sleep 2
done

echo "document did not become queryable after indexer restart" >&2
exit 1
