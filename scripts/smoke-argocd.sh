#!/usr/bin/env bash
set -Eeuo pipefail

# Reconciles the current worktree through Argo CD in a disposable kind cluster.
# The worktree is copied into a temporary Git repository so this check does not
# require a pushed commit or mutate the configured origin repository.

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CLUSTER_NAME=${LATTICE_ARGOCD_CLUSTER:-lattice-argocd-e2e}
ARGOCD_VERSION=${LATTICE_ARGOCD_VERSION:-v3.3.6}
ARGOCD_GIT_PORT=${LATTICE_ARGOCD_GIT_PORT:-19418}
ARGOCD_APP=${LATTICE_ARGOCD_APP:-lattice}
LATTICE_IMAGE=${LATTICE_IMAGE:-lattice:dev}
TMP_DIR=$(mktemp -d "/tmp/${CLUSTER_NAME}.XXXXXX")
GIT_PID=""
PORT_FORWARD_PID=""

cleanup() {
  if [[ -n "$GIT_PID" ]]; then
    kill "$GIT_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "$PORT_FORWARD_PID" ]]; then
    kill "$PORT_FORWARD_PID" >/dev/null 2>&1 || true
  fi
  kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

for command_name in docker kind kubectl helm terraform git rsync curl; do
  need "$command_name"
done

if [[ "${LATTICE_BUILD_IMAGE:-0}" == "1" ]] || ! docker image inspect "$LATTICE_IMAGE" >/dev/null 2>&1; then
  echo "building current worktree image: $LATTICE_IMAGE"
  docker build -t "$LATTICE_IMAGE" "$ROOT_DIR"
else
  echo "using existing image: $LATTICE_IMAGE"
fi

echo "creating disposable kind cluster: $CLUSTER_NAME"
kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
kind create cluster --name "$CLUSTER_NAME" --wait 120s
control_plane=$(kind get nodes --name "$CLUSTER_NAME" | head -n 1)

load_image() {
  local image="$1"
  if kind load docker-image "$image" --name "$CLUSTER_NAME"; then
    return 0
  fi
  echo "kind load rejected $image; importing its amd64 image directly"
  docker save --platform=linux/amd64 "$image" | \
    docker exec --privileged -i "$control_plane" \
      ctr --namespace=k8s.io images import --platform linux/amd64 --digests --snapshotter=overlayfs -
}

load_image "$LATTICE_IMAGE"
if ! docker image inspect busybox:latest >/dev/null 2>&1 && docker image inspect busybox:1.36 >/dev/null 2>&1; then
  docker tag busybox:1.36 busybox:latest
fi
for dependency_image in \
  opensearchproject/opensearch:2.17.1 \
  docker.redpanda.com/redpandadata/redpanda:v24.3.8; do
  if docker image inspect "$dependency_image" >/dev/null 2>&1; then
    echo "loading cached dependency image: $dependency_image"
    if ! load_image "$dependency_image"; then
      echo "warning: could not preload $dependency_image; Kubernetes will pull it" >&2
    fi
  fi
done
docker exec --privileged "$control_plane" sysctl -w fs.aio-max-nr=1048576 >/dev/null

echo "creating temporary Git source"
snapshot_dir="$TMP_DIR/snapshot"
bare_repo="$TMP_DIR/lattice.git"
git init "$snapshot_dir" >/dev/null
rsync -a --exclude=.git --exclude='.env' --exclude='.env.*' \
  --exclude='deploy/terraform/.terraform' --exclude='*.tfstate' \
  --exclude='*.tfstate.*' "$ROOT_DIR/" "$snapshot_dir/"
git -C "$snapshot_dir" add -A
git -C "$snapshot_dir" -c user.name=LATTICE -c user.email=lattice@example.invalid commit -m snapshot >/dev/null
git clone --bare "$snapshot_dir" "$bare_repo" >/dev/null
git -C "$bare_repo" config daemon.export ok
git daemon --reuseaddr --base-path="$TMP_DIR" --export-all --port="$ARGOCD_GIT_PORT" "$TMP_DIR" >"$TMP_DIR/git-daemon.log" 2>&1 &
GIT_PID=$!

git_gateway=""
for candidate in $(docker network inspect kind -f '{{range .IPAM.Config}}{{.Gateway}} {{end}}'); do
  if [[ "$candidate" == *.* ]]; then
    git_gateway="$candidate"
    break
  fi
done
if [[ -z "$git_gateway" ]]; then
  echo "could not find an IPv4 gateway for the kind Docker network" >&2
  exit 1
fi

echo "installing cert-manager and OpenSearch Operator"
helm repo add jetstack https://charts.jetstack.io >/dev/null 2>&1 || true
helm repo add opensearch-operator https://opensearch-project.github.io/opensearch-k8s-operator/ >/dev/null 2>&1 || true
helm repo update >/dev/null
helm upgrade --install cert-manager jetstack/cert-manager \
  --version v1.16.3 --namespace cert-manager --create-namespace \
  --set crds.enabled=true --wait --timeout 5m >/dev/null
helm upgrade --install opensearch-operator opensearch-operator/opensearch-operator \
  --version 2.8.4 --namespace opensearch-operator-system --create-namespace \
  --wait --timeout 5m >/dev/null

echo "installing the Prometheus CRDs consumed by the GitOps application"
prometheus_crd_base=https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/v0.77.1/example/prometheus-operator-crd
kubectl apply -f "$prometheus_crd_base/monitoring.coreos.com_servicemonitors.yaml" >/dev/null
kubectl apply -f "$prometheus_crd_base/monitoring.coreos.com_prometheusrules.yaml" >/dev/null

echo "applying Terraform-owned namespace and OpenSearch cluster"
terraform -chdir="$ROOT_DIR/deploy/terraform" init -input=false >/dev/null
test_password="${LATTICE_TEST_OPENSEARCH_PASSWORD:-lattice-e2e-$(date +%s)-temporary-password}"
terraform -chdir="$ROOT_DIR/deploy/terraform" apply -input=false -auto-approve \
  -state="$TMP_DIR/terraform.tfstate" \
  -var="opensearch_admin_password=$test_password" \
  -var='opensearch_replicas=1' >/dev/null

echo "installing Argo CD $ARGOCD_VERSION"
kubectl create namespace argocd >/dev/null 2>&1 || true
kubectl apply --server-side --force-conflicts -n argocd \
  -f "https://raw.githubusercontent.com/argoproj/argo-cd/${ARGOCD_VERSION}/manifests/install.yaml" >/dev/null
kubectl -n argocd rollout status deployment/argocd-repo-server --timeout=10m
kubectl -n argocd rollout status statefulset/argocd-application-controller --timeout=10m

echo "submitting Application pointing at the temporary Git source"
sed "s#https://github.com/nickemma/lattice.git#git://${git_gateway}:${ARGOCD_GIT_PORT}/lattice.git#" \
  "$ROOT_DIR/deploy/argocd/application.yaml" | kubectl apply -f - >/dev/null

echo "waiting for Argo to report Synced"
sync_status=""
for attempt in $(seq 1 120); do
  sync_status=$(kubectl -n argocd get application "$ARGOCD_APP" -o jsonpath='{.status.sync.status}' 2>/dev/null || true)
  if [[ "$sync_status" == "Synced" ]]; then
    break
  fi
  sleep 5
done
if [[ "$sync_status" != "Synced" ]]; then
  kubectl -n argocd get application "$ARGOCD_APP" -o yaml >&2 || true
  kubectl -n argocd logs deployment/argocd-repo-server --tail=80 >&2 || true
  exit 1
fi

# OpenSearch 2.17 can take more than the operator's default five-minute
# startup-probe window on a resource-constrained WSL/kind host. Extend only
# this disposable verification cluster's probe so the smoke test measures
# readiness and the publish path instead of host JVM startup speed.
if kubectl -n lattice get statefulset/lattice-search-masters >/dev/null 2>&1; then
  kubectl -n lattice patch statefulset lattice-search-masters --type=json \
    -p='[{"op":"replace","path":"/spec/template/spec/containers/0/startupProbe/failureThreshold","value":30}]' \
    >/dev/null
fi

wait_ready() {
  local resource="$1"
  local label="$2"
  if kubectl -n lattice wait --for=condition=available "$resource" --timeout=10m; then
    return 0
  fi
  echo "resource did not become available: $resource" >&2
  kubectl -n lattice get pods -o wide >&2 || true
  kubectl -n lattice describe "$resource" >&2 || true
  kubectl -n lattice logs -l app="$label" --all-containers --tail=120 >&2 || true
  return 1
}

if ! kubectl -n lattice wait --for=condition=ready pod/lattice-search-masters-0 --timeout=10m; then
  kubectl -n lattice describe pod/lattice-search-masters-0 >&2 || true
  kubectl -n lattice logs pod/lattice-search-masters-0 --all-containers --tail=160 >&2 || true
  exit 1
fi
wait_ready deployment/lattice-kafka lattice-kafka
wait_ready deployment/lattice-redis lattice-redis
wait_ready deployment/lattice-indexer lattice-indexer
wait_ready deployment/lattice-query lattice-query

port_forward_log="$TMP_DIR/port-forward.log"
kubectl -n lattice port-forward svc/lattice-query 18080:8080 >"$port_forward_log" 2>&1 &
PORT_FORWARD_PID=$!
for attempt in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:18080/readyz >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

curl -fsS -X POST http://127.0.0.1:18080/v1/documents \
  -H 'content-type: application/json' \
  -H 'x-api-key: kind-e2e-key' \
  -d '{"id":"argocd-e2e-001","title":"Argo reconciliation","body":"LATTICE was reconciled by Argo CD."}' >/dev/null

found="false"
for attempt in $(seq 1 60); do
  response=$(curl -fsS 'http://127.0.0.1:18080/v1/search?q=Argo%20reconciliation&deadline=500ms' \
    -H 'x-api-key: kind-e2e-key')
  if grep -q 'argocd-e2e-001' <<<"$response"; then
    found="true"
    if ! grep -q '"complete":true' <<<"$response"; then
      echo "search found the document but coverage was incomplete: $response" >&2
      exit 1
    fi
    break
  fi
  sleep 2
done
if [[ "$found" != "true" ]]; then
  echo "Argo-synced application did not make the published document searchable" >&2
  exit 1
fi

echo "argocd_sync=$sync_status"
echo "argocd_application=$(kubectl -n argocd get application "$ARGOCD_APP" -o jsonpath='{.status.health.status}')"
echo "publish_to_kafka_indexer_search=passed"
