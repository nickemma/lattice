# LATTICE end-to-end walkthrough

This is the executable path through the project. Use Swagger at `/docs`, the browser playground at `/playground`, or the equivalent `curl` commands.

## What LATTICE is, in plain language

LATTICE is a search system for a large pile of documents. A producer sends in
documents, a stream carries them to an indexer, and a query service finds
documents by both exact words and meaning. The answer combines those two kinds
of search so a user can find a document even when their wording is different
from the wording in the document.

The project solves the operational problems that appear after a basic search
demo: data must survive a crash, messages may be delivered twice, one part of
the system may be unavailable, indexes need rebuilding without taking search
down, and backups must be restorable. LATTICE reports partial coverage openly
(`complete: false`) instead of silently pretending that unavailable shards
answered. It also exposes the timings and metrics an engineer needs to tell
whether a problem is ingestion, search, caching, or a dependency.

The repository contains both teaching implementations of a WAL, LSM/MVCC
store, replication, Raft, quorum, and sharding, and the production-shaped
search path built around Redpanda and OpenSearch. The teaching components make
the failure and consistency decisions inspectable; OpenSearch is the final
search index used by the Compose and Kubernetes paths.

The complete API reference, including every route, request, response, error,
authentication rule, and curl example, is in [`api.md`](../api.md).

## The shortest successful test

If you only need to understand the product surface, this is the smallest
end-to-end exercise:

```bash
make dev
curl -fsS -X POST http://localhost:8080/v1/documents \
  -H 'content-type: application/json' \
  -d '{"id":"walkthrough-001","title":"Durable search","body":"LATTICE combines keyword and semantic retrieval."}'
curl -fsS -G http://localhost:8080/v1/search \
  --data-urlencode 'q=semantic retrieval'
```

The first command writes through the local ingest/index path. The second
should return the document with `coverage.complete:true`. Once that works,
follow the numbered sections below to exercise the real stream, failure, and
recovery boundaries.

## Configuration and secrets

There is deliberately no checked-in `.env` file and no real credential file.
The default Compose path needs no secrets: it runs local development instances
of Redpanda, OpenSearch with its security plugin disabled, and Redis, with
configuration declared directly in `deploy/docker-compose.yaml`.

For convenience, [`.env.example`](../.env.example) lists optional local
variables, but it is only a template. Copy it to `.env` only for local tooling;
`.env` and other environment files are ignored by Git.

Kubernetes credentials are different: Terraform creates the OpenSearch
credential Secret and optional API-key Secret from input variables. Supply
`TF_VAR_opensearch_admin_password` (and, when needed, `lattice_api_key`) from a
secret manager or the shell. Terraform state contains the Secret value, so
shared deployments must use encrypted, access-controlled remote state. The
secure overlay additionally requires certificate Secrets; create those from
your organization’s CA process as described in `deploy/k8s-secure/README.md`.

## 1. Start LATTICE

For the fastest learning loop, run the durable local backend:

```bash
make dev
```

To run the complete local process-boundary smoke test automatically:

```bash
make smoke-local
```

It verifies readiness, Swagger UI, playground, publish/search, snapshot and
restore, and the honest partial-results response from a disabled local shard.

For the production-shaped path—Redpanda → Go indexer → OpenSearch—run:

```bash
make compose-up
```

With Docker available, the repeatable Compose acceptance flow exercises the
same stack plus Kafka replay and alias-based reindexing:

```bash
make smoke-compose
```

The script publishes through the query service, waits for the separate
indexer to make the document searchable, stops the indexer while another
document is published, restarts it, and verifies redelivery. It then runs a
real OpenSearch reindex and checks that the alias remains readable immediately.

Then open:

| URL | Purpose |
|---|---|
| http://localhost:8080/docs | Swagger UI |
| http://localhost:8080/playground | Guided browser test flow |
| http://localhost:8080/openapi.yaml | OpenAPI contract |
| http://localhost:8080/readyz | Dependency readiness |
| http://localhost:8080/metrics | Prometheus text metrics |
| http://localhost:19091/metrics | Compose indexer lag and DLQ metrics |

```bash
curl -i http://localhost:8080/readyz
curl -fsS http://localhost:8080/openapi.yaml
curl -fsS http://localhost:19091/metrics
```

If `LATTICE_API_KEY` is set, add `-H 'X-API-Key: ...'` to every `/v1` request. Health, readiness, documentation, and metrics remain available for probes and operators.

For TLS/mTLS, set `LATTICE_TLS_CERT_FILE` and `LATTICE_TLS_KEY_FILE`; also set `LATTICE_TLS_CA_FILE` to require verified client certificates. The local walkthrough intentionally uses HTTP unless those variables are configured.

Local mode reports local adapters. Compose mode returns `503` until Kafka and OpenSearch are reachable and the search index exists.

## 2. Publish documents

The endpoint is intentionally shared by the playground and automated tests. Local mode uses the in-process broker and indexer; Compose mode publishes to Redpanda and the separate indexer writes OpenSearch.

```bash
curl -fsS -X POST http://localhost:8080/v1/documents \
  -H 'content-type: application/json' \
  -d '{"id":"doc-001","title":"Raft leader election","body":"A replicated system chooses a leader and commits a log safely.","tags":["consensus","distributed-systems"]}'
```

Publishing the same ID is idempotent at the index: the later document replaces the earlier one.

## 3. Search keyword and semantic meaning

Exact-term search exercises the BM25 branch:

```bash
curl -G http://localhost:8080/v1/search \
  --data-urlencode 'q=Raft leader election' \
  --data-urlencode 'deadline=150ms'

# Require every listed tag (repeat the parameter or use tag=systems,raft)
curl -G http://localhost:8080/v1/search \
  --data-urlencode 'q=consensus' \
  --data-urlencode 'tag=distributed-systems' \
  --data-urlencode 'tag=consensus'
```

Paraphrase search exercises the vector branch and hybrid fusion:

```bash
curl -G http://localhost:8080/v1/search \
  --data-urlencode 'q=how does a cluster choose a coordinator?' \
  --data-urlencode 'deadline=150ms'
```

Inspect `results`, `coverage.shards_queried`, `coverage.shards_answered`, `coverage.complete`, and `timings_ms`. The local adapter uses deterministic vectors so the workflow is reproducible; Compose uses the OpenSearch kNN field.

## 4. Verify cache and pagination behavior

Run one query twice and inspect `/metrics` for cache hits and misses:

```bash
curl -G http://localhost:8080/v1/search --data-urlencode 'q=distributed consensus'
curl -G http://localhost:8080/v1/search --data-urlencode 'q=distributed consensus'
curl -fsS http://localhost:8080/metrics | grep lattice_query_cache
```

Deep offset pagination is rejected:

```bash
curl -i -G http://localhost:8080/v1/search \
  --data-urlencode 'q=consensus' --data-urlencode 'from=10000' --data-urlencode 'size=20'
```

Use the opaque `next_cursor` from a successful response to continue paging. It
is bound to the query, tags, and page size, so changing any of them is rejected:

```bash
FIRST=$(curl -fsS -G http://localhost:8080/v1/search \
  --data-urlencode 'q=consensus' --data-urlencode 'size=20')
CURSOR=$(printf '%s' "$FIRST" | jq -r '.next_cursor')
curl -fsS -G http://localhost:8080/v1/search \
  --data-urlencode 'q=consensus' --data-urlencode 'size=20' \
  --data-urlencode "search_after=$CURSOR"
```

The cursor is an API continuation token; clients should store it exactly as
returned and never decode or construct one themselves.

## 5. Test malformed ingest and replay semantics

```bash
curl -i -X POST http://localhost:8080/v1/documents \
  -H 'content-type: application/json' \
  -d '{"id":"bad","title":42,"body":"invalid"}'
```

The HTTP adapter rejects malformed JSON or schema types with `400`. The indexer path also validates events and places permanent failures in the DLQ. In Compose mode, stop the indexer during ingestion, restart it, and verify that uncommitted Kafka records are redelivered. Deterministic document IDs prevent duplicates.

## 6. Prove honest partial results

Local mode exposes safe failure controls:

```bash
curl -fsS -X POST http://localhost:8080/v1/debug/shards/0 \
  -H 'content-type: application/json' -d '{"available":false}'
curl -G http://localhost:8080/v1/search \
  --data-urlencode 'q=distributed consensus' --data-urlencode 'deadline=150ms'
curl -fsS -X POST http://localhost:8080/v1/debug/shards/0 \
  -H 'content-type: application/json' -d '{"available":true}'
```

The degraded response should arrive within the deadline and report `complete: false` with fewer answered shards. Results from healthy shards remain usable. The cache is invalidated when shard availability changes.

## 7. Exercise operations

These local admin endpoints make recovery visible in Swagger and the playground:

```bash
curl -fsS -X POST http://localhost:8080/v1/admin/snapshot
curl -fsS -X POST http://localhost:8080/v1/admin/reindex
curl -fsS -X POST http://localhost:8080/v1/admin/restore
```

The local snapshot is written atomically and restored through the same index replacement path. In the Compose/Kubernetes OpenSearch path, reindex uses the configured `lattice-documents-read` alias:

```bash
curl -fsS -X POST http://localhost:8080/v1/admin/reindex
curl -fsS -X POST http://localhost:8080/v1/admin/snapshot \
  -H 'content-type: application/json' \
  -d '{"repository":"lattice-s3","snapshot":"manual-2026-08-30"}'
curl -fsS -X POST http://localhost:8080/v1/admin/restore \
  -H 'content-type: application/json' \
  -d '{"repository":"lattice-s3","snapshot":"manual-2026-08-30","source_index":"lattice-documents-1","target_index":"lattice-documents-restore"}'
```

The repository must already be configured in OpenSearch and the concrete `source_index` must match the snapshot metadata. Compose configures a disposable filesystem repository named `lattice-local`, so its native snapshot/restore path can be tested locally:

```bash
SOURCE_INDEX=$(curl -fsS 'http://localhost:19200/_cat/aliases?h=alias,index' \
  | awk '$1 == "lattice-documents-read" {print $2}')
SNAPSHOT="walkthrough-$(date -u +%Y%m%dT%H%M%SZ)"
TARGET_INDEX="lattice-restore-walkthrough-$(date -u +%s)"
curl -fsS -X POST http://localhost:8080/v1/admin/snapshot \
  -H 'content-type: application/json' \
  -d "{\"repository\":\"lattice-local\",\"snapshot\":\"$SNAPSHOT\"}"
time curl -fsS -X POST http://localhost:8080/v1/admin/restore \
  -H 'content-type: application/json' \
  -d "{\"repository\":\"lattice-local\",\"snapshot\":\"$SNAPSHOT\",\"source_index\":\"$SOURCE_INDEX\",\"target_index\":\"$TARGET_INDEX\"}"
```

The Kubernetes snapshot CronJob is enabled for scheduled backups; the restore-drill CronJob is intentionally suspended until it points at an empty scratch cluster. This makes the production restore claim measurable instead of implied by the local snapshot test.

## 8. Run the automated evidence

```bash
make test
make test-integration
make test-chaos
make wal-crash-evidence
make election-evidence
go test -race ./...
go vet ./...
make bench-query
```

The integration and chaos suites are deterministic local acceptance tests. The
Compose production-shaped capacity and restore measurements are recorded in
`docs/benchmarks.md`; Kubernetes-scale and production-hardware evidence remains
separate work.

Once the declared corpus is loaded in Compose or Kubernetes, measure API
percentiles with the production-shaped runner:

```bash
LATTICE_E2E_URL=http://localhost:8080 \
LATTICE_BENCH_QUERIES=10000 \
LATTICE_BENCH_CONCURRENCY=32 \
make bench-remote
```

The runner writes p50/p95/p99, throughput, HTTP failures, incomplete coverage,
and API-error counts to `docs/remote-benchmark.json`. Run it once at rest and
again while indexing is triggering segment merges; do not treat local-mode
numbers as OpenSearch capacity evidence.

To load a deterministic corpus through the same API used by the playground,
use the CLI. This can target local mode or the Kafka-backed Compose service:

```bash
LATTICE_URL=http://localhost:8080 \
LATTICE_API_KEY="$LATTICE_API_KEY" \
go run ./cmd/latticectl seed -count 1000000 -concurrency 64
```

The command uses stable IDs, reports progress every 1,000 documents, and
limits concurrent publish requests to the configured worker count. It is safe
to rerun because indexing is an ID-based replacement. Choose the concurrency
after measuring the cluster’s rejection and ingest-lag metrics; `64` is only a
starting point for a declared capacity run.

Publishing is asynchronous in Compose. After the CLI exits, wait for
`/readyz` to report the declared corpus count and watch
`http://localhost:19091/metrics` until `lattice_ingest_lag_seconds` returns to
the normal range. The measured million-document run took 450.53s to publish
and then drained to 1,000,008 searchable documents before benchmarking.

## 9. Kubernetes path

Render the manifests before applying them:

```bash
kubectl kustomize deploy/k8s
terraform -chdir=deploy/terraform fmt -check -recursive
```

For the certificate-backed deployment, create the required secrets described
in [`deploy/k8s-secure/README.md`](../deploy/k8s-secure/README.md), then render
or apply the secure overlay:

```bash
kubectl kustomize deploy/k8s-secure
kubectl apply -k deploy/k8s-secure
```

Install cert-manager and the OpenSearch Operator first; the exact pinned
commands are in [`deploy/terraform/README.md`](../deploy/terraform/README.md).
Install the Prometheus Operator (or its CRDs) before applying the base
Kustomization because `ServiceMonitor` and `PrometheusRule` are intentionally
part of the observability contract. Terraform creates the namespace,
operator-managed OpenSearch cluster/node pool, credentials, and optional
API-key Secret. Argo CD then reconciles the application and configuration
resources; the namespace and OpenSearchCluster reference in
`deploy/k8s/opensearch.yaml` are intentionally not part of the Argo
kustomization because Terraform owns them:

```bash
export TF_VAR_opensearch_admin_password='replace-with-a-strong-local-value'
terraform -chdir=deploy/terraform init
terraform -chdir=deploy/terraform plan \
  -var='lattice_api_key=replace-me'
terraform -chdir=deploy/terraform apply \
  -var='lattice_api_key=replace-me'
```

For a disposable one-node kind validation, use
`-var='opensearch_replicas=1'`. Configure the host AIO limit described in the
Terraform README, then apply the base manifests and wait for all five pods
(OpenSearch, Kafka, Redis, indexer, and query) to become ready. Verify the
actual application path through a port-forward:

```bash
kubectl -n lattice port-forward svc/lattice-query 18080:8080
curl -fsS http://localhost:18080/readyz
curl -fsS -X POST http://localhost:18080/v1/documents \
  -H 'content-type: application/json' -H 'X-API-Key: replace-me' \
  -d '{"id":"kind-e2e-001","title":"Kubernetes Kafka path","body":"OpenSearch operator and Redpanda end to end verification","tags":["kind","e2e"]}'
curl -fsS -G http://localhost:18080/v1/search \
  -H 'X-API-Key: replace-me' --data-urlencode 'q=Kubernetes Kafka path'
```

For the secure overlay, generate the Terraform plan with
`-var='opensearch_http_tls=true'` and provide
`TF_VAR_opensearch_admin_password` before applying `deploy/k8s-secure`; this
enables HTTPS on the Terraform-owned OpenSearch HTTP endpoint to match the
overlay’s client URLs.

For an authenticated OpenSearch cluster, provide `OPENSEARCH_USERNAME` and `OPENSEARCH_PASSWORD` through the external Secret referenced by the workloads, and mount its CA certificate at the path supplied by `OPENSEARCH_CA_FILE`.

Argo CD is represented by `deploy/argocd/application.yaml` and reconciles the application/configuration resources. Before exposing a cluster, apply the security baseline in `docs/THREAT_MODEL.md`, configure real TLS/secrets, and complete the restore drill in `docs/RUNBOOK.md`.

To verify Argo reconciliation against the exact current worktree without
pushing first, run:

```bash
make smoke-argocd
```

The harness creates a temporary Git snapshot, disposable kind cluster, and
temporary Argo Application. It installs the pinned operators, applies
Terraform-owned infrastructure, waits for Argo to reconcile the application,
and then exercises publish → Redpanda → indexer → hybrid search. It deletes the
temporary cluster and Git source when it exits. OpenSearch JVM startup can be
slow on resource-constrained WSL/kind hosts; the harness extends that
disposable pod's startup window and fails with pod diagnostics if readiness is
not reached. Set
`LATTICE_BUILD_IMAGE=1` when the local `lattice:dev` image must be rebuilt.

The repository is ready to use after the local and Compose checks above. The
Kubernetes secure overlay still requires environment-owned certificates and a
secret manager; those values are intentionally absent from Git. The
production scratch-cluster restore drill, real mTLS identity wiring, and
long-running cluster chaos campaign remain deployment-specific verification,
not hidden claims in the local walkthrough.
