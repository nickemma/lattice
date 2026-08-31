# LATTICE end-to-end walkthrough

This is the executable path through the project. Use Swagger at `/docs`, the browser playground at `/playground`, or the equivalent `curl` commands.

## 1. Start LATTICE

For the fastest learning loop, run the durable local backend:

```bash
make dev
```

For the production-shaped path—Redpanda → Go indexer → OpenSearch—run:

```bash
make compose-up
```

Then open:

| URL | Purpose |
|---|---|
| http://localhost:8080/docs | Swagger UI |
| http://localhost:8080/playground | Guided browser test flow |
| http://localhost:8080/openapi.yaml | OpenAPI contract |
| http://localhost:8080/readyz | Dependency readiness |
| http://localhost:8080/metrics | Prometheus text metrics |

```bash
curl -i http://localhost:8080/readyz
curl -fsS http://localhost:8080/openapi.yaml
```

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

The snapshot is written atomically, restored through the same index replacement path, and can be inspected in the response. The remote OpenSearch deployment uses its native alias and snapshot mechanisms; the local endpoints intentionally return `501` there rather than pretending to control a remote cluster.

## 8. Run the automated evidence

```bash
make test
make test-integration
make test-chaos
go test -race ./...
go vet ./...
make bench-query
```

The integration and chaos suites are deterministic local acceptance tests. Production capacity claims remain unfilled until the one-million-document benchmark and restore drill are run on declared hardware; record those results in `docs/benchmarks.md`.

## 9. Kubernetes path

Render the manifests before applying them:

```bash
kubectl kustomize deploy/k8s
terraform -chdir=deploy/terraform fmt -check -recursive
```

Argo CD is represented by `deploy/argocd/application.yaml`. Before exposing a cluster, apply the security baseline in `docs/THREAT_MODEL.md`, configure real TLS/secrets, and complete the restore drill in `docs/RUNBOOK.md`.
