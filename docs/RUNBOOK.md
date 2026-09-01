# LATTICE Runbook

## Search is slow

1. Check query p99 by stage in Grafana.
2. Correlate a slow request using its `X-Request-ID` in the query-service log.
3. Compare `opensearch_merge_duration_seconds` with the query histogram.
4. Check `opensearch_jvm_heap_used_ratio` and circuit-breaker trips.
5. Check shard-level latency and document skew.
6. Reject deep pagination or expensive aggregations at the API boundary.
7. Record the incident, query mix, merge state, and mitigation in the postmortem.

## A shard or data node is unavailable

1. Confirm cluster health and the affected shard count.
2. Confirm responses expose `coverage.complete: false`.
3. Keep serving available results within the deadline.
4. Replace the node or restore the shard from its replica.
5. Verify the cluster returns green within the recovery target.
6. Record the duration and number of incomplete responses.

## Ingest lag is rising

1. Check `lattice_ingest_lag_seconds` by topic and partition.
2. Check bulk rejection and OpenSearch heap pressure.
3. Confirm the indexer is pausing on rejection instead of tight-loop retrying.
4. Scale indexers only after confirming the cluster can absorb more work.
5. Inspect the DLQ for poison documents.

## Restore drill

1. Create or select a dated snapshot.
2. Restore into an empty scratch cluster.
3. Record elapsed restore time.
4. Compare document count with the source.
5. Run sample keyword and semantic queries.
6. Publish the result in `docs/benchmarks.md`.

The Compose acceptance path performs this drill against an OpenSearch
filesystem repository named `lattice-local`. The latest full-corpus run
restored 1,000,008 documents into a fresh target in 25.016s, with the target
document count verified through OpenSearch. The eight-document smoke run
completed in 712ms. These are local Compose measurements, not the production
S3 restore SLO; that still requires a declared scratch cluster and object-store
credentials.

## Security baseline

Configure `LATTICE_API_KEY` through an external Secret. For the Go API, set `LATTICE_TLS_CERT_FILE` and `LATTICE_TLS_KEY_FILE`; add `LATTICE_TLS_CA_FILE` to require and verify client certificates (mTLS). Then use default-deny network policies, non-root read-only containers, scoped RBAC, external secret storage, and audit logging before exposing the cluster beyond a local development environment. The API key is a baseline guard for `/v1`, not a complete identity or authorization system.
