# LATTICE Runbook

## Search is slow

1. Check query p99 by stage in Grafana.
2. Compare `opensearch_merge_duration_seconds` with the query histogram.
3. Check `opensearch_jvm_heap_used_ratio` and circuit-breaker trips.
4. Check shard-level latency and document skew.
5. Reject deep pagination or expensive aggregations at the API boundary.
6. Record the incident, query mix, merge state, and mitigation in the postmortem.

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

## Security baseline

Use mTLS, default-deny network policies, non-root read-only containers, scoped RBAC, external secret storage, and audit logging before exposing the cluster beyond a local development environment.
