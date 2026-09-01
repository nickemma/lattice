# Postmortem: Compose integration brought up the wrong system

## Summary

- Date: 2026-08-31
- Duration: approximately 90 minutes of local integration work
- Severity: SEV-3 (pre-production; no customer traffic)
- User impact: the first Compose attempts could not publish documents or make
  them searchable. No production data was affected.

## What happened

The first real Redpanda/OpenSearch Compose run exposed six independent
integration defects. Redpanda was marked unhealthy because its health command
used an unsupported flag. Its Kafka metadata also advertised loopback and did
not expose a named listener to other containers. The indexer service started
the query binary because the image entrypoint was fixed at `/lattice` and
Compose `command` only supplied an argument. Finally, a Redis empty-result
cache could outlive asynchronous indexing, and an OpenSearch reindex could
swap the alias before the destination index had refreshed. The subsequent
recovery drill found two more boundary issues: the non-root OpenSearch process
could not write a fresh snapshot volume, and restoring alias metadata conflicted
with the existing write alias.

## Detection

The failures were detected by the executable Compose walkthrough:

1. `docker compose up` reported an unhealthy Redpanda dependency.
2. A document publish returned `502 Unknown Topic Or Partition`.
3. Search polling found no document while the supposed indexer was actually
   serving the query binary.
4. Direct OpenSearch inspection showed documents arriving after the indexer
   entrypoint was corrected, exposing the cache and refresh races.

## Timeline

| Event | Observation | Correction |
|---|---|---|
| Stack start | Redpanda unhealthy | Use `rpk cluster health --exit-when-healthy` |
| Broker metadata check | No reachable broker in metadata | Configure `internal://0.0.0.0:9092` and advertise `internal://redpanda:9092` |
| First publish | Topic did not exist | Add an idempotent `redpanda-init` topic initializer |
| Indexer inspection | Query server was running in indexer container | Override Compose entrypoint with `/lattice-indexer` |
| Search after publish | Empty result was cached | Include backend count in cache keys and clear Redis on state-changing operations |
| Search after reindex | New alias briefly returned empty results | Request `refresh=true` on `_reindex` before swapping the alias |
| Snapshot repository registration | Non-root OpenSearch could not write the named volume | Add a one-shot root-owned permission initializer |
| API restore | Restored alias metadata created two write indices | Restore with `include_aliases:false`, then normalize the alias |

## Root cause

The deployment shape had been validated syntactically and with unit tests,
but not against the actual container entrypoints, broker metadata, or
OpenSearch refresh behavior. The missing test was a real service-boundary
acceptance test.

## Coverage and data integrity

No acknowledged document was lost. After the fixes, the acceptance flow
published a document, stopped the indexer, published another document,
restarted the indexer, and observed the buffered document after Kafka replay.
The final Compose run reported complete three-shard coverage, and direct
OpenSearch count and alias checks agreed with the API results.

## Mitigation and recovery

The corrected flow is automated as `make smoke-compose`. It verifies readiness,
Swagger UI, the playground, asynchronous publish/search, indexer restart and
replay, alias reindex visibility, metrics, and the OpenSearch read alias.

## Corrective actions

- [x] Provision the Kafka topic as an idempotent Compose dependency.
- [x] Make broker listener advertisements explicit and network-reachable.
- [x] Start the actual indexer executable in the indexer container.
- [x] Prevent asynchronous ingest from serving stale empty Redis results.
- [x] Refresh reindexed documents before the alias swap.
- [x] Initialize the non-root snapshot volume with writable permissions.
- [x] Restore without alias metadata, then attach one write alias atomically.
- [x] Keep this scenario executable in the walkthrough and benchmark log.
- [ ] Add the equivalent acceptance run to a provisioned Kubernetes cluster.
- [ ] Measure the same behavior under node loss, segment merges, and restore.

## What we learned

Builds and rendered manifests do not prove that a distributed system is wired
correctly. The acceptance boundary must include container process selection,
service discovery metadata, durable stream replay, cache invalidation, and
read-after-operation visibility. LATTICE now tests those boundaries directly
in Compose while keeping production-scale claims separate in the benchmark
report.
