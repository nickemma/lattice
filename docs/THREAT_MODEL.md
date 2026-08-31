# LATTICE Threat Model

## Assets

- Source documents and their embeddings
- Kafka events and offsets
- Search indexes and snapshots
- API credentials and cluster credentials
- Availability, integrity, and coverage signals

## Threats and controls

| Threat | Control |
|---|---|
| Document loss during indexer crash | At-least-once offsets, deterministic IDs, WAL-backed local mode, DLQ |
| Unauthorized document access | Authentication, authorization, TLS/mTLS, network policy |
| Query-driven memory exhaustion | Pagination caps, bounded query cost, circuit breakers, rate limits |
| Poison document blocking ingestion | Bounded retries and DLQ with partition progress |
| Snapshot exposure | Private bucket, encryption, least-privilege IAM, retention policy |
| Split brain in custom store | Raft quorum and leader terms; partition tests |
| Compromised container | Non-root user, read-only filesystem, minimal image, restricted RBAC |
| Silent degraded search | Coverage in every response, metrics, alerts, and runbook |

## Trust boundaries

Clients cross into the API boundary. Producers cross into the ingest boundary. The API, indexer, embedding service, Kafka, OpenSearch, Redis, and object storage are separate operational dependencies and must not share implicit trust.

The local debug shard endpoint is development-only and must be disabled or authenticated in production.
