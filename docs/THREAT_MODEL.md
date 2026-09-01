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
| Unauthorized document access | Optional `LATTICE_API_KEY` / `X-API-Key` guard for `/v1`, plus TLS/mTLS, network policy |
| Query-driven memory exhaustion | Pagination caps, bounded query cost, circuit breakers, rate limits |
| Poison document blocking ingestion | Bounded retries and DLQ with partition progress |
| Snapshot exposure | Private bucket, encryption, least-privilege IAM, retention policy |
| Split brain in custom store | Raft quorum and leader terms; partition tests |
| Compromised container | Non-root user, read-only filesystem, minimal image, restricted RBAC |
| Silent degraded search | Coverage in every response, metrics, alerts, and runbook |

## Trust boundaries

Clients cross into the API boundary. Producers cross into the ingest boundary. The API, indexer, embedding service, Kafka, OpenSearch, Redis, and object storage are separate operational dependencies and must not share implicit trust.

The local debug shard endpoint is development-only and must be disabled or authenticated in production. The Go API supports TLS 1.3 and client-certificate verification when `LATTICE_TLS_CA_FILE` is configured. The OpenSearch adapter supports basic credentials, a custom CA, and an optional client certificate through `OPENSEARCH_USERNAME`, `OPENSEARCH_PASSWORD`, `OPENSEARCH_CA_FILE`, `OPENSEARCH_CLIENT_CERT_FILE`, and `OPENSEARCH_CLIENT_KEY_FILE`. The `deploy/k8s-secure` overlay wires those files from required secrets. Kubernetes applies an application-pod default-deny baseline and only namespace-local service traffic is allowed; an external ingress controller must be granted an explicit, reviewed allow rule. The API-key guard is a deployable baseline, not a replacement for identity-aware authorization, secret rotation, or audit logging.
