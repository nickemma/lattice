# LATTICE API reference

This is the complete HTTP contract for the LATTICE query service. It is
intended for a second engineer who has not read the implementation. The
machine-readable contract is served at `GET /openapi.yaml`; interactive
Swagger UI is at `GET /docs`.

## Quick start

```bash
make dev
open http://localhost:8080/docs
open http://localhost:8080/playground
```

The shortest API check is: publish a document, search for a word from its
title, then repeat the search with a paraphrase. The expected publish status
is `202`; the expected search status is `200` with `coverage.complete: true`.
The complete, copy/paste version of this flow—including Compose, Kafka replay,
partial results, snapshots, restore, and Kubernetes—is in
[`docs/walkthrough.md`](docs/walkthrough.md).

## Connection and authentication

The default local address is `http://localhost:8080`. In Kubernetes, port
forward the `lattice-query` Service or expose it through a reviewed ingress.

When `LATTICE_API_KEY` is set, every path beginning with `/v1/` requires the
same value in `X-API-Key`. Health, readiness, metrics, documentation, and the
playground remain available to probes and developers.

```bash
curl -H 'X-API-Key: replace-with-your-key' \
  'http://localhost:8080/v1/search?q=consensus'
```

An invalid or missing key returns `401` and `WWW-Authenticate`. TLS uses
`LATTICE_TLS_CERT_FILE` and `LATTICE_TLS_KEY_FILE`; adding
`LATTICE_TLS_CA_FILE` requires a client certificate signed by that CA.

Every response includes `X-Request-ID`. A caller-supplied ID is preserved when
it is at most 128 characters and contains only letters, digits, `-`, `_`, `.`,
or `:`. Otherwise the service generates one. The ID is written to logs.

Unless specified otherwise, errors are JSON:

```json
{"error":"human-readable explanation"}
```

## Endpoint summary

| Method | Path | Purpose | API key when configured |
|---|---|---|---|
| GET | `/healthz` | Process liveness | No |
| GET | `/readyz` | Dependency readiness | No |
| GET | `/metrics` | Prometheus text metrics | No |
| GET | `/openapi.yaml` | OpenAPI 3.0 contract | No |
| GET | `/docs` | Swagger UI | No |
| GET | `/playground` | Browser testing console | No |
| POST | `/v1/documents` | Publish one document | Yes |
| GET | `/v1/search` | Hybrid keyword/vector search | Yes |
| POST | `/v1/debug/shards/{shard}` | Local failure drill | Yes |
| POST | `/v1/admin/reindex` | Rebuild and swap an index | Yes |
| POST | `/v1/admin/snapshot` | Create a snapshot | Yes |
| POST | `/v1/admin/restore` | Restore a snapshot | Yes |

There is intentionally no public delete endpoint in v1. Publishing an existing
document ID is an idempotent replacement.

## Health and operations

### `GET /healthz`

Returns `200` when the process is alive. It does not test dependencies.

```json
{"status":"ok"}
```

### `GET /readyz`

Returns `200` when configured dependencies are ready, otherwise `503`. The
body identifies the dependency and whether the local or production-shaped
adapter is active.

```json
{
  "status":"ready",
  "dependencies": {
    "kafka":{"status":"ready","mode":"kafka"},
    "opensearch":{"status":"ready","mode":"configured-backend","documents":42},
    "redis":{"status":"ready","mode":"redis"},
    "embedding":{"status":"ready","mode":"deterministic-local"}
  }
}
```

### `GET /metrics`

Returns Prometheus text format. The important query series are
`lattice_query_requests_total`, `lattice_query_incomplete_total`,
`lattice_query_duration_seconds` (histogram),
`lattice_query_cache_hits_total`, `lattice_query_cache_misses_total`, and
`lattice_index_documents`. Ingest totals include
`lattice_ingest_documents_total` and `lattice_ingest_dlq_total`.

The separate Compose indexer exposes partition lag at
`http://localhost:19091/metrics`, including
`lattice_ingest_lag_seconds{partition="..."}`.

## Documents

### `POST /v1/documents`

Accepts one document. Unknown JSON fields are rejected, the body is limited to
1 MiB, and `id`, `title`, and `body` must be non-empty strings. `tags` is
optional.

Request:

```json
{
  "id":"doc-001",
  "title":"Raft leader election",
  "body":"A replicated system chooses a leader and commits a log safely.",
  "tags":["consensus","distributed-systems"]
}
```

The response is `202 Accepted`. Local mode returns after the in-process broker
and indexer path; Kafka mode returns after publishing to the stream and marks
the operation as asynchronous.

Local response:

```json
{"accepted":true,"id":"doc-001"}
```

Kafka-backed response:

```json
{"accepted":true,"id":"doc-001","streamed":true}
```

`400` means malformed JSON, an unknown field, missing required field, or an
oversized body. `401` means authentication failed. `502` means the configured
stream rejected the event.

```bash
curl -fsS -X POST http://localhost:8080/v1/documents \
  -H 'content-type: application/json' \
  -d '{"id":"doc-001","title":"Raft leader election","body":"A replicated system chooses a leader.","tags":["consensus"]}'
```

The Kafka path is at-least-once: an offset is committed only after indexing.
Stable IDs make redelivery safe. Permanent per-document failures are sent to
`<topic>.dlq` with the source payload and error.

## Search

### `GET /v1/search`

Runs BM25 keyword search and semantic vector search, then combines both with
reciprocal rank fusion. It returns completed work inside the deadline rather
than hiding unavailable shards.

| Parameter | Required | Default/limits | Meaning |
|---|---:|---|---|
| `q` | Yes | non-empty | Query text |
| `deadline` | No | `200ms`, max `30s` | End-to-end search budget |
| `size` | No | `10`, range `1..100` | Results per page |
| `from` | No | `0`, non-negative | Shallow offset |
| `search_after` | No | opaque | Cursor from a previous response |
| `tag` | No | none | Required tags; repeat or comma-separate |

Tags are lowercased, trimmed, deduplicated, and treated as an AND filter.

```bash
curl -G http://localhost:8080/v1/search \
  --data-urlencode 'q=cluster coordinator' \
  --data-urlencode 'tag=consensus' \
  --data-urlencode 'tag=distributed-systems' \
  --data-urlencode 'deadline=150ms'
```

Response shape:

```json
{
  "results":[{
    "document":{"id":"doc-001","title":"Raft leader election","body":"A replicated system chooses a leader.","tags":["consensus"]},
    "score":0.0164,
    "source":"hybrid"
  }],
  "coverage":{"shards_queried":3,"shards_answered":3,"complete":true},
  "timings_ms":{"parse":0.04,"bm25":0.31,"vector":0.22,"fuse":0.05}
}
```

`source` identifies the contributing branch (`bm25`, `vector`, or `hybrid`).
`timings_ms` contains parse, BM25, vector, and fusion phase timings.
`errors` describes a deadline or branch failure. `cache_hit` is present when a
complete response came from the bounded cache.

Coverage is the correctness contract:

- `complete: true` means every queried shard answered within the budget.
- `complete: false` means at least one shard did not answer; returned results
  remain usable but are partial.
- `shards_answered` can be lower than `shards_queried` because of a failed
  shard or deadline.
- Incomplete responses do not advertise a continuation cursor.

`400` covers a missing/invalid query, deadline, paging value, malformed
cursor, cursor mismatch, or deep offset without a cursor. `429` means the
configured `LATTICE_MAX_INFLIGHT_QUERIES` limit is exhausted. A usable partial
response is still returned as `200`.

### Pagination

Offsets below `10000` are allowed. At `from >= 10000`, use the opaque
`next_cursor` from a complete response. The cursor is bound to query, tags,
and page size; never construct or decode it in a client.

```bash
first=$(curl -fsS -G http://localhost:8080/v1/search \
  --data-urlencode 'q=consensus' --data-urlencode 'size=20')
cursor=$(printf '%s' "$first" | jq -r '.next_cursor')
curl -fsS -G http://localhost:8080/v1/search \
  --data-urlencode 'q=consensus' --data-urlencode 'size=20' \
  --data-urlencode "search_after=$cursor"
```

## Local failure drill

### `POST /v1/debug/shards/{shard}`

This route works only with the local in-process backend and returns `501`
against OpenSearch. It toggles availability and/or injects a bounded delay.

```json
{"available":false,"delay_ms":0}
```

`delay_ms` must be `0..30000`; invalid JSON or shard IDs return `400`.

```bash
curl -fsS -X POST http://localhost:8080/v1/debug/shards/0 \
  -H 'content-type: application/json' -d '{"available":false}'
curl -fsS -G http://localhost:8080/v1/search \
  --data-urlencode 'q=distributed consensus' --data-urlencode 'deadline=150ms'
curl -fsS -X POST http://localhost:8080/v1/debug/shards/0 \
  -H 'content-type: application/json' -d '{"available":true}'
```

The middle response should show fewer answered shards and `complete:false`.

## Administrative operations

All admin routes are `POST` and are operator actions, not application-level
authorization boundaries.

### `POST /v1/admin/reindex`

Local mode rebuilds the local index. With `OPENSEARCH_ALIAS` configured, the
remote adapter creates a new generation, reindexes into it, and atomically
swaps the read alias without query downtime. Remote failures return `502` and
an unsupported backend returns `501`.

### `POST /v1/admin/snapshot`

Local mode writes an fsynced snapshot and atomically replaces the path:

```json
{"path":"/tmp/lattice-snapshot.json"}
```

Remote OpenSearch mode requires `repository` and `snapshot`:

```json
{"repository":"lattice-local","snapshot":"manual-2026-09-01"}
```

Missing remote fields return `400`; remote failures return `502`; local I/O
failures return `500`.

### `POST /v1/admin/restore`

Local mode accepts `{"path":"/tmp/lattice-snapshot.json"}` and returns the
restored count. Remote mode requires `repository` and `snapshot`; optional
`source_index` and `target_index` control the concrete restore indices:

```json
{
  "repository":"lattice-local",
  "snapshot":"manual-2026-09-01",
  "source_index":"lattice-documents-1",
  "target_index":"lattice-documents-restore"
}
```

When an alias is configured, the adapter restores the target and reattaches
the read alias. Missing fields return `400`, remote failures `502`, and local
I/O failures `500`.

## Browser and generated clients

`/docs` loads Swagger UI against `/openapi.yaml`; use **Authorize** to enter an
API key and try requests. `/playground` provides publish, search, shard-failure,
reindex, snapshot, and restore buttons. It is a development aid, not a
security boundary. Import `/openapi.yaml` into client generators or Postman.

For environment setup and the complete publish → stream → index → search →
failure → restore exercise, see [`docs/walkthrough.md`](docs/walkthrough.md).

## Configuration boundary

No real secret is needed for `make dev` or the default Compose development
profile. If `LATTICE_API_KEY` is set, send it in `X-API-Key`; if it is not set,
local `/v1` requests are intentionally unauthenticated. Kubernetes and
production-shaped deployments must inject API keys, OpenSearch credentials,
TLS keys, and client certificates from a secret manager or externally-created
Kubernetes Secrets. Never copy real values into `.env.example`, this file, or
the OpenAPI document.
