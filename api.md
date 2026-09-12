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
is `202`; the expected search status is `200` with `coverage.complete: true`
and `coverage.reason: "complete"`.
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
`lattice_query_requests_total`, `lattice_query_duration_seconds` (histogram),
`lattice_query_cache_hits_total`, `lattice_query_cache_misses_total`, and
`lattice_index_documents`. Ingest totals include
`lattice_ingest_documents_total` and `lattice_ingest_dlq_total`.

Three series describe query health, and they are deliberately independent:

| Series | Counts |
|---|---|
| `lattice_query_incomplete_total` | Responses that lost shard coverage |
| `lattice_query_degraded_total` | Responses that carried at least one error |
| `lattice_query_coverage_reason_total{reason="…"}` | Responses by coverage reason |

A response can be in either counter, both, or neither. Alert on them
separately: rising `incomplete` with flat `degraded` is a shard leaving
rotation, while rising `degraded` with flat `incomplete` is a dependency
failing behind an otherwise healthy cluster. Every `reason` label is exported
even at zero, so a missing label means an old build rather than a quiet system.

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
  "coverage":{"shards_queried":3,"shards_answered":3,"complete":true,"degraded":false,"reason":"complete"},
  "timings_ms":{"parse":0.04,"bm25":0.31,"vector":0.22,"fuse":0.05}
}
```

`source` identifies the contributing branch (`bm25`, `vector`, or `hybrid`).
`timings_ms` contains parse, BM25, vector, and fusion phase timings, so a tail
can be attributed to the keyword branch or the kNN branch rather than to the
query as a whole. `errors` carries human-readable detail. `cache_hit` is
present when a response came from the bounded cache.

### Coverage

Coverage is the correctness contract, and it answers **two independent
questions**. Reading either one alone will mislead you.

| Field | Answers only |
|---|---|
| `complete` | Did every queried shard answer? |
| `degraded` | Did anything fail while answering? |

All four combinations are real and all four are reachable:

| `complete` | `degraded` | What happened |
|---|---|---|
| `true` | `false` | Normal. Every shard answered, nothing failed. |
| `false` | `false` | A shard is out of rotation. Nothing errored; results are partial. |
| `true` | `true` | Every shard answered, but a dependency failed — for example the embedding service died, so only the keyword branch contributed. |
| `false` | `true` | The deadline expired with work outstanding. |

`reason` is the machine-readable classification. **Switch on it; never parse
the `errors` strings.** It is a closed set:

| `reason` | Meaning | Operational response |
|---|---|---|
| `complete` | Nothing went wrong. | None. |
| `deadline` | The budget expired before outstanding work returned. Coverage may still be complete if what was lost was a whole branch rather than a shard. | Raise the deadline, or find what got slow. |
| `shard_unavailable` | A shard did not answer and no deadline fired. The shard is out of rotation, not merely slow. | Check cluster health and shard allocation. |
| `dependency_error` | A dependency the query needs failed. Shard coverage may still be complete. | Check the embedding service and the index client. |
| `invalid_request` | Rejected before any shard was queried. Normally surfaced as `400` instead. | Fix the caller. |

Further guarantees:

- `shards_answered` below `shards_queried` always means real coverage loss.
  With OpenSearch it is the **minimum** successful shard count across the
  keyword and vector branches, not the maximum: if the keyword branch saw 3 of
  3 shards while the vector branch saw 2 of 3, the response really is missing a
  shard's worth of vector candidates, and reporting 3 would hide exactly the
  event this field exists to expose.
- A response that is incomplete **or** degraded is never cached and never
  advertises a continuation cursor. Only `complete && !degraded` is
  continuable.
- Partial results are still `200`. The status code describes the request, not
  the completeness of the answer; `coverage` describes the answer.

```bash
# Route on the classification, not on the prose.
curl -fsS -G http://localhost:8080/v1/search \
  --data-urlencode 'q=consensus' | jq -r '.coverage.reason'
```

#### This contract is measured, not asserted

Two experiments drive 5,000 searches per configuration and fail loudly if the
contract does not hold. Both were run at commit `c0a724f`.

**On a real cluster** (`make experiment-partial-opensearch`): OpenSearch 2.17.1,
three data nodes, three shards, no replicas. A data node was stopped, the cluster
went red, the API kept serving, and the search API reported `_shards total=3
successful=2`:

| `phrase`, concurrency 4 | Baseline | Node stopped | Recovered |
|---|---:|---:|---:|
| Incomplete | 0 | **5,000** | 0 |
| Degraded | 0 | **0** | 0 |
| With `errors` | 0 | **0** | 0 |
| `reason` | `complete` | `shard_unavailable` | `complete` |

**In-process** (`make experiment-partial-local`), the controlled comparison:

| | Baseline | Shard removed |
|---|---:|---:|
| Completed | 5,000 | 5,000 |
| `complete: false` | 0 | 5,000 |
| `degraded: true` | 0 | 0 |
| Responses with `errors` | 0 | 0 |
| `reason` | `complete` × 5,000 | `shard_unavailable` × 5,000 |

Five of nine configurations isolated the availability path exactly like that
in-process, and one did on the real cluster. Where a configuration ran hot enough
that the deadline also expired, the invariant the split exists to guarantee held
to the response:

```
api_error_responses == coverage_reasons["deadline"]
```

In-process: 1203 = 1203, 16 = 16, 1370 = 1370, 768 = 768. On OpenSearch: 7 = 7,
10 = 10, 2 = 2. **Every error was a deadline expiry; none was an availability
loss.** The experiments fail loudly if that equality breaks, which is what would
happen if completeness were ever recoupled to the error list.

Note the caching consequence below is in-process behaviour. The OpenSearch
overlay runs without Redis, where losing a shard means scanning less data and
degraded mode is actually *cheaper* — the effect reverses between backends,
which is why no latency figure here is quoted without naming the backend that
produced it.

One consequence worth planning for as a client: an incomplete or degraded
response is never cached, so a degraded response is also an uncached one. In the
run above the baseline served 5,000 of 5,000 from cache and the fault run served
none, which moved p99 from 2 ms to 87 ms. Most of that gap is the cold cache
rather than the missing shard — compare degraded states against each other, not
against a cache-warm baseline.

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

**This is an experiment control, not a production API.** It injects a fault
in-process. A number produced with it is an *injected* fault on the *in-memory*
backend, and must be reported as such — it is not an observed node failure, and
OpenSearch has no equivalent knob, so results from the two backends are not
interchangeable. To induce a real shard loss in a real cluster, stop a data node
(see [`scripts/partial-results-opensearch.sh`](scripts/partial-results-opensearch.sh)).

The two failure classes look different in the response, which is the point:

```bash
# 1. A shard leaves rotation. Nothing errors.
curl -fsS -X POST http://localhost:8080/v1/debug/shards/1 \
  -H 'content-type: application/json' -d '{"available":false}'
curl -fsS -G http://localhost:8080/v1/search \
  --data-urlencode 'q=distributed consensus' --data-urlencode 'deadline=150ms' \
  | jq '{complete: .coverage.complete, degraded: .coverage.degraded,
         reason: .coverage.reason, errors: .errors}'
# → {"complete": false, "degraded": false, "reason": "shard_unavailable", "errors": null}

# 2. A shard stalls past the deadline. This one does error.
curl -fsS -X POST http://localhost:8080/v1/debug/shards/1 \
  -H 'content-type: application/json' -d '{"available":true,"delay_ms":5000}'
curl -fsS -G http://localhost:8080/v1/search \
  --data-urlencode 'q=distributed consensus' --data-urlencode 'deadline=150ms' \
  | jq '{complete: .coverage.complete, degraded: .coverage.degraded,
         reason: .coverage.reason}'
# → {"complete": false, "degraded": true, "reason": "deadline"}

# 3. Put it back.
curl -fsS -X POST http://localhost:8080/v1/debug/shards/1 \
  -H 'content-type: application/json' -d '{"available":true,"delay_ms":0}'
```

Both middle responses show `complete: false` with fewer answered shards. They
are different events, and `reason` is what tells them apart.

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

## Index layout

When LATTICE creates its OpenSearch index it uses 3 primary shards and 1
replica. Both are overridable, and they only apply at index creation — an
existing index is never re-laid-out.

| Variable | Default | Notes |
|---|---:|---|
| `OPENSEARCH_SHARDS` | `3` | Must be at least 1 |
| `OPENSEARCH_REPLICAS` | `1` | `0` is meaningful, not "unset" |

`OPENSEARCH_REPLICAS=0` exists for the partial-results experiment. With a
replica available, stopping a data node is routed around and coverage correctly
stays complete, so there is no way to observe an unavailable shard. Zero
replicas means one stopped node genuinely removes a shard from the cluster.
**Do not run zero replicas in production**; it trades durability and
availability for an observable failure mode.

## Configuration boundary

No real secret is needed for `make dev` or the default Compose development
profile. If `LATTICE_API_KEY` is set, send it in `X-API-Key`; if it is not set,
local `/v1` requests are intentionally unauthenticated. Kubernetes and
production-shaped deployments must inject API keys, OpenSearch credentials,
TLS keys, and client certificates from a secret manager or externally-created
Kubernetes Secrets. Never copy real values into `.env.example`, this file, or
the OpenAPI document.
