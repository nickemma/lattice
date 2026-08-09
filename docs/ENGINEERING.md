# LATTICE — Engineering Design

**Companion to:** `RPD.md` (what and why). This document is *how*.

---

## 1. Build vs buy

| Concern | Decision | Reasoning |
|---|---|---|
| Inverted index, BM25, kNN | **Buy — OpenSearch** | Lucene underneath. Decades of relevance and merge-policy work. Keyword and vector in one cluster avoids keeping two systems consistent. |
| Cluster orchestration | **Buy — OpenSearch K8s Operator** | Rolling upgrades, scaling, and config as CRDs. Hand-rolled StatefulSet management is where data gets lost. |
| Ingest log | **Buy — Kafka / Redpanda** | Replay, partitioning, consumer offsets, and backpressure semantics for free. Rebuilding an index means resetting an offset. |
| Provisioning | **Buy — Terraform** | Cluster, node pools, storage classes, IAM. |
| Delivery | **Buy — Argo CD** | Index templates and ISM policies reconciled from git. |
| **Indexer** | **Build — Go** | Batching strategy, backpressure response, DLQ policy, and embedding coordination are workload-specific. |
| **Query service** | **Build — Go** | Hybrid fusion, per-stage deadlines, and coverage reporting are not features of OpenSearch's own API. |
| **Operational apparatus** | **Build** | SLOs, chaos suite, capacity model, runbook. This is the deliverable. |

**The line:** OpenSearch owns the data; we own the pipeline into it, the contract out of it, and the operations around it.

---

## 2. Why OpenSearch rather than a vector database

The workload is hybrid: keyword relevance matters for exact terms and names, semantic similarity matters for paraphrase. Two options:

**A. OpenSearch with a kNN field.** One cluster, one write path, one consistency story. Fusion happens over two branches of the same query. Vector performance is good, not best-in-class.

**B. OpenSearch + a dedicated vector DB (Qdrant, Weaviate).** Better vector performance, and a second system that must be kept consistent with the first — dual writes, divergent failure modes, two restore procedures, two capacity models.

**Chosen: A.** At 1M documents, option B's vector advantage does not pay for a distributed consistency problem in the ingest path. The threshold where that flips is a real number and is stated in the benchmark report, so the decision is revisitable rather than permanent.

---

## 3. Sharding — the decision that cannot be undone cheaply

Primary shard count is fixed at index creation. Getting it wrong means a full reindex.

The heuristics that apply here:

| Consideration | Position |
|---|---|
| Shard size | Target 10–30GB per shard. Smaller wastes overhead; larger slows recovery and merges |
| Shard count vs node count | A multiple of data nodes, so shards distribute evenly |
| Over-sharding | Each shard is a Lucene index with its own memory and file handles. "Extra shards for future growth" is the most common self-inflicted latency problem |
| Query fan-out | Every shard is queried on every search. More shards means more parallelism *and* more tail exposure — the slowest shard sets p99 |
| Replicas | Minimum 1 for availability. Replicas also serve reads, so they are throughput as well as safety |

**The decision is made at row 8 of the build order, after measuring at 1M documents — not guessed at row 1.** Rows 1–7 run on a deliberately small index that is expected to be reindexed. The measurement and reasoning go in `docs/adr/`, because "why is this 6 shards" is the question a future operator will ask.

---

## 4. Ingest: backpressure is the design

The indexer's job is to go exactly as fast as the cluster can absorb, and no faster.

```
Kafka partition
   → batch accumulator (size OR time trigger)
   → embedding call (batched)
   → _bulk request
   → inspect per-item responses
       ├─ success       → advance offset
       ├─ 429 / rejected → slow down, retry with backoff, do NOT advance
       └─ 4xx permanent  → DLQ with error, advance offset
```

**Bulk responses are per-item.** A `200` on the bulk request does not mean every document indexed — individual items carry their own status. An indexer that checks only the HTTP status silently loses documents, and this is the single most common OpenSearch ingest bug.

**Backpressure comes from consumer pause, not from retry.** When the cluster rejects, the correct response is to stop consuming from Kafka — not to retry in a tight loop, which converts cluster pressure into cluster collapse. Kafka is the buffer; that is what it is for.

**Offsets advance only after confirmed outcome** (indexed or dead-lettered), which is what makes the "kill it mid-batch" acceptance test pass. At-least-once delivery plus a deterministic document ID gives idempotent indexing — a redelivered document overwrites itself rather than duplicating.

**Refresh interval is a tuning decision with a tradeoff:** shorter means fresher search and more segments to merge; longer means better ingest throughput and staler results. It is set from the ingest-freshness SLO, not from a default.

---

## 5. Query: deadlines and honest coverage

```go
// Both branches run under one budget. Whatever is ready at expiry is what ships.
ctx, cancel := context.WithTimeout(ctx, req.Deadline)
defer cancel()

bm25 := goSearch(ctx, keywordQuery)
vec  := goSearch(ctx, knnQuery)

results, coverage := fuse(collectReady(ctx, bm25, vec))
```

**Reciprocal rank fusion** rather than score normalisation: BM25 scores and cosine similarities are not on comparable scales, and normalising them requires a corpus-dependent constant that drifts. RRF uses rank position only, needs no tuning, and is robust — it is the standard choice for exactly this reason.

**Coverage is computed, not assumed.** OpenSearch reports `_shards.successful` and `_shards.total` per response; the query service surfaces those rather than discarding them. This is the mechanism behind `"complete": false`.

**Pagination is bounded.** Deep `from`/`size` paging makes every shard sort and return `from + size` documents — at `from=10000` that is a cluster memory incident. Past a threshold, `search_after` is required. The cap is enforced at the API, not documented as a guideline.

---

## 6. Zero-downtime reindex

Mappings are largely immutable. Changing an analyzer means a new index.

```
1. Create index_v2 with the new mapping
2. Reindex from index_v1 (or replay Kafka from offset 0 — preferred: proves the pipeline)
3. Dual-write new documents to both while catching up
4. Verify: document counts match, sample queries return comparable results
5. Atomically swap the alias:  search → index_v2
6. Retain index_v1 for the rollback window, then drop
```

Clients only ever address the alias. Step 2 preferring a Kafka replay over `_reindex` is deliberate: it exercises the real ingest path, so a bug in the pipeline is found during the migration rather than after it.

---

## 7. What actually causes incidents

Three things, in order of how often they are the answer:

**Segment merges.** Lucene merges segments continuously. A large merge consumes IO and CPU and raises query latency with no deployment to blame. Mitigations: throttled merge policy, force-merge only on read-only indices, and `opensearch_merge_duration_seconds` on the dashboard next to query p99 so the correlation is visible rather than deduced.

**JVM heap pressure.** Heap exhaustion produces GC pauses that present as network timeouts. Circuit breakers reject expensive queries before OOM — a rejected query is better than a dead node. Alert on heap ratio, not on the resulting symptom.

**Unbounded queries.** Deep pagination, huge aggregations, and wildcard-leading terms are the usual culprits. Guard at the API layer; the cluster's own breakers are the last line, not the first.

Each of these gets a runbook section with the signal, the confirmation query, and the action.

---

## 8. Restore is a feature, not a backup

The snapshot is not the deliverable. **The measured restore is.**

- Snapshots to an S3 repository on a schedule
- `lattice_snapshot_age_seconds` alerts on *age*, not on failure — a snapshot job that silently stopped is the dangerous case, and a failure alert never fires for a job that is not running
- A restore drill runs on a schedule into a scratch cluster: restore, verify document count, run sample queries, record elapsed time
- The elapsed time is published in `docs/benchmarks.md` and is the input to any realistic RTO claim

---

## 9. Cost model

```
cost_per_million_docs    = (node_hours × hourly_rate + storage_gb_months × rate) / docs_indexed × 1e6
cost_per_million_queries = (node_hours × hourly_rate) / queries_served × 1e6
```

Published alongside managed-service pricing at the date of measurement, with the date stated because vendor pricing moves. Idle capacity is included — a cluster provisioned for peak and running at 15% has a high cost per query, and hiding that would defeat the purpose.

---

## 10. Open questions

1. **Corpus source.** 1M documents from where? Common Crawl segment, Wikipedia dump, or an internal corpus. Wikipedia is the pragmatic choice — real text, real term distribution, legally clean, and reproducible by anyone reading the benchmark.
2. **Kafka or Redpanda?** Redpanda is materially lighter to operate single-node. Kafka is what appears in job descriptions. Lean Kafka for that reason alone, unless operating it eats the project.
3. **Where does the cluster run?** Managed Kubernetes with real node pools, or local (kind/k3s) with the honest caveat that capacity numbers reflect the hardware tested. Either is publishable; the hardware must be stated.
4. **Embedding model and dimensionality.** Dimension affects index size and kNN memory materially. Pick one, state it, and include index size per million documents in the report.
