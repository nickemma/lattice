# RPD — LATTICE v1

**System:** LATTICE — Distributed Search & Retrieval Platform
**Owner:** Nicholas Emmanuel
**Status:** Building — Compose path verified; production-scale evidence pending
**Date:** August 2026

**Roadmap scope:** [`docs/lattice.md`](lattice.md) is the authoritative six-phase roadmap. This RPD defines the Phase 5 search product and Phase 6 operational slice, including the developer-facing Swagger UI and playground described in [`docs/walkthrough.md`](walkthrough.md).

---

## 1. Problem

Search is easy to stand up and hard to operate. A `helm install` gets you a cluster; it does not get you answers to the questions that arrive on day thirty:

- Query p99 tripled overnight and nothing was deployed. Why?
- A data node was replaced. How many queries returned incomplete results during the window, and did anyone know?
- The index needs rebuilding with a new analyzer. How, without downtime?
- We have been snapshotting to S3 for six months. Has anyone restored one?
- What does this cost per million documents, and what would managed cost instead?

Each of those is a normal Tuesday for anyone operating search in production, and none of them is answered by the tutorial that got the cluster running.

## 2. Goal

Operate a hybrid search platform over 1M+ documents to a stated SLO, with capacity, cost, failure behaviour, and restore time all measured and published.

## 3. Non-goal: why not just buy

| Option | When it wins | Why we still build this |
|---|---|---|
| Elastic Cloud / OpenSearch Serverless | Small team, no ops appetite, standard workload | The operational skill *is* the deliverable; and cost crosses over at sustained volume |
| Algolia / Typesense | Site search, simple relevance, fast setup | No control over sharding, hybrid fusion, or the latency envelope |
| pgvector on existing Postgres | Small corpora, already running Postgres | Does not scale to this corpus with keyword + vector fused |
| Dedicated vector DB (Qdrant, Weaviate) | Vector-only workloads | Would require a second system kept consistent with the keyword index |

**The cost crossover is a deliverable.** `docs/benchmarks.md` publishes measured self-hosted cost against managed pricing at the date of measurement. If managed wins at this scale, that is published too.

## 4. Users

| Role | Needs |
|---|---|
| **Application developer** | A search API with predictable latency and honest partial-result reporting |
| **Data producer** | To publish documents and know when they became queryable, or why they did not |
| **Platform engineer** | To scale, upgrade, and restore the cluster without downtime |
| **On-call** | To diagnose "search is slow" from a dashboard in under five minutes |

## 5. User stories

- As a **developer**, I want hybrid keyword and semantic results in one call so that I do not fuse two systems in application code.
- As a **developer**, I want to pass a deadline and get partial results rather than a timeout, so that my page renders.
- As a **developer**, I want the response to tell me coverage was incomplete, so that a degraded result is visible rather than silently wrong.
- As a **data producer**, I want a failed document to land in a dead-letter queue with its error, so that nothing disappears silently.
- As a **platform engineer**, I want index templates and lifecycle policies in git, so that cluster state is reviewable and reproducible.
- As a **platform engineer**, I want to rehearse a restore on a schedule, so that the backup is known to work rather than assumed to.
- As **on-call**, I want query latency broken down by phase, so that I can tell a slow merge from a slow embedding call.

## 6. Functional requirements

**Ingest**
1. Consume documents from a Kafka topic with at-least-once delivery.
2. Generate embeddings in batches for the vector field.
3. Bulk-index with bounded concurrency and backpressure — the indexer must never overwhelm the cluster.
4. Retry transient failures with backoff; dead-letter permanent failures with the error attached.
5. Expose ingest lag per partition.
6. Support full reindex into a new index and an atomic alias swap, with no query downtime.

**Query**
7. Expose a search API accepting query text, filters, pagination, and a deadline.
8. Execute BM25 and kNN branches and fuse via reciprocal rank fusion.
9. Return whatever completed inside the deadline; never exceed it.
10. Report coverage (shards queried, shards answered, complete) in every response.
11. Report per-stage timings in every response.
12. Cap pagination depth and require cursor-based paging past the threshold.
13. Cache repeated queries with a bounded TTL.

**Developer experience**
14. Serve an OpenAPI specification and interactive Swagger UI for the public API.
15. Provide a browser playground that exercises document publishing, search, coverage, timings, and safe local failure drills.

**Operations**
16. Provision cluster and node pools via Terraform.
17. Manage index templates, ISM policies, and application config via Argo CD from git.
18. Snapshot to S3 on a schedule; alert on snapshot age.
19. Rehearse restore on a schedule and record the elapsed time.
20. Expose the metrics listed in the README.
21. Run a chaos suite in CI: node kill, network partition, merge under load.

## 7. Acceptance criteria

**Deadlines and coverage**
> Given a query with a 150ms deadline and one unreachable shard
> When the deadline expires
> Then results from responding shards are returned within 150ms, with `complete: false` and the shard counts in the body.

> Given a query whose vector branch exceeds the deadline
> When results are assembled
> Then BM25 results are returned alone, inside the deadline, with the degradation reported.

**Ingest durability**
> Given a document that fails indexing permanently (malformed field)
> When retries are exhausted
> Then it lands in the DLQ with the source document and the error, and the partition continues.

> Given the indexer is killed mid-batch
> When it restarts
> Then it resumes from the last committed offset with no document lost and no duplicate document in the index.

**Backpressure**
> Given the cluster rejects bulk requests (queue full)
> When the indexer receives rejections
> Then it slows consumption rather than retrying immediately, and cluster rejection count returns to zero without operator action.

**Zero-downtime reindex**
> Given a live index serving queries
> When a full reindex into a new index completes and the alias is swapped
> Then no query returns an error during the swap, and post-swap results reflect the new mapping.

**Node loss**
> Given a three-node data tier under query load
> When one node is killed
> Then queries continue, coverage degradation is reported and metered, and the cluster returns to green within the recovery target.

**Restore**
> Given a snapshot in S3
> When a restore is performed into an empty cluster
> Then the index is queryable and document count matches, and the elapsed time is recorded in the benchmark report.

**Merge behaviour**
> Given sustained indexing triggering segment merges
> When queries run concurrently
> Then p99 stays within the stated merge-window target, or the deviation is documented with the merge policy that caused it.

**Developer experience**
> Given the service is ready
> When a developer opens Swagger UI or the playground
> Then they can publish test documents, run hybrid searches, inspect coverage and timings, and reproduce the same requests with the documented API contract.

## 8. Non-functional requirements

| Property | Target |
|---|---|
| Corpus | ≥ 1,000,000 documents |
| Query latency | p99 < 200ms; p99 < 400ms during merge |
| Ingest freshness | p95 < 60s from publish to queryable |
| Availability | 99.9% query availability / 30 days |
| Durability | No silent document loss — DLQ or indexed, never neither |
| Recovery | Cluster green within 5 minutes of single-node loss |
| Reproducibility | Cluster rebuildable from Terraform + git with no manual steps |

## 9. Out of scope for v1

- Web-scale crawling (ingest sources are bounded and named)
- Multi-region replication
- Learning-to-rank and personalised relevance
- Query autocomplete and spell correction
- Non-English analyzers
- A public-facing search UI; the developer Swagger UI and testing playground are in scope
- RAG orchestration — that is [TESSERA](https://github.com/nickemma/tessera)

## 10. Success metrics

- 1M+ documents indexed, queryable at p99 under 200ms
- A published capacity report with measured cost per million documents and per million queries
- A timed restore drill in the report
- Chaos suite green in CI, including node kill under load
- A second engineer diagnoses a seeded "search is slow" incident using only the runbook

## 11. Build order

| # | Increment | Requirements | Done when |
|---|---|---|---|
| 1 | Terraform cluster + OpenSearch operator | 16 | Cluster green, reachable, rebuildable from scratch |
| 2 | Index template + mapping in git, Argo-reconciled | 17 | Changing a mapping is a pull request |
| 3 | Direct bulk indexing, 10k docs, no pipeline | 3 | Docs queryable via OpenSearch directly |
| 4 | **Baseline benchmark** | — | BM25 p99 at 10k docs recorded — the number everything is compared against |
| 5 | Kafka + indexer with offsets and backpressure | 1, 4, 5 | Kill the indexer mid-batch; resume, no loss, no duplicates |
| 6 | Embeddings + kNN field | 2 | Vector search returns sensible neighbours |
| 7 | Query service: hybrid fusion, deadlines, coverage | 7–11 | `complete: false` observed under a killed shard |
| 8 | Scale corpus to 1M, retune shards | — | Shard sizing decision documented with the measurement behind it |
| 9 | ISM lifecycle + snapshots + **restore drill** | 18, 19 | Restore completed and timed |
| 10 | Zero-downtime reindex + alias swap | 6 | Mapping changed under live query load, zero errors |
| 11 | Result cache + pagination guards | 12, 13 | Deep-pagination query rejected; cache hit rate measured |
| 12 | Full observability + dashboards | 20 | Every README metric live in Grafana |
| 13 | Chaos suite in CI | 21 | Node kill and partition run automatically, assertions hold |
| 14 | Capacity + cost report | — | `docs/benchmarks.md` complete, no estimated cells |

Row 4 exists for the same reason it does in TESSERA: without a pre-platform baseline you cannot attribute later latency to anything. Row 9 is the row most projects skip and the one that most distinguishes an operator from a tutorial-follower.
