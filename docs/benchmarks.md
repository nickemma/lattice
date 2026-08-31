# LATTICE Benchmarks

This report is intentionally a measurement log. Empty cells are not estimates.

## Required measurements

| Measurement | Target | Result | Environment | Date |
|---|---:|---:|---|---|
| Corpus size | ≥ 1,000,000 docs | — | — | — |
| Query p50 | < 50ms target | — | — | — |
| Query p99 | < 200ms target | — | — | — |
| Query p99 during segment merge | < 400ms target | — | — | — |
| Sustained query throughput | — | — | — | — |
| Sustained indexing throughput | — | — | — | — |
| Single-node recovery | < 5 minutes target | — | — | — |
| Snapshot restore time | — | — | — | — |
| Cost per million documents | — | — | — | — |
| Cost per million queries | — | — | — | — |
| Local hybrid search benchmark, 10k docs | — | 89.96ms/op | AMD Ryzen 7 PRO 5850U, Go benchmark, 16 workers | 2026-08-30 |

## Method

Each run records the commit, corpus source, document size distribution, embedding model and dimensions, shard/replica count, node sizes, storage type, query mix, concurrency, warm-up period, and measurement duration.

The recorded local baseline above is a development signal only: it uses the deterministic in-process backend and is not evidence for the OpenSearch one-million-document SLOs.

Latency is reported as histograms and percentiles. Averages are not used to make SLO claims. Merge-window measurements run indexing and querying concurrently while segment-merge duration is recorded beside query p99.

## Cost model

```text
cost_per_million_docs =
  (node_hours × hourly_rate + storage_gb_months × rate)
  / docs_indexed × 1e6

cost_per_million_queries =
  (node_hours × hourly_rate)
  / queries_served × 1e6
```

Idle provisioned capacity is included. Managed-service comparison prices include their measurement date.
