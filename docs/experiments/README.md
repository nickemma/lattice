# Experiments

Each experiment here is a script with a pass condition written into it. The
script fails loudly if the thing it claims to demonstrate did not happen, so an
artifact in this directory means the check passed, not that a command ran.

Every artifact records, in `environment`:

| Field | Why it is mandatory |
|---|---|
| `backend` | `in-memory` or `opensearch`. The two are different systems and their numbers are not interchangeable. |
| `fault_source` | `none`, `injected-in-process`, or `induced-in-cluster`. An injected fault is not an observed node failure. |
| `scenario` | What this run measured. |
| `commit` | The build the numbers came from. |
| Cluster shape | Node count, heap, shards, replicas, OpenSearch version, segment count. |

Anything that could not be collected is listed under `environment.unavailable`
rather than omitted, so a reader can tell a zero from a value that was never
gathered.

---

## 1. Partial results — the headline claim

The claim under test: **when part of the cluster is unreachable, LATTICE
returns usable partial results and says so — and that is distinguishable from a
deadline expiring.**

This needed an experiment because the earlier benchmark runs never produced it.
In those runs, completeness was *defined* to include "no errors occurred", so
every incomplete response also carried an error and the availability path was
never exercised. The counts of incomplete and error responses were identical in
all four runs — 56/56, 93/93, 64/64, 24/24 — which was an identity in the code,
not a property of the system.

### 1a. On a real cluster — `scripts/partial-results-opensearch.sh`

```bash
make multinode-up
make experiment-partial-opensearch
make multinode-down
```

A three-data-node OpenSearch cluster with **zero replicas**. Zero replicas is
the whole design of the experiment: with a replica available OpenSearch routes
around a stopped node and coverage correctly stays complete, which measures the
wrong thing. With none, one stopped node means one shard has no copy anywhere.

| Scenario | Setup |
|---|---|
| `baseline` | Three data nodes, everything answers |
| `shard-unavailable` | One data node stopped with `docker compose stop`; API stays up |
| `recovered` | Node restarted, cluster back to green |

**Pass condition:** in the `shard-unavailable` scenario,
`incomplete_responses > 0` **and** `api_error_responses == 0`. Until that run
exists, the headline claim is unsupported.

Artifacts: `opensearch-baseline.json`, `opensearch-shard-unavailable.json`,
`opensearch-recovered.json`.

### 1b. Controlled comparison — `scripts/partial-results-local.sh`

```bash
make experiment-partial-local
```

Self-contained: builds, starts the in-process backend, seeds, measures,
verifies. Faults are injected through `/v1/debug/shards`, so
`fault_source` is `injected-in-process` in every artifact it writes.

| Scenario | Setup | Expected |
|---|---|---|
| `baseline` | All three shards answer | Clean |
| `shard-unavailable` | Shard removed from the fan-out | Incomplete, **no** errors, `reason: shard_unavailable` |
| `deadline` | Shard stalled past the budget | Incomplete, **with** errors, `reason: deadline` |

The two fault scenarios are the point: they both produce `complete: false`, and
they must not look the same.

Artifacts: `local-baseline.json`, `local-shard-unavailable.json`,
`local-deadline.json`.

#### One confound, stated rather than buried

The baseline is *faster* than either fault scenario, and not because losing a
shard is expensive. Only a complete, non-degraded response is cacheable, so the
baseline serves most queries from cache while neither fault scenario can cache
at all. A degraded run is also an uncached run. `cache_hits` is recorded per run
so this is visible in the artifact; compare fault scenarios against each other,
not against a cache-warm baseline.

---

## 2. Segment count against tail latency — `scripts/segment-latency.sh`

```bash
# with a stack running and a declared corpus already loaded
make experiment-segments
```

This started as an accident. Across the 1M-document runs, p99 moved
191ms → 95ms → 49ms as segments were merged, while p50 barely moved
(16.7 → 24.7 → 18.0). Segment count was dominating tail latency and almost
nothing else — a real, explainable result sitting in three incidental JSON files
with no experiment around it.

The script force-merges down through a series of targets and, at each point,
records:

- the **actual** segment count from the `_segments` API, not the requested one;
- p50/p95/p99 over repeated runs, with an IQR;
- **BM25 and kNN branch timings separately**, so the sensitivity can be
  attributed to one branch rather than to "the query". If the tail belongs to
  the kNN path, that is a sharper finding than the aggregate;
- the wall-clock **cost of the merge** that produced the point.

The cost column is not optional. Force-merge is expensive and one-way. A result
that says "p99 fell 4×" without saying "for 360 seconds of merge you cannot run
continuously in production" is incomplete.

The script warns if fewer than four distinct segment counts were reached: below
that it is an anecdote, not a curve.

Artifacts: `segment-latency-*.json` plus `segment-latency-index.json`, a flat
table of `(segments, class, p50, p99, IQR, bm25_p99, knn_p99, merge_seconds)`
ready to plot.

---

## Reading an artifact

```bash
# Did the availability path get exercised?
jq '[.configurations[].runs[] |
     {incomplete: .incomplete_responses,
      errors: .api_error_responses,
      reasons: .coverage_reasons}]' \
  docs/experiments/opensearch-shard-unavailable.json

# Headline number with its dispersion, never bare.
jq '.configurations[] |
    {class: .query_class, concurrency,
     p99_median: .aggregate.p99_ms.median,
     p99_iqr: .aggregate.p99_ms.iqr,
     n: .aggregate.p99_ms.samples}' \
  docs/experiments/opensearch-baseline.json

# What produced these numbers?
jq '.environment' docs/experiments/opensearch-baseline.json
```

Any `aggregate` entry with `"samples": 1` has no dispersion behind it and must
not be quoted as a headline.
