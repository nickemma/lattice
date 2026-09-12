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

**Pass condition:** see [Scoring](#scoring) — at least one configuration must
isolate the availability path, and every error anywhere must be accounted for by
a deadline expiry. Until such a run exists, the headline claim is unsupported.

Artifacts: `opensearch-baseline.json`, `opensearch-shard-unavailable.json`,
`opensearch-recovered.json`.

#### Result (commit `c0a724f`, 2026-09-12)

OpenSearch 2.17.1, 3 data nodes at 256mb heap, 3 primary shards, 0 replicas,
20,000 documents, 17 primary segments, 200ms deadline, 5,000 searches per
configuration.

With `opensearch-3` stopped the cluster went **red with 2 data nodes**, the API
kept serving throughout, and the search API reported `_shards total=3
successful=2`. The headline configuration:

| `phrase`, concurrency 4 | Baseline | Node stopped | Recovered |
|---|---:|---:|---:|
| Completed | 5,000 | 5,000 | 5,000 |
| Incomplete | 0 | **5,000** | 0 |
| Degraded | 0 | **0** | 0 |
| With errors | 0 | **0** | 0 |
| Reason | `complete` | `shard_unavailable` | `complete` |

Five thousand searches against a cluster missing a third of its data: every one
returned results, every one said it was incomplete, **none carried an error**.
The node was restarted, the cluster returned to green, and the numbers returned
to baseline.

Three compound configurations held the invariant exactly:

| Configuration | Error responses | Deadline expiries |
|---|---:|---:|
| phrase, c=8 | 7 | 7 |
| rare, c=4 | 10 | 10 |
| rare, c=8 | 2 | 2 |

Five configurations were excluded as saturated. Note `common` c=8's fault run was
itself perfect — 5,000 `shard_unavailable`, 0 errors — but its *baseline* carried
14 deadline expiries, so the verifier excluded it anyway. That is the scoring
erring in the conservative direction, which is the direction it should err in.

**Limitation, stated plainly: only one configuration was pure.** The claim rests
on that one plus three compound ones. This is a consequence of running a
CPU-capped cluster on a shared laptop; a faster cluster or a longer deadline
would yield several. It is sufficient but it is not a broad sweep, and it should
not be described as one.

#### The same fault, opposite sign, on the two backends

Worth putting beside each other, because it is the sharpest argument for why
every table needs a backend column:

| | In-process | OpenSearch |
|---|---:|---:|
| Baseline p99 | 2.00ms | 195.35ms |
| Shard-unavailable p99 | 86.56ms | 102.79ms |
| Effect of losing a shard | **43x slower** | **1.9x faster** |

Both are correct. On OpenSearch a missing shard means a third less data to score
and this overlay has no Redis, so degraded mode is genuinely cheaper. In-process,
the incomplete response became uncacheable and losing a 100% cache hit rate
swamped the saving.

The *sign of the effect flips between backends*. Quoting one backend's degraded
latency as though it characterised the other would be straightforwardly wrong,
and nothing but the `backend` and `fault_source` fields stops that happening.

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

#### Result (commit `c0a724f`, 2026-09-12)

5,000 searches per configuration, 3 shards, 5,000 documents, 200ms deadline,
concurrency 4/8/16, three query classes.

| | Baseline | Shard removed |
|---|---:|---:|
| Completed | 5,000 | 5,000 |
| `complete: false` | 0 | 5,000 |
| `degraded: true` | 0 | 0 |
| With `errors` | 0 | 0 |
| Served from cache | 5,000 | 0 |
| `reason` | `complete` x 5,000 | `shard_unavailable` x 5,000 |

**5 of 9 configurations were pure** — incomplete with zero errors, which is the
pass condition. **4 were compound**, having also expired the deadline under
load, and in every one the invariant held to the response:

| Configuration | Error responses | Deadline expiries |
|---|---:|---:|
| common, c=16 | 1203 | 1203 |
| rare, c=16 | 768 | 768 |
| phrase, c=4 | 16 | 16 |
| phrase, c=16 | 1370 | 1370 |

Every error was accounted for by a deadline. None was an availability loss.

p99 (median +/- IQR, n=5, ms):

| Class | Baseline c=4 | Fault c=4 | Baseline c=16 | Fault c=16 |
|---|---:|---:|---:|---:|
| common | 2.00 +/- 0.33 | 86.56 +/- 5.58 | 4.30 +/- 0.29 | 288.45 +/- 22.36 |
| rare | 1.74 +/- 0.20 | 55.72 +/- 1.13 | 4.35 +/- 0.32 | 227.05 +/- 1.03 |
| phrase | 1.52 +/- 0.12 | 77.28 +/- 87.40 | 5.25 +/- 0.30 | 267.32 +/- 6.31 |

#### The two failure classes have different latency signatures

This is the most useful part of the result, because it lets a reviewer check the
classification without trusting the label. p99 for the `rare` query class, median
+/- IQR over 5 runs:

| Concurrency | Shard unavailable | Deadline stall |
|---:|---:|---:|
| 4 | 55.72 +/- 1.13 | 206.86 +/- 1.05 |
| 8 | 128.16 +/- 5.79 | 207.09 +/- 0.55 |
| 16 | 227.05 +/- 1.03 | 219.20 +/- 28.00 |

**Availability-driven incompleteness scales with offered load** — 56 -> 128 ->
227ms — because the surviving shards absorb the work and queue as concurrency
rises.

**Deadline-driven incompleteness is flat and pinned at the budget** — 207 -> 207
-> 219ms, with an IQR under 1ms at low load — because the response leaves when
the clock says so, regardless of what is happening behind it.

Two different mechanisms produce the same `complete: false`, and they do not
look alike in the data. An independent check on the `reason` field is therefore
available to anyone who doubts it: the shapes have to match the labels.

#### One confound, stated rather than buried

The baseline is *faster* than either fault scenario, and not because losing a
shard is expensive. Only a complete, non-degraded response is cacheable, so the
baseline serves most queries from cache while neither fault scenario can cache
at all. A degraded run is also an uncached run. `cache_hits` is recorded per run
so this is visible in the artifact; compare fault scenarios against each other,
not against a cache-warm baseline.

---

## Scoring

`scripts/verify_partial_results.py` is shared by both partial-results
experiments so they cannot drift. It compares like-for-like, per (query class,
concurrency) configuration, and labels each one:

| Label | Meaning | Counts toward the claim |
|---|---|---|
| **saturated** | The *baseline* was already incomplete under offered load alone | No — excluded and reported as excluded |
| **pure** | The fault registered; no deadline expired | Yes — this is the claim |
| **compound** | The fault registered *and* the deadline expired | Invariant only |

Checked per usable configuration:

1. `incomplete_responses > 0` — the fault registered at all.
2. `coverage_reasons["shard_unavailable"] > 0` — classified as availability loss.
3. `api_error_responses == coverage_reasons["deadline"]` — **the core invariant.**
   Every error is accounted for by a deadline, which is the precise statement
   that no availability-classified response carries an error. If coverage and
   errors were still welded together, every incomplete response would carry an
   error and this equality would break immediately.
4. `degraded_responses == coverage_reasons["deadline"]` — same for degradation.

And once globally: **at least one configuration must be pure.** Without it,
"partial results with no error" was never demonstrated in isolation and the run
fails.

### Why it is not simply "degraded == 0"

That was the first version, and real data showed it wrong. Removing a shard also
removes cacheability, so the fault run does real work on every query; at higher
concurrency that alone exceeds the deadline. Demanding `degraded == 0` therefore
fails on behaviour that is correct — and a check that fails on correct behaviour
invites explaining the failure away, which is worse than having no check. The
invariant above is the honest form: it permits compound failure while still
being falsified the instant an availability loss is reported as an error.

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
