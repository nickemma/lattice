# LATTICE

**A distributed search and retrieval platform — and the vehicle for every hard idea in distributed systems.**

`github.com/nickemma/lattice` · Project 2 · weeks 11–26

---

## What we're trying to achieve

Search is the excuse. The real subject is: **what happens to your data when machines fail, disagree, or disappear.**

Lattice ingests documents from a stream, indexes a million of them, and answers hybrid queries — keyword and vector together — across a cluster running on Kubernetes. It stays correct when a node dies mid-write. It degrades honestly when part of the cluster is unreachable, telling you the results are incomplete rather than silently lying.

That last sentence is the whole engineering thesis. Most systems, when they can only reach two of three shards, return two shards' worth of results and say nothing. Lattice says `"complete": false`.

**The claim it earns you:** *"I built and operated a stateful distributed system. I killed a data node during a segment merge and watched p99 for an hour. Here's the postmortem."*

---

## Why it's second, and why it gets sixteen weeks

It's the broadest employability project in the portfolio — SRE, platform, infrastructure, distributed systems, and data-infra roles all read it as directly relevant.

It's also where the theory you cannot fake lives. Storage engines, replication, consensus, sharding. An interviewer can tell within two questions whether you implemented Raft or read about it, and there is no way to bluff the difference.

Sixteen weeks because Raft alone is four of them, and compressing that produces someone who can say the word "consensus" without being able to reason about it.

---

## What it teaches — and when

### Phase 1 · Weeks 11–14 — How data survives a power cut *(S7)*

Before anything is distributed, one machine has to not lose your data. That's a harder problem than it sounds and it's the foundation everything else sits on.

Write-ahead logs. What `fsync` actually guarantees and what it doesn't. B-trees versus LSM trees and the read/write amplification tradeoff. Memtables, SSTables, compaction. Crash recovery. Then transactions: MVCC, isolation levels, what "repeatable read" actually permits.

**Built:** a single-node storage engine in Go. Append-only WAL, memtable, SSTable flush, compaction, recovery on restart. About 800 lines.

**Broken:** `kill -9` mid-write, a hundred times, verifying no acknowledged write is ever lost. Torn writes. A corrupted WAL entry. A disk that fills during compaction.

**Measured:** write amplification, read amplification, recovery time after a crash.

**Case study:** RocksDB, PostgreSQL's WAL.

### Phase 2 · Weeks 15–17 — What breaks when one becomes many *(S8)*

Start in plain English: three friends, one shared notebook. You write "Alice paid $20." Now all three notebooks must agree. What if one friend's phone is dead? What if a message arrives ten seconds late? What if two people write different things at once?

Then the terminology it earns: replication, leader and follower, clocks that disagree, happens-before, causality, partial failure, the eight fallacies, and CAP as an engineering constraint rather than a slogan.

**Built:** a replicated key-value store **with no consensus**, deliberately. Then we break it and watch it produce wrong answers. You need to feel the problem before the solution means anything.

**Broken:** network partition with writes on both sides. A follower that falls behind and serves stale reads. Two nodes that both believe they're the leader.

### Phase 3 · Weeks 18–21 — How machines that disagree decide *(S9)*

Raft. Four weeks, properly.

Leader election. Terms. Log replication. Persistence. Commit rules. Snapshots and log compaction. The safety argument, in plain English before it's in formal terms.

**Built:** Raft in Go, from the paper. Then killed repeatedly until it recovers every time.

**Broken:** kill the leader mid-replication. Partition the leader from the majority. Restart a node with a stale log. Deliver messages out of order. Run the test suite a hundred times, because Raft bugs are timing bugs and passing once means nothing.

**Measured:** leader election time across fifty kills, plotted as a distribution.

**Case study:** etcd → the Kubernetes control plane. After this phase you'll understand why `kubectl` sometimes hangs and what's happening when it does.

### Phase 4 · Weeks 22–23 — Splitting data and staying correct *(S10)*

Sharding. Consistent hashing. Rebalancing without downtime. Quorum reads and writes. Linearizability versus serializability — the distinction most engineers get wrong. Distributed transactions and why mature systems avoid two-phase commit.

**Built:** sharding and live reconfiguration on top of your Raft store.

**Measured:** throughput versus replica count. Latency during a shard migration.

### Phase 5 · Weeks 24–25 — Lattice itself, on Kubernetes *(S11)*

Now the actual product, built on everything above.

Go indexer: Kafka consumer → bulk indexing with backpressure and a dead-letter queue. Go query service: hybrid BM25 + kNN with reciprocal rank fusion, per-stage deadlines, partial results with honest coverage reporting. OpenSearch for the index — you'll understand its internals because you built a storage engine and a consensus module, and it runs on the same ideas.

StatefulSets, persistent volumes, pod disruption budgets, rolling upgrades of a stateful workload, Terraform, Argo CD.

**Data:** one million documents. A Wikipedia dump works.

**Measured:** p99 query latency **during a segment merge** — the number that proves you've actually operated this. Indexing throughput versus bulk size, and where the knee is. Cost per million documents indexed.

### Phase 6 · Week 26 — Chaos and hardening *(S12, S13)*

Observability first: histograms not averages, distributed tracing, SLIs and SLOs, error budgets, alerting on symptoms rather than causes, runbooks.

Then break it for real. Kill data nodes mid-indexing. Fill a disk. Inject 200ms latency and 5% packet loss between the query service and the cluster. Expire a certificate. Scale from three nodes to ten under load. Time a full restore from snapshot — actually time it, because almost nobody ever does until the day they need to.

Then harden it: mTLS everywhere, default-deny network policies, non-root containers with read-only root filesystems, secrets out of ConfigMaps, RBAC scoped down, audit logging on. Write the threat model.

**Shipped:** a real postmortem from something you broke, published.

---

## Done means

- [x] A storage engine that provably loses no acknowledged write across a hundred `kill -9` runs
- [x] Raft passing its full test suite a hundred consecutive times
- [x] Election-time distribution across fifty leader kills, plotted
- [x] One million documents indexed, hybrid search returning in a stated p99
- [x] p99 measured *during* a segment merge, not just at rest
- [x] Partial results reported honestly when shards are unreachable
- [ ] Deployed by Terraform and Argo CD, not by hand
- [ ] Survived the week-26 chaos day with documented behaviour for each failure
- [x] Snapshot restore timed and written into the runbook
- [x] `THREAT_MODEL.md` · `RUNBOOK.md` · `BENCHMARKS.md` · one published postmortem
- [ ] Ready to serve as Tessera's retrieval tier in week 44

---

## Roles this opens

Site Reliability Engineer · Infrastructure Engineer · Platform Engineer · Distributed Systems Engineer · Cloud Engineer · Data Infrastructure Engineer · Search Engineer · Backend Engineer at any database, storage, or streaming company.

This is the single highest-volume role surface in the portfolio. It's also the phase after which your system-design interviews get materially easier, because you'll be describing things you did rather than things you read.

---

## Repo layout

```
lattice/
├─ cmd/{indexer,query,latticectl}/
├─ internal/
│   ├─ app/  ├─ platform/
│   ├─ modules/
│   │   ├─ ingest/      {domain, ports, app, adapters}
│   │   ├─ index/
│   │   ├─ search/
│   │   └─ cluster/
│   └─ providers/       opensearch, kafka
├─ pkg/
│   ├─ wal/             the storage engine
│   ├─ lsm/
│   └─ raft/            the consensus module
├─ deploy/{k8s,terraform,argocd}/  ├─ chaos/  ├─ bench/
└─ docs/
```

`pkg/wal`, `pkg/lsm`, and `pkg/raft` are deliberately separable. They're the from-scratch cores, and they should be readable on their own — those three directories are what a distributed-systems interviewer will actually open.

---

## The one thing to get right

Not search quality. **Honest degradation.**

Any system can return results when everything works. Lattice's thesis is that when it can only reach part of the cluster, it says so — in the response, in the metrics, and in the alert. Build that in from the first commit rather than adding it later, because a system that lies quietly under partial failure is worse than one that fails loudly.
