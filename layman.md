# LATTICE in plain language

This is the version you can explain to someone over coffee, and then defend
when they push back. No jargon is used without being unpacked first.

Read it top to bottom the first time. After that, [Part 6](#part-6--the-hard-questions-and-honest-answers)
is the part worth rereading before you present.

---

## Part 1 — What problem is this solving?

### The one-sentence version

> LATTICE is a search engine for a very large pile of documents that keeps
> answering — and tells you honestly what it could not reach — when part of the
> machinery breaks.

### The slightly longer version

Imagine a library with a million books, spread across three buildings.

Someone asks: *"find me everything about how a group of computers agrees on a
decision."*

Three things have to happen:

1. **Find matches by words.** Books with "consensus" and "agreement" in them.
2. **Find matches by meaning.** A book titled *"Raft leader election"* never
   uses the word "agreement", but it is exactly what was asked for.
3. **Come back fast, even if a building is on fire.** If building 3 is
   unreachable, do you (a) wait forever, (b) return an error, or (c) return
   what buildings 1 and 2 have, and clearly say building 3 is missing?

LATTICE does (c). **That is the whole point of the project.** Everything else is
supporting machinery.

### Why (c) is the interesting answer

Most systems do (a) or (b), and both are worse than they look:

- **(a) Wait forever** — one broken building makes the whole library appear
  broken. One slow component takes down everything that depends on it.
- **(b) Return an error** — you had 2/3 of the answer and threw it away. The
  user gets nothing when they could have had most of what they wanted.
- **(c) Return partial results, labelled** — the user gets 2/3 of the answer
  *and knows it is 2/3*. They decide whether that is good enough.

The danger with (c) is the one thing you must never do: return 2/3 of the
answer and let it **look** like the whole answer. A silently incomplete answer
is worse than an error, because nobody knows to distrust it.

So every LATTICE search response carries a `coverage` block that says exactly
how much of the library was reached.

---

## Part 2 — The five moving parts

Follow one document from arrival to search result.

### 1. Something publishes a document

A document is just `{id, title, body, tags}`. It arrives over HTTP.

### 2. It goes into a queue (Kafka)

Rather than writing straight into the search index, the document is put on a
queue. **Why:** if the search index is slow, down, or being rebuilt, documents
pile up in the queue instead of being lost. The queue is the shock absorber.

### 3. An indexer takes it off the queue

A small program reads from the queue in batches, and for each document:

- computes an **embedding** — a list of 64 numbers representing the document's
  *meaning*. Documents about similar things get similar numbers. This is what
  makes search-by-meaning possible.
- writes the document and its numbers into the search index.

Only **after** the write succeeds does it tell the queue "I'm done with that
one." **Why:** if the indexer crashes mid-batch, the queue redelivers those
documents. Nothing is lost. If a specific document can never be indexed (it's
malformed), it goes to a **dead-letter queue** — a side bin — so one bad
document cannot block the other million.

### 4. The search index stores it (OpenSearch)

The index is split into three **shards** — three roughly equal piles. A search
asks all three piles and combines the answers. This is the "three buildings" of
the library analogy.

### 5. Someone searches

The query service does something slightly clever. It runs **two searches at
once**:

- **Keyword search (BM25):** classic word matching. Rare words count for more
  than common ones — matching "sstables" is more informative than matching
  "the".
- **Vector search (kNN):** converts the query into those same 64 numbers, and
  finds documents whose numbers are closest. This is the search-by-meaning
  half.

Then it **fuses** the two ranked lists. The method is *reciprocal rank fusion*,
which is simpler than it sounds: each list votes, a document's vote is worth
`1/(60 + its position)`, and the votes are added up. A document ranked #1 by
keywords and #2 by meaning beats one ranked #1 by keywords and #400 by meaning.

The reason to fuse by *rank* rather than by *score* is that the two searches
produce scores on completely different scales. Positions are comparable;
raw scores are not.

Both halves run under **one shared deadline** — by default 200 milliseconds.
When the clock runs out, whatever has arrived is fused and returned, with
coverage saying what was missing.

### The whole flow

```
publish → queue → indexer → embeddings → search index
                                              ↓
                    search ← fuse ← keyword search + meaning search
                                              ↓
                              results + coverage + timings
```

---

## Part 3 — The idea worth defending

Almost any search engine can do keyword search, meaning search, and fusion.
That part is table stakes.

The part worth defending is **coverage**, and specifically this: LATTICE treats
"how much did I reach?" and "did anything break?" as **two separate questions**.

### Why two questions and not one

Consider two very different bad days:

- **Day A:** a machine holding one shard is switched off. Every remaining
  machine works perfectly. Nothing has "failed" in the sense of throwing an
  error. You are simply searching 2/3 of the library.
- **Day B:** every machine is up, but the embedding service — the thing that
  turns text into those 64 numbers — returns a 500 error. Every shard answers
  perfectly. You searched 3/3 of the library, but only with the keyword half.

Both are degraded. They are *completely different problems* with completely
different fixes. Day A means "go look at cluster health." Day B means "go look
at the embedding service."

If you have only one field — say, a single `complete: true/false` — you cannot
tell them apart. Worse: on Day B, coverage was genuinely complete, so a naive
`complete: false` would be lying about which thing went wrong.

### What LATTICE actually returns

```json
"coverage": {
  "shards_queried": 3,
  "shards_answered": 2,
  "complete": false,
  "degraded": false,
  "reason": "shard_unavailable"
}
```

- `complete` answers **only** "did every shard answer?"
- `degraded` answers **only** "did anything fail?"
- `reason` is a single word naming what happened, from a fixed list:
  `complete`, `deadline`, `shard_unavailable`, `dependency_error`,
  `invalid_request`.

All four combinations of `complete`/`degraded` are real:

| complete | degraded | Meaning |
|---|---|---|
| true | false | Everything fine |
| false | false | Day A — a shard is out, nothing errored |
| true | true | Day B — full coverage, a dependency died |
| false | true | The deadline ran out with work outstanding |

**The `reason` field is the thing to point at in a demo.** A client program
switches on one word instead of pattern-matching English error strings, which
is the difference between an API and a log file.

### The honest history of this idea

This is worth saying out loud, because it makes the story stronger rather than
weaker:

**The first version got this wrong.** Completeness was originally computed as
"every shard answered **and** no errors occurred" — a single line of code that
welded the two questions together. The consequence showed up in the data: across
four benchmark runs, the count of incomplete responses and the count of error
responses were identical every time — 56/56, 93/93, 64/64, 24/24. That was not a
coincidence in the measurements. It was an identity in the code.

The fix separated the two concepts, put the definition in exactly one place so
the two backends could not drift apart, and added tests that fail if either
half of the matrix stops being representable.

"We found a flaw in our own instrumentation, and here is the test that stops it
coming back" is a much better story than "everything worked first time."

---

## Part 4 — The two halves of the repository

This is the second thing to be upfront about, because a sharp reviewer will
find it in about four minutes and it looks much worse if they find it first.

**The repository contains two independent tracks.**

### Track 1: the learning track (`pkg/`)

Built from scratch, from first principles, to understand how distributed
systems work:

| Component | What it is |
|---|---|
| `pkg/wal` | A write-ahead log — survives `kill -9` without losing acknowledged writes |
| `pkg/lsm` | A storage engine — memtables, sorted files on disk, compaction |
| `pkg/replication` | Copying data between nodes *without* consensus, to demonstrate exactly how that goes wrong |
| `pkg/raft` | The Raft consensus algorithm — leader election, log replication, snapshots |
| `pkg/shard` | Consistent hashing and rebalancing |

These are real, they work, and they are tested. `pkg/raft` passed its full suite
100 consecutive times; the WAL survived 100 out of 100 `kill -9` recoveries.

### Track 2: the product track (`internal/`, `cmd/`)

The actual search platform: the query API, the Kafka indexer, OpenSearch,
Redis, Kubernetes, Terraform.

### They are not connected, and that is fine

The product does not import `pkg/raft`, `pkg/shard`, `pkg/replication`, or
`pkg/wal`. Only `pkg/lsm` is used, by the local development backend. OpenSearch
does the real work of consensus, sharding, and replication in the product.

**Say this before anyone asks.** The framing that is both true and strong:

> "I built the foundations from scratch so I would understand what OpenSearch
> is doing, not so I could replace it. Wiring my own Raft into the query path
> would be weeks of work that changes no measurement and adds no finding. The
> paper measures the product track, and the tables say so."

One further precision, because it matters: `pkg/raft` is a **deterministic
in-process cluster**. The "nodes" are objects in one program passing messages
through a queue, with logical time. That is a completely legitimate way to build
and test consensus — it makes the tests deterministic and repeatable, which
networked tests are not. But it is **not a networked cluster**, and describing
it as one would be false. Its election measurements are in *logical ticks*, not
milliseconds.

---

## Part 5 — What was actually measured

Three rules govern every number in this project:

1. **Every table says which backend produced it.** The in-memory backend and
   OpenSearch are different systems. Their numbers are not interchangeable.
2. **Every table says where the fault came from.** *Injected in-process*
   (a test knob flipped inside the program) or *induced in the cluster* (a
   machine actually stopped). These are not the same evidence.
3. **Nothing is reported from a single run.** Each configuration is measured
   at least five times, and reported as a median with an interquartile range —
   a spread, showing how much the number moves between runs. A p99 from one
   six-second run is one sample of a noisy quantity, not a measurement.

### The measurements that matter

**Corpus and latency.** Over 1,000,008 documents: p50 around 17ms, p99 around
191ms at rest, under a 200ms deadline.

**Partial results actually work — on a real cluster.** This is the measurement
the earlier runs could not produce. A data node was genuinely stopped in a
three-node OpenSearch cluster. The cluster went red, a third of the data became
unreachable, and the search service kept answering. Across 5,000 searches:

| | Baseline | Node stopped | Node back |
|---|---:|---:|---:|
| Searches completed | 5,000 | 5,000 | 5,000 |
| Marked incomplete | 0 | **5,000** | 0 |
| Carrying an error | 0 | **0** | 0 |
| Reason reported | `complete` | `shard_unavailable` | `complete` |

Every search still came back. Every one said it was incomplete. **Not one
reported an error** — because nothing had errored; a shard was simply out of
reach. The node was restarted, the cluster went green, and the numbers returned
to where they started. That is the claim, and it is now a number rather than a
sentence.

The same experiment runs in-process too, with the fault injected rather than
induced, as a controlled comparison. Both are scored by the same checker.

Some configurations were *compound*: under heavier load the fault also pushed
queries past the deadline. In every one of those, the error count matched the
deadline count **exactly** — 7 and 7, 10 and 10, 2 and 2 on the cluster; 1203 and
1203, 16 and 16, 1370 and 1370, 768 and 768 in-process. Every error was accounted
for by a deadline. **None was an availability loss.** That equality is the whole
point of splitting the two signals: if they were still welded together it would
break immediately.

Be precise about the strength of this if someone asks: on the real cluster
**one** configuration isolated the availability path perfectly, plus three
compound ones that held the invariant. Five more were thrown out because the
cluster was already missing its deadline under load alone, before any fault — and
a configuration that is broken to begin with cannot tell you what the fault did.
That is a small clean result, not a broad sweep, and the reason is that the
cluster was CPU-limited to keep a laptop usable while measuring.

**The two failure classes look different — measurably.** This is the part worth
showing someone who doubts the labels:

| Load | Shard removed | Deadline stalled |
|---|---:|---:|
| light | 56ms | 207ms |
| medium | 128ms | 207ms |
| heavy | 227ms | 219ms |

Losing a shard gets **worse as the system gets busier** — the remaining shards
absorb the work. Blowing the deadline stays **flat**, because the answer leaves
when the clock says so no matter what is happening behind it.

That means the classification can be checked independently: the shape of the
data has to match the label on it. You are not asked to take the `reason` field
on trust.

**And one honest complication, which is itself a finding.** The same fault costs
the opposite thing on the two backends:

| Losing one shard | In-process | Real cluster |
|---|---:|---:|
| Before | 2 ms | 195 ms |
| After | 87 ms | 103 ms |
| Effect | **43x slower** | **1.9x faster** |

Both are right. On the real cluster a missing shard means a third less data to
search, so it is genuinely *quicker*. In-process, an incomplete response can
never be cached, so losing the shard also lost a 100% cache hit rate — and that
swamped the saving.

**The effect does not just differ in size, it flips direction.** This is why
every table in the project names its backend. Quoting one of these numbers as
though it described the other would be simply false, and nothing but the labelling
prevents it.

### The finding nobody was looking for

Across the million-document runs, as the index was compacted from many
**segments** (the immutable files a search index is physically made of) down to
a few:

| Segment state | p50 | p99 |
|---|---:|---:|
| At rest (many segments) | 16.7ms | 191.9ms |
| During merge | 24.7ms | 95.6ms |
| After force-merge (59 → 3) | 18.0ms | 49.1ms |

**p99 dropped roughly 4×. p50 barely moved.**

In plain terms: *how many pieces your index is physically stored in barely
affects the typical search, and hugely affects the worst search.* Each search
has to check every segment; more segments means more chances for one of them to
be slow, and the worst case is what a user actually feels.

The honest caveat, which must be stated alongside it: **the merge that produced
that 4× took 360 seconds** of heavy disk and CPU work. "You can cut p99 4×" is
marketing. "You can cut p99 4× for six minutes of expensive merging, which you
cannot do continuously in production" is a result.

### What is deliberately not claimed

- No measurement with a real embedding model — the embeddings here are a fast
  deterministic stand-in.
- No 30-day availability window; no production traffic exists.
- No cost-per-million figures. These are recorded as **unmeasured**, not
  estimated. An estimate in a benchmark table is a lie with a decimal point.

---

## Part 6 — The hard questions, and honest answers

### "Isn't this just OpenSearch with extra steps?"

Partly, and that is the correct engineering choice. OpenSearch is the index. The
contribution is the **query contract** on top of it: a response that tells you
what it reached and why it fell short, in a form a program can act on. Most
systems either hide that or expose it as unparseable error text.

### "You didn't build the distributed part."

For the product, correct — OpenSearch does the sharding and replication. I built
those from scratch separately in `pkg/` to understand them. The paper measures
the product, and every table says which backend produced it.

### "Your Raft isn't real Raft."

It is a correct Raft implementation running as a deterministic in-process
cluster, which is the standard way to test consensus algorithms — real networks
make tests flaky, not rigorous. It is not deployed over a network, and I do not
claim it is. Election times are reported in logical ticks for exactly that
reason.

### "How do I know your partial results claim is true?"

Run `make experiment-partial-local` for the in-process version, or
`make multinode-up && make experiment-partial-opensearch` to stop a real data
node in a three-node cluster. Both **fail loudly** unless some configuration
produced incomplete responses with zero errors, and unless every error that did
appear is accounted for by a deadline expiry. The pass condition is written into
the script, not into the prose, and the raw artifacts are in
[`docs/experiments/`](docs/experiments/README.md).

Both have been executed. On the real cluster a stopped data node produced 5,000
incomplete responses with 0 errors and the cluster then recovered; in-process, 5
of 9 configurations did the same. Raw artifacts are committed, so the numbers can
be checked rather than believed.

### "Why 64-dimensional embeddings? That's tiny."

Because it is a deterministic stand-in, not a real model, and swapping in a real
one is a configuration change (`EMBEDDING_URL`). Using a small deterministic
embedder keeps every test reproducible. The tradeoff is stated rather than
hidden: no result here is evidence about real embedding quality.

### "Your benchmark p99 of 191ms is close to your 200ms deadline."

Yes, and that is why the deadline behaviour is measured rather than assumed.
Responses that hit the deadline are reported as incomplete with
`reason: "deadline"` — they are counted, not swallowed.

### "What would you do next?"

Ingest freshness (how long from publish to searchable), single-node recovery
time, and cost per million. All three are currently listed as unmeasured, which
is where they should stay until they are measured.

---

## Part 7 — The 60-second script

> LATTICE is a hybrid search platform: it searches by keywords and by meaning
> simultaneously and fuses the two rankings.
>
> The real subject is what happens when part of the cluster is unreachable.
> Most systems either hang or return an error. LATTICE returns what it managed
> to reach, and — this is the part I care about — it separates *"how much did I
> reach"* from *"did anything break"* into two independent fields plus a
> one-word reason code. A client can tell "a shard is down" apart from "the
> embedding service died" without parsing an error message.
>
> I got that wrong the first time: completeness was defined to include "no
> errors occurred", which made the two indistinguishable. It showed up in the
> data as two counters that were identical across four runs. I separated them,
> put the definition in one place so the two backends can't disagree, and wrote
> a test that fails if either case stops being representable. Then I ran the
> experiment that actually needed it: take a shard out and confirm the searches
> come back incomplete with zero errors. Five thousand searches, all incomplete,
> none carrying an error. Where load also blew the deadline, the error count
> matched the deadline count exactly — so every error was a timeout and none was
> a missing shard.
>
> The repository also has a from-scratch WAL, LSM engine, Raft, and sharding.
> Those are a learning track — the product uses OpenSearch, and I keep the two
> clearly separated rather than pretending my Raft is in the query path.
>
> The finding I did not expect: segment count dominates tail latency and barely
> touches median latency. Merging cut p99 by 4× — and cost six minutes of
> merging to do it, which is the half of that sentence most people leave out.

---

## Where to go next

| You want | Read |
|---|---|
| The exact HTTP contract | [`api.md`](api.md) |
| To run it | [`docs/walkthrough.md`](docs/walkthrough.md) |
| The numbers and how they were taken | [`docs/benchmarks.md`](docs/benchmarks.md) |
| The experiments and their pass conditions | [`docs/experiments/README.md`](docs/experiments/README.md) |
| The design decisions | [`docs/ENGINEERING.md`](docs/ENGINEERING.md) |
