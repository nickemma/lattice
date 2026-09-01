# Architecture decision records

This directory holds decisions that depend on measurements rather than taste.

The current OpenSearch template starts with three primary shards and one replica so the local Compose path is small and failure drills are understandable. That is a bootstrap value, not the final one-million-document sizing decision.

Before Phase 6 is marked complete, add an ADR containing the corpus distribution, index size, merge behavior, query p50/p95/p99 at rest and during a merge, ingest throughput and rejection rate by bulk size, node and replica assumptions, and the resulting shard-count decision with its reindex plan.
