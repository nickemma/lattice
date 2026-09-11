.PHONY: dev run compose-up compose-down compose-logs multinode-up multinode-down smoke-local smoke-compose smoke-argocd test test-integration test-chaos wal-crash-evidence election-evidence bench-query bench-remote experiment-partial-local experiment-partial-opensearch experiment-segments reindex snapshot restore-drill chaos-k8s render-k8s render-k8s-secure

GO_ENV=GOCACHE=$${LATTICE_GOCACHE:-/tmp/lattice-gocache}

dev run:
	$(GO_ENV) go run ./cmd

compose-up:
	docker compose -f deploy/docker-compose.yaml up --build

compose-down:
	docker compose -f deploy/docker-compose.yaml down

compose-logs:
	docker compose -f deploy/docker-compose.yaml logs -f lattice-query lattice-indexer

# Three-data-node OpenSearch with a replica-free index. Required for the
# partial-results experiment: the default single-node stack cannot produce a
# state where the API is up and a shard is missing.
multinode-up:
	docker compose -f deploy/docker-compose.multinode.yaml up -d --build

multinode-down:
	docker compose -f deploy/docker-compose.multinode.yaml down -v

smoke-local:
	bash scripts/smoke-local.sh

smoke-compose:
	bash scripts/smoke-compose.sh

smoke-argocd:
	bash scripts/smoke-argocd.sh

test:
	$(GO_ENV) go test ./...

test-integration:
	$(GO_ENV) go test -tags=integration ./...

test-chaos:
	$(GO_ENV) go test -tags=chaos ./...

wal-crash-evidence:
	$(GO_ENV) go test -run '^TestWALCrashRecovery$$' ./pkg/wal

election-evidence:
	LATTICE_ELECTION_OUT=$(CURDIR)/docs/election-distribution.csv $(GO_ENV) go test -run '^TestElectionDistribution$$' ./bench

bench-query:
	$(GO_ENV) go test -run '^$$' -bench . ./bench/... -benchmem

# Repeats, query classes, a concurrency sweep, and a warm-up are defaults rather
# than options: a single run at one concurrency with one query is one sample.
bench-remote:
	$(GO_ENV) go run ./cmd/latticebench \
		-url "$${LATTICE_E2E_URL:-http://localhost:8080}" \
		-q "$${LATTICE_BENCH_QUERY_COMMON:-common=raft}" \
		-q "$${LATTICE_BENCH_QUERY_RARE:-rare=sstables}" \
		-q "$${LATTICE_BENCH_QUERY_PHRASE:-phrase=distributed consensus leader election}" \
		-queries "$${LATTICE_BENCH_QUERIES:-1000}" \
		-concurrency "$${LATTICE_BENCH_CONCURRENCY:-8,32,128}" \
		-runs "$${LATTICE_BENCH_RUNS:-5}" \
		-warmup "$${LATTICE_BENCH_WARMUP:-10s}" \
		-backend "$${LATTICE_BENCH_BACKEND:-opensearch}" \
		-fault-source "$${LATTICE_BENCH_FAULT_SOURCE:-none}" \
		-opensearch-url "$${LATTICE_BENCH_OPENSEARCH_URL:-http://localhost:19200}" \
		-commit "$$(git rev-parse --short HEAD 2>/dev/null || echo unknown)" \
		-output "$${LATTICE_BENCH_OUTPUT:-docs/remote-benchmark.json}"

# Partial results on the in-process backend, with faults injected through
# /v1/debug/shards. Self-contained: builds, starts, seeds, measures, verifies.
experiment-partial-local:
	bash scripts/partial-results-local.sh $${LATTICE_EXPERIMENT_OUT:-docs/experiments}

# Partial results on a real cluster, with a data node actually stopped.
# Requires `make multinode-up` first.
experiment-partial-opensearch:
	bash scripts/partial-results-opensearch.sh $${LATTICE_EXPERIMENT_OUT:-docs/experiments}

# Tail latency against segment count. Requires a running stack with a declared
# corpus already loaded; force-merges the index, so do not aim it at real data.
experiment-segments:
	bash scripts/segment-latency.sh $${LATTICE_EXPERIMENT_OUT:-docs/experiments}

reindex:
	curl -fsS -X POST http://localhost:8080/v1/admin/reindex

snapshot:
	curl -fsS -X POST http://localhost:8080/v1/admin/snapshot

restore-drill:
	curl -fsS -X POST http://localhost:8080/v1/admin/restore

chaos-k8s:
	bash chaos/run.sh $${LATTICE_NAMESPACE:-lattice}

render-k8s:
	kubectl kustomize deploy/k8s >/tmp/lattice-k8s.yaml

render-k8s-secure:
	kubectl kustomize deploy/k8s-secure >/tmp/lattice-k8s-secure.yaml
