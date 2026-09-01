.PHONY: dev run compose-up compose-down compose-logs smoke-local smoke-compose smoke-argocd test test-integration test-chaos wal-crash-evidence election-evidence bench-query bench-remote reindex snapshot restore-drill chaos-k8s render-k8s render-k8s-secure

GO_ENV=GOCACHE=$${LATTICE_GOCACHE:-/tmp/lattice-gocache}

dev run:
	$(GO_ENV) go run ./cmd

compose-up:
	docker compose -f deploy/docker-compose.yaml up --build

compose-down:
	docker compose -f deploy/docker-compose.yaml down

compose-logs:
	docker compose -f deploy/docker-compose.yaml logs -f lattice-query lattice-indexer

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

bench-remote:
	$(GO_ENV) go run ./cmd/latticebench -url "$${LATTICE_E2E_URL:-http://localhost:8080}" -queries "$${LATTICE_BENCH_QUERIES:-1000}" -concurrency "$${LATTICE_BENCH_CONCURRENCY:-16}" -output "$${LATTICE_BENCH_OUTPUT:-docs/remote-benchmark.json}"

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
