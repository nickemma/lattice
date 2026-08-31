.PHONY: dev run compose-up compose-down compose-logs test test-integration test-chaos bench-query reindex snapshot restore-drill

GO_ENV=GOCACHE=$${LATTICE_GOCACHE:-/tmp/lattice-gocache}

dev run:
	$(GO_ENV) go run ./cmd

compose-up:
	docker compose -f deploy/docker-compose.yaml up --build

compose-down:
	docker compose -f deploy/docker-compose.yaml down

compose-logs:
	docker compose -f deploy/docker-compose.yaml logs -f lattice-query lattice-indexer

test:
	$(GO_ENV) go test ./...

test-integration:
	$(GO_ENV) go test -tags=integration ./...

test-chaos:
	$(GO_ENV) go test -tags=chaos ./...

bench-query:
	$(GO_ENV) go test -run '^$$' -bench . ./bench/...

reindex:
	curl -fsS -X POST http://localhost:8080/v1/admin/reindex

snapshot:
	curl -fsS -X POST http://localhost:8080/v1/admin/snapshot

restore-drill:
	curl -fsS -X POST http://localhost:8080/v1/admin/restore
