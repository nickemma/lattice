# LATTICE chaos experiments

The Go tests in this directory prove quorum loss and leader failure without requiring a cluster. The following commands exercise the Kubernetes path after applying `deploy/k8s`:

For the repeatable pod-restart/replay drill, run:

```bash
bash chaos/run.sh lattice
```

Set `LATTICE_API_KEY` if the deployment protects its `/v1` routes. The script records response bodies in `/tmp/lattice-chaos-*.json` and exits non-zero if readiness or replay recovery fails.

For an actual query-to-data-tier network partition, install Chaos Mesh and review/apply the optional experiment:

```bash
kubectl apply -f chaos/network-partition.yaml
```

During the two-minute window, assert that searches remain within their deadline and expose `coverage.complete: false`; remove the experiment early with `kubectl -n lattice delete networkchaos lattice-query-opensearch-partition` if the test window must end.

```bash
# Query remains available while one query pod is recreated.
kubectl -n lattice delete pod -l app=lattice-query --wait=false

# Indexer restart: Kafka offsets should cause uncommitted records to replay.
kubectl -n lattice delete pod -l app=lattice-indexer --wait=false

# Confirm the API reports degraded readiness or incomplete coverage during a dependency outage.
kubectl -n lattice get pods
kubectl -n lattice port-forward svc/lattice-query 8080:8080
curl -i http://localhost:8080/readyz
curl -G http://localhost:8080/v1/search --data-urlencode 'q=consensus'
```

Record the injected failure, start/end times, coverage, error rate, recovery time, and document count in the postmortem template. Do not run destructive experiments against a production namespace without an approved change window.
