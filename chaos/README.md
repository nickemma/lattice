# LATTICE chaos experiments

The Go tests in this directory prove quorum loss and leader failure without requiring a cluster. The following commands exercise the Kubernetes path after applying `deploy/k8s`:

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
