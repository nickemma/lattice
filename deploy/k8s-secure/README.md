# LATTICE secure Kubernetes overlay

The base manifests are intentionally easy to run on a local cluster. This
overlay turns on TLS for the API and mutual TLS for the OpenSearch connections.
It requires these pre-created secrets in the `lattice` namespace:

```bash
kubectl -n lattice create secret generic lattice-query-tls \
  --from-file=tls.crt=server.crt --from-file=tls.key=server.key --from-file=ca.crt=client-ca.crt
kubectl -n lattice create secret generic lattice-opensearch-mtls \
  --from-file=ca.crt=opensearch-ca.crt --from-file=tls.crt=client.crt --from-file=tls.key=client.key
```

Render and apply it with:

```bash
export TF_VAR_opensearch_admin_password='replace-with-a-strong-local-value'
terraform -chdir=deploy/terraform plan -var='opensearch_http_tls=true' -out=secure.tfplan
terraform -chdir=deploy/terraform apply secure.tfplan
kubectl kustomize deploy/k8s-secure
kubectl apply -k deploy/k8s-secure
```

The Terraform OpenSearch resource must be applied with
`opensearch_http_tls=true`; the overlay alone changes client URLs and
certificates but cannot mutate the Terraform-owned cluster resource.

Do not commit the password or certificate material. In shared environments,
source them from a secret manager and use encrypted Terraform state because
the Kubernetes Secret value is represented in state.

The API requires client certificates signed by `client-ca.crt`. The
OpenSearch client certificate must be authorized by the cluster's security
configuration. Snapshot and restore jobs use the same client certificate.
Prometheus scraping of the query service should be configured with the same
CA/client certificate pair in the Prometheus deployment.
