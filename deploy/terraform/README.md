# LATTICE Terraform deployment

Terraform owns the `lattice` namespace, the operator-managed OpenSearch
cluster, and the optional API-key Secret. Argo CD owns the application and
configuration resources under `deploy/k8s`; the OpenSearch cluster resource is
intentionally not in that kustomization.

Install cert-manager and the OpenSearch Kubernetes Operator before running
Terraform. The deployment uses the Operator 2.8.4 chart and current
`opensearch.org/v1` CRDs. The chart retains legacy CRDs during migration, but
LATTICE uses only the current API:

```bash
helm repo add opensearch-operator https://opensearch-project.github.io/opensearch-k8s-operator/
helm repo update
helm repo add jetstack https://charts.jetstack.io
helm repo update
helm upgrade --install cert-manager jetstack/cert-manager \
  --version v1.16.3 \
  --namespace cert-manager \
  --create-namespace \
  --set crds.enabled=true \
  --wait
helm upgrade --install opensearch-operator \
  opensearch-operator/opensearch-operator \
  --version 2.8.4 \
  --namespace opensearch-operator-system \
  --create-namespace \
  --wait
```

Then provide the required OpenSearch admin password through your secret
manager or shell environment. Terraform stores the resulting Kubernetes
Secret in state, so use an encrypted remote state backend for shared or
production environments. Then plan and apply against the selected kubeconfig.
Terraform creates the
`lattice` namespace before Argo CD is bootstrapped; Argo owns only the
resources inside it:

```bash
export TF_VAR_opensearch_admin_password='replace-with-a-strong-local-value'
terraform init
terraform plan -out=lattice.tfplan
terraform apply lattice.tfplan
```

For a local one-node test, use `-var='opensearch_replicas=1'`. Terraform adds
the single-node bootstrap discovery override only for that setting; production
defaults to three nodes. The non-TLS development profile explicitly disables
the OpenSearch security plugin, while `-var='opensearch_http_tls=true'` enables
the secure HTTP/TLS profile. Use the latter together with the secure overlay and
certificate secrets when enabling HTTPS/mTLS.

The Kubernetes Kafka manifest uses the pinned Redpanda image and runs it with
one Seastar core for a small local cluster. Redpanda requires the host kernel
AIO limit to be raised on some kind clusters; configure that on the node before
 applying the manifests rather than granting the broker a privileged container:

```bash
docker exec --privileged lattice-final-e2e-control-plane \
  sysctl -w fs.aio-max-nr=1048576
```

Use the actual kind node name for another cluster. This prerequisite is only
for the local single-broker validation profile; production nodes should carry
the kernel setting through their host image/bootstrap configuration.
