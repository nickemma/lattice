terraform {
  required_version = ">= 1.6.0"
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.30"
    }
  }
}

variable "kubeconfig" {
  type        = string
  description = "Path to the Kubernetes config used for the LATTICE cluster."
  default     = "~/.kube/config"
}

provider "kubernetes" {
  config_path = pathexpand(var.kubeconfig)
}

resource "kubernetes_namespace" "lattice" {
  metadata { name = "lattice" }
}

output "namespace" {
  value       = kubernetes_namespace.lattice.metadata[0].name
  description = "Namespace consumed by the Argo CD workload application."
}

output "opensearch_cluster" {
  value       = kubernetes_manifest.opensearch_cluster.manifest.metadata.name
  description = "OpenSearchCluster reconciled by the OpenSearch Kubernetes Operator."
}
