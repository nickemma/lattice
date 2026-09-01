variable "opensearch_version" {
  type        = string
  description = "OpenSearch version managed by the OpenSearch Kubernetes Operator."
  default     = "2.17.1"
}

variable "opensearch_replicas" {
  type        = number
  description = "Number of OpenSearch nodes in the initial data/master pool."
  default     = 3
}

variable "opensearch_disk_size" {
  type        = string
  description = "Persistent disk size for each OpenSearch node."
  default     = "30Gi"
}

variable "lattice_api_key" {
  type        = string
  description = "Optional API key injected into the query deployment. Leave empty to keep local-style unauthenticated development behavior."
  default     = ""
  sensitive   = true
}

variable "opensearch_admin_password" {
  type        = string
  description = "Strong password for the OpenSearch admin user. Supply it through TF_VAR_opensearch_admin_password or a secret manager; it is intentionally not stored in this repository."
  sensitive   = true

  validation {
    condition     = length(var.opensearch_admin_password) >= 16
    error_message = "opensearch_admin_password must be at least 16 characters."
  }
}

variable "opensearch_http_tls" {
  type        = bool
  description = "Enable HTTPS for the OpenSearch HTTP API. Set true with the secure Kubernetes overlay and provide the corresponding CA/client certificate secrets."
  default     = false
}

resource "kubernetes_secret" "opensearch_admin_credentials" {
  metadata {
    name      = "lattice-opensearch-auth"
    namespace = kubernetes_namespace.lattice.metadata[0].name
  }

  type = "Opaque"

  data = {
    username = "admin"
    password = var.opensearch_admin_password
  }
}

locals {
  opensearch_base_spec = {
    general = {
      version     = var.opensearch_version
      httpPort    = 9200
      vendor      = "opensearch"
      serviceName = "lattice-search-cluster-master"
      additionalConfig = var.opensearch_http_tls ? {} : {
        "plugins.security.disabled" = "true"
      }
    }
    dashboards = {
      enable   = false
      replicas = 0
      version  = var.opensearch_version
    }
    nodePools = [{
      component = "masters"
      replicas  = var.opensearch_replicas
      diskSize  = var.opensearch_disk_size
      roles     = ["cluster_manager", "data"]
      additionalConfig = var.opensearch_replicas == 1 ? {
        "discovery.seed_hosts"                  = "lattice-search-masters-0"
        "cluster.initial_master_nodes"          = "lattice-search-masters-0"
        "cluster.initial_cluster_manager_nodes" = "lattice-search-masters-0"
      } : {}
      env = var.opensearch_http_tls ? [{
        name = "OPENSEARCH_INITIAL_ADMIN_PASSWORD"
        valueFrom = {
          secretKeyRef = {
            name = kubernetes_secret.opensearch_admin_credentials.metadata[0].name
            key  = "password"
          }
        }
      }] : []
      resources = {
        requests = {
          memory = "2Gi"
          cpu    = "500m"
        }
        limits = {
          memory = "4Gi"
          cpu    = "2"
        }
      }
    }]
  }

  opensearch_secure_spec = merge(local.opensearch_base_spec, {
    bootstrap = {
      env = [{
        name = "OPENSEARCH_INITIAL_ADMIN_PASSWORD"
        valueFrom = {
          secretKeyRef = {
            name = kubernetes_secret.opensearch_admin_credentials.metadata[0].name
            key  = "password"
          }
        }
      }]
    }
    security = {
      config = {
        adminCredentialsSecret = {
          name = kubernetes_secret.opensearch_admin_credentials.metadata[0].name
        }
      }
      tls = {
        http = {
          enabled                = true
          duration               = "8760h"
          rotateDaysBeforeExpiry = -1
        }
        transport = {
          enabled                = true
          generate               = true
          perNode                = false
          duration               = "8760h"
          rotateDaysBeforeExpiry = -1
        }
      }
    }
  })

  # YAML round-tripping makes the conditional select between two complete
  # manifest shapes without Terraform trying to unify optional CRD objects.
  opensearch_spec = yamldecode(var.opensearch_http_tls ? yamlencode(local.opensearch_secure_spec) : yamlencode(local.opensearch_base_spec))
}

resource "kubernetes_manifest" "opensearch_cluster" {
  field_manager {
    force_conflicts = true
  }

  manifest = {
    apiVersion = "opensearch.org/v1"
    kind       = "OpenSearchCluster"
    metadata = {
      name      = "lattice-search"
      namespace = kubernetes_namespace.lattice.metadata[0].name
    }
    spec = local.opensearch_spec
  }

  depends_on = [kubernetes_namespace.lattice]
}

resource "kubernetes_secret" "lattice_api_key" {
  count = var.lattice_api_key == "" ? 0 : 1

  metadata {
    name      = "lattice-api-key"
    namespace = kubernetes_namespace.lattice.metadata[0].name
  }

  type = "Opaque"

  data = {
    value = var.lattice_api_key
  }
}
