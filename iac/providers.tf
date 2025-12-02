terraform {
  required_version = ">= 1.5.0"

  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.33"
    }

    helm = {
      source  = "hashicorp/helm"
      version = ">= 2.13"
    }
  }
}

provider "kubernetes" {
  config_path    = "~/.kube/config"
  config_context = "big-data-group"
}

provider "helm" {
  kubernetes = {
    config_path    = "~/.kube/config"
    config_context = "big-data-group"
  }
}
