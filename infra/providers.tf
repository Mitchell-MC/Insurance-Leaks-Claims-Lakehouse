terraform {
  required_version = ">= 1.7.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }

  # Values supplied at `terraform init` time via -backend-config, see README.md.
  backend "azurerm" {}
}

provider "azurerm" {
  features {}
}

provider "databricks" {
  # Reason: token auth (vs. azure_workspace_resource_id) means this provider
  # doesn't need its own Azure AD token exchange -- it authenticates the same
  # way the `databricks` CLI already does via ~/.databrickscfg. The azurerm
  # provider above still needs its own Azure credential source (CLI session
  # or ARM_* env vars) to initialize, since Terraform validates every
  # declared provider block regardless of which resources actually change.
  host  = azurerm_databricks_workspace.this.workspace_url
  token = var.databricks_pat
}
