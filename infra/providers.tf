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
  host                         = azurerm_databricks_workspace.this.workspace_url
  azure_workspace_resource_id  = azurerm_databricks_workspace.this.id
}
