output "workspace_url" {
  description = "URL of the provisioned Azure Databricks workspace."
  value       = azurerm_databricks_workspace.this.workspace_url
}

output "catalog_name" {
  description = "Unity Catalog catalog name (must match LakehouseSettings.catalog_name)."
  value       = databricks_catalog.lakehouse.name
}

output "verification_cluster_id" {
  description = "Cluster ID for the Phase 0 manual verification run."
  value       = databricks_cluster.verification.cluster_id
}
