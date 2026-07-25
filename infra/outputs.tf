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

output "power_bi_sql_warehouse_http_path" {
  description = "HTTP path for Power BI's Databricks connector (Server Hostname + this = the Get Data connection)."
  value       = databricks_sql_endpoint.power_bi.odbc_params[0].path
}

output "power_bi_sql_warehouse_hostname" {
  description = "Server hostname for Power BI's Databricks connector."
  value       = databricks_sql_endpoint.power_bi.odbc_params[0].hostname
}
