output "workspace_url" {
  description = "URL of the provisioned Azure Databricks workspace."
  value       = azurerm_databricks_workspace.this.workspace_url
}

output "catalog_name" {
  description = "Unity Catalog catalog name (must match LakehouseSettings.catalog_name)."
  value       = databricks_catalog.lakehouse.name
}

output "verification_cluster_id" {
  description = "Cluster ID for the Phase 0 manual verification run. Null when enable_verification_cluster is false (e.g. on subscriptions that cannot allocate a supported node type)."
  value       = one(databricks_cluster.verification[*].cluster_id)
}

# Reason: these resolve to null when `enable_sql_warehouse = false` (the
# default), since the warehouse resource is then not created.
output "power_bi_sql_warehouse_http_path" {
  description = "HTTP path for Power BI's Databricks connector (Server Hostname + this = the Get Data connection). Null unless enable_sql_warehouse is true."
  value       = one(databricks_sql_endpoint.power_bi[*].odbc_params[0].path)
}

output "power_bi_sql_warehouse_hostname" {
  description = "Server hostname for Power BI's Databricks connector. Null unless enable_sql_warehouse is true."
  value       = one(databricks_sql_endpoint.power_bi[*].odbc_params[0].hostname)
}
