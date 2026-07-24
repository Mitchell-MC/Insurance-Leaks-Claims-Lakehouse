# Requires a Unity Catalog metastore already assigned to this workspace's
# region at the account level — see README.md prerequisites.

resource "databricks_storage_credential" "lakehouse" {
  name = "${var.workspace_name}-storage-credential"

  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.this.id
  }

  depends_on = [azurerm_role_assignment.access_connector_storage]
}

resource "databricks_external_location" "lakehouse" {
  name            = "lakehouse-external-location"
  url             = "abfss://${var.storage_container_name}@${azurerm_storage_account.lakehouse.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.lakehouse.id
}

resource "databricks_catalog" "lakehouse" {
  name    = var.catalog_name
  comment = "Insurance claims leakage & catastrophe response analytics lakehouse."

  storage_root = databricks_external_location.lakehouse.url

  depends_on = [databricks_external_location.lakehouse]
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.lakehouse.name
  name         = "bronze"
  comment      = "Raw ingested data, one Delta table per source."
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.lakehouse.name
  name         = "silver"
  comment      = "Standardized, deduplicated, quality-checked data."
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.lakehouse.name
  name         = "gold"
  comment      = "Dimensional model consumed by Power BI."
}
