resource "azurerm_resource_group" "this" {
  name     = var.resource_group_name
  location = var.location
}

resource "azurerm_databricks_workspace" "this" {
  name                = var.workspace_name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  sku                 = "premium" # Premium required for Unity Catalog
}

resource "azurerm_storage_account" "lakehouse" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # ADLS Gen2 hierarchical namespace, required for Unity Catalog
}

resource "azurerm_storage_container" "lakehouse" {
  name                  = var.storage_container_name
  storage_account_name  = azurerm_storage_account.lakehouse.name
  container_access_type = "private"
}

# Lets Unity Catalog read/write the storage account without a stored secret.
resource "azurerm_databricks_access_connector" "this" {
  name                = "${var.workspace_name}-access-connector"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "access_connector_storage" {
  scope                = azurerm_storage_account.lakehouse.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.this.identity[0].principal_id
}
