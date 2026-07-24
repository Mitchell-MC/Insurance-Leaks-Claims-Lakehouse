variable "resource_group_name" {
  description = "Resource group for the workspace and its storage."
  type        = string
  default     = "rg-insurance-lakehouse"
}

variable "location" {
  description = "Azure region for all resources."
  type        = string
  default     = "eastus2"
}

variable "workspace_name" {
  description = "Azure Databricks workspace name."
  type        = string
  default     = "dbx-insurance-lakehouse"
}

variable "storage_account_name" {
  description = "ADLS Gen2 storage account name for Delta table data (globally unique, lowercase, no dashes)."
  type        = string
}

variable "storage_container_name" {
  description = "ADLS Gen2 container name used as the Unity Catalog external location root."
  type        = string
  default     = "lakehouse"
}

variable "catalog_name" {
  description = "Unity Catalog catalog name. Must match LakehouseSettings.catalog_name."
  type        = string
  default     = "insurance_lakehouse"
}

variable "cluster_node_type" {
  description = "VM size for the Phase 0 verification cluster."
  type        = string
  default     = "Standard_DS3_v2"
}

variable "cluster_spark_version" {
  description = "Databricks runtime version for the verification cluster."
  type        = string
  default     = "14.3.x-scala2.12"
}
