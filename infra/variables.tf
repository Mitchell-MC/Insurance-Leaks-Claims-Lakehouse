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

variable "lakehouse_wheel_path" {
  description = "Workspace/DBFS/Volumes path to the built `lakehouse` wheel, published by CI (see .github/workflows/ci.yml) ahead of `terraform apply`."
  type        = string
  default     = "dbfs:/FileStore/wheels/lakehouse-0.1.0-py3-none-any.whl"
}

variable "job_failure_notification_emails" {
  description = "Email addresses notified when a lakehouse job task fails."
  type        = list(string)
  default     = []
}

variable "job_failure_webhook_id" {
  description = "Optional Databricks notification-destination webhook ID for job failures. Leave empty to disable."
  type        = string
  default     = ""
}
