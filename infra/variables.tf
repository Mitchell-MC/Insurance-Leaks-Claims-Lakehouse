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

# Reason: the scheduled jobs and the Power BI SQL warehouse are the only
# continuously-billing resources here, and they are not needed to demonstrate
# that ingestion writes real Delta tables into Unity Catalog. Note especially
# that `lakehouse-alerts-snapshot` runs every 15 minutes against a cluster with
# `autotermination_minutes = 30`, so enabling it keeps that cluster awake ~24/7
# rather than letting it idle down. Both default to disabled so a plain
# `terraform apply` provisions the workspace/catalog/cluster only; set these to
# true deliberately, and expect an ongoing bill.
variable "enable_scheduled_jobs" {
  description = "Create the two scheduled Databricks Jobs (jobs.tf). Keeps the verification cluster awake ~24/7 when true, because the alerts job's 15-minute schedule preempts the cluster's 30-minute autotermination."
  type        = bool
  default     = false
}

variable "enable_sql_warehouse" {
  description = "Create the Power BI SQL warehouse (sql_warehouse.tf). Only needed when actually building the .pbix against Gold."
  type        = bool
  default     = false
}

# Reason: the original Standard_DS3_v2 default fails on two counts in eastus2 --
# that SKU is no longer offered there at all (the DS/Dv3 families have aged out
# in favour of v6/v7), and at 4 vCPUs it exactly consumes a Free Trial's
# "Total Regional vCPUs" limit of 4, leaving no headroom for the driver. The
# resulting Databricks error ("The VM size you are specifying is not available")
# reads like a stockout but is really an availability+quota wall. D2ds_v7 is
# 2 vCPU / 8 GB with local SSD and premium storage, which fits inside the cap.
# On a subscription with a raised quota, a 4-vCPU node is the better default.
variable "cluster_node_type" {
  description = "VM size for the Phase 0 verification cluster. Must be offered in var.location and fit the subscription's regional vCPU quota (Free Trial caps this at 4)."
  type        = string
  default     = "Standard_D2ds_v7"
}

variable "cluster_single_user_name" {
  description = "Databricks user principal that owns the SINGLE_USER verification cluster (usually your Entra ID UPN, e.g. you@yourtenant.onmicrosoft.com). Required because Unity Catalog workspaces reject the legacy NO_ISOLATION single-node mode."
  type        = string
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
