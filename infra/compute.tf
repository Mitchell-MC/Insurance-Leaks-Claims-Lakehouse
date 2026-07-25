# Single small-node cluster for Phase 0 verification (manual notebook read/write
# against the bronze schema) and ad-hoc Phase 2 ingestion runs. A SQL warehouse
# for Power BI is added in Phase 7 once the gold mart exists to query.

resource "databricks_cluster" "verification" {
  count                   = var.enable_verification_cluster ? 1 : 0
  cluster_name            = "lakehouse-verification"
  spark_version           = var.cluster_spark_version
  node_type_id            = var.cluster_node_type
  autotermination_minutes = 30
  num_workers             = 0 # single-node

  # Reason: a Unity Catalog workspace rejects the legacy NO_ISOLATION access
  # mode that `spark.databricks.cluster.profile = singleNode` alone implies
  # ("NO_ISOLATION or custom access modes are not allowed in this workspace").
  # SINGLE_USER is the UC-compatible single-node mode and is what lets this
  # cluster read/write the insurance_lakehouse catalog at all.
  data_security_mode = "SINGLE_USER"
  single_user_name   = var.cluster_single_user_name

  spark_conf = {
    "spark.databricks.cluster.profile" = "singleNode"
    "spark.master"                     = "local[*]"
  }

  custom_tags = {
    ResourceClass = "SingleNode"
    Project       = "insurance-lakehouse"
  }
}
