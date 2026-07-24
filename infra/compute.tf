# Single small-node cluster for Phase 0 verification (manual notebook read/write
# against the bronze schema) and ad-hoc Phase 2 ingestion runs. A SQL warehouse
# for Power BI is added in Phase 7 once the gold mart exists to query.

resource "databricks_cluster" "verification" {
  cluster_name            = "lakehouse-verification"
  spark_version            = var.cluster_spark_version
  node_type_id              = var.cluster_node_type
  autotermination_minutes = 30
  num_workers              = 0 # single-node

  spark_conf = {
    "spark.databricks.cluster.profile" = "singleNode"
    "spark.master"                     = "local[*]"
  }

  custom_tags = {
    ResourceClass = "SingleNode"
    Project       = "insurance-lakehouse"
  }
}
