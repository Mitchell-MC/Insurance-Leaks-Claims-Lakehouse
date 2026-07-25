# SQL warehouse for Power BI, added now that the Gold mart exists to query
# (infra/compute.tf's own comment earmarked this for Phase 7). Serverless +
# auto-stop keeps cost near zero between dashboard refreshes/interactive use,
# appropriate for this portfolio project's query volume.

resource "databricks_sql_endpoint" "power_bi" {
  name             = "lakehouse-power-bi"
  cluster_size     = "2X-Small"
  auto_stop_mins   = 10
  enable_serverless_compute = true

  tags {
    custom_tags {
      key   = "Project"
      value = "insurance-lakehouse"
    }
  }
}
