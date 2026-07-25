# Two independently-scheduled jobs, per the mentoring brief's Phase 6 ask to
# schedule historical batch refreshes separately from near-real-time weather
# alert refreshes -- see docs/batch_vs_streaming_memo.md for the reasoning.
# Both reuse the Phase 0 verification cluster (infra/compute.tf) rather than
# provisioning dedicated job clusters, appropriate for this portfolio
# project's data volumes; a production deployment would size dedicated
# autoscaling job clusters per workload instead.

locals {
  wheel_package_name = "lakehouse"
}

# ------------------------------------------------------------------------
# Batch job: FEMA + NOAA + geography ingestion, then Silver -> Process -> Gold.
# Runs once daily -- these sources update at most daily upstream (FEMA/NOAA
# publish new data on their own daily-or-slower cadence), so more frequent
# batch runs would just re-read unchanged source files.
# ------------------------------------------------------------------------
resource "databricks_job" "lakehouse_batch" {
  name = "lakehouse-batch-pipeline"

  schedule {
    quartz_cron_expression = "0 0 6 * * ?" # 06:00 UTC daily
    timezone_id            = "UTC"
  }

  task {
    task_key            = "ingest_historical"
    existing_cluster_id = databricks_cluster.verification.cluster_id
    library {
      whl = var.lakehouse_wheel_path
    }
    python_wheel_task {
      package_name = local.wheel_package_name
      entry_point  = "lakehouse"
      parameters   = ["ingest", "--source", "historical"]
    }
  }

  task {
    task_key = "silver"
    depends_on {
      task_key = "ingest_historical"
    }
    existing_cluster_id = databricks_cluster.verification.cluster_id
    library {
      whl = var.lakehouse_wheel_path
    }
    python_wheel_task {
      package_name = local.wheel_package_name
      entry_point  = "lakehouse"
      parameters   = ["silver"]
    }
  }

  task {
    task_key = "process"
    depends_on {
      task_key = "silver"
    }
    existing_cluster_id = databricks_cluster.verification.cluster_id
    library {
      whl = var.lakehouse_wheel_path
    }
    python_wheel_task {
      package_name = local.wheel_package_name
      entry_point  = "lakehouse"
      parameters   = ["process"]
    }
  }

  task {
    task_key = "gold"
    depends_on {
      task_key = "process"
    }
    existing_cluster_id = databricks_cluster.verification.cluster_id
    library {
      whl = var.lakehouse_wheel_path
    }
    python_wheel_task {
      package_name = local.wheel_package_name
      entry_point  = "lakehouse"
      parameters   = ["gold"]
    }
  }

  email_notifications {
    on_failure = var.job_failure_notification_emails
  }

  webhook_notifications {
    dynamic "on_failure" {
      for_each = var.job_failure_webhook_id == "" ? [] : [var.job_failure_webhook_id]
      content {
        id = on_failure.value
      }
    }
  }
}

# ------------------------------------------------------------------------
# Alerts job: NWS active-alert snapshots only, on a tight schedule to
# approximate near-real-time operational visibility. Deliberately does NOT
# chain into silver/process/gold -- those stages re-run on the batch job's
# daily schedule and will pick up whatever alert snapshots landed since the
# last run. Running the full downstream pipeline every 15 minutes would
# reprocess unchanged FEMA/NOAA data for no benefit.
# ------------------------------------------------------------------------
resource "databricks_job" "lakehouse_alerts" {
  name = "lakehouse-alerts-snapshot"

  schedule {
    quartz_cron_expression = "0 */15 * * * ?" # every 15 minutes
    timezone_id            = "UTC"
  }

  task {
    task_key            = "ingest_alerts"
    existing_cluster_id = databricks_cluster.verification.cluster_id
    library {
      whl = var.lakehouse_wheel_path
    }
    python_wheel_task {
      package_name = local.wheel_package_name
      entry_point  = "lakehouse"
      parameters   = ["ingest", "--source", "nws_alerts_snapshots"]
    }
  }

  email_notifications {
    on_failure = var.job_failure_notification_emails
  }

  webhook_notifications {
    dynamic "on_failure" {
      for_each = var.job_failure_webhook_id == "" ? [] : [var.job_failure_webhook_id]
      content {
        id = on_failure.value
      }
    }
  }
}
