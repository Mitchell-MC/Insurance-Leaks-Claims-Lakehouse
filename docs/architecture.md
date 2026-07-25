# Architecture

```mermaid
flowchart TB
    subgraph Sources["Public Data Sources"]
        FEMA["FEMA OpenFEMA API\nDisaster Declarations"]
        NOAA["NOAA Storm Events\nbulk CSV.gz (1996-present)"]
        NWS["NWS Alerts API\napi.weather.gov"]
        CENSUS["Census Gazetteer\ncounty reference"]
    end

    subgraph Ingestion["Ingestion (src/lakehouse/ingestion/)"]
        ING["BaseIngestor subclasses\nretry + _ingestion_metadata"]
    end

    subgraph Bronze["Bronze (raw, append)"]
        B_FEMA[(bronze.fema_declarations)]
        B_NOAA[(bronze.noaa_storm_events)]
        B_NWS[(bronze.nws_alerts_snapshots)]
        B_GEO[(bronze.geography_reference_counties)]
    end

    subgraph Silver["Silver (src/lakehouse/silver/) — standardize, dedupe, DQ-check"]
        S_FEMA[(silver.fema_declarations)]
        S_NOAA[(silver.noaa_storm_events)]
        S_GEO[(silver.geography_reference_counties)]
    end

    subgraph Processing["Processing (src/lakehouse/processing/) — distributed enrichment"]
        JOIN["regional_event_join +\nrolling_event_intensity"]
        SCORE["catastrophe_pressure_score\n(claims_surge_risk)"]
    end

    subgraph Gold["Gold (src/lakehouse/gold/) — star schema"]
        DIMS[("dim_date, dim_geography,\ndim_event_type, dim_alert_status")]
        F_CAT[(fact_catastrophe_event)]
        F_ALERT[(fact_regional_alert_activity)]
        F_COMPLAINT[(fact_complaint_trend\n-- empty, see data_limitations.md)]
        LEAK["leakage_risk_metric\n(leakage_exposure_proxy)"]
    end

    subgraph Orchestration["Orchestration (infra/jobs.tf)"]
        JOB_BATCH["lakehouse-batch-pipeline\ndaily: ingest historical -> silver -> process -> gold"]
        JOB_ALERTS["lakehouse-alerts-snapshot\nevery 15 min: ingest NWS alerts only"]
    end

    subgraph Serving["Serving"]
        VIEWS["gold.v_* reporting views\n(docs/reporting_views.sql)"]
        PBI["Power BI dashboard\n(via SQL warehouse)"]
    end

    FEMA --> ING
    NOAA --> ING
    NWS --> ING
    CENSUS --> ING
    ING --> B_FEMA & B_NOAA & B_NWS & B_GEO

    B_FEMA --> S_FEMA
    B_NOAA --> S_NOAA
    B_GEO --> S_GEO

    S_NOAA --> JOIN
    S_FEMA --> JOIN
    JOIN --> SCORE
    B_NWS -.-> SCORE

    S_FEMA --> Gold
    S_GEO --> Gold
    JOIN --> F_CAT
    SCORE --> F_ALERT
    F_CAT --> LEAK
    F_ALERT --> LEAK

    JOB_BATCH -.orchestrates.-> Ingestion
    JOB_BATCH -.orchestrates.-> Silver
    JOB_BATCH -.orchestrates.-> Processing
    JOB_BATCH -.orchestrates.-> Gold
    JOB_ALERTS -.orchestrates.-> B_NWS

    Gold --> VIEWS --> PBI
```

## Layer responsibilities

| Layer | Package | Responsibility |
|---|---|---|
| Ingestion | `src/lakehouse/ingestion/` | Fetch raw source data, attach `_ingestion_metadata` (source date, load time, run id, record count), append to Bronze Delta tables. |
| Silver | `src/lakehouse/silver/` | Normalize schemas/geography (`us_state_codes.py` reconciles FEMA/NOAA's different state-name conventions), dedupe, run data-quality checks, attach `_dq_metadata`. |
| Processing | `src/lakehouse/processing/` | Distributed joins and window-function enrichment (`Window.rangeBetween` for rolling 7d/30d metrics); the layer this project's weakest-pillar focus (Distributed Computing) lives in. |
| Gold | `src/lakehouse/gold/` | Star schema: `dimensions/` and `facts/` sub-packages, plus derived metrics (`leakage_risk_metric`). |
| Orchestration | `src/lakehouse/orchestration/` + `infra/jobs.tf` | Wires stages together (`main.py`'s CLI dispatch), two independently-scheduled Databricks Jobs, structured run logging (`run_logger.py`). |
| Serving | `docs/reporting_views.sql` + Power BI | SQL views Power BI queries directly via a Databricks SQL warehouse (`infra/sql_warehouse.tf`). |

## Why this shape

Every arrow in the diagram is a real, tested function call, not aspirational
architecture — `orchestration/gold_stage.py` and `orchestration/process_stage.py`
literally wire the boxes above together. The one dotted-vs-solid distinction
that matters: `JOB_ALERTS` only ever writes to `bronze.nws_alerts_snapshots`;
it does not trigger Silver/Process/Gold itself (see
[batch_vs_streaming_memo.md](batch_vs_streaming_memo.md) for why).
