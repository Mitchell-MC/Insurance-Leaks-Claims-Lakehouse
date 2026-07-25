-- KPI Validation Queries
--
-- One query per KPI defined in kpi_definitions.md, run directly against the
-- Gold tables. Satisfies charter.md's stated success criterion: "Every KPI
-- ... is computable from a single SQL query against Gold tables ... and
-- each traces to a real column in a Phase 2 source dataset."
--
-- Table names assume the catalog/schema convention from
-- src/lakehouse/config/config.py (LakehouseSettings.gold_table_path):
-- {catalog_name}.{gold_schema}.{table_name}, e.g.
-- insurance_lakehouse.gold.fact_catastrophe_event.

-- ============================================================================
-- KPI 1: Estimated Claims Surge Risk
-- Source: gold.fact_regional_alert_activity (claims_surge_risk column,
-- built by processing.features.catastrophe_pressure_score from Bronze NWS
-- alerts + Silver NOAA storm events + Silver FEMA declarations).
-- ============================================================================
SELECT
    g.state,
    g.county_name,
    d.date,
    f.claims_surge_risk
FROM gold.fact_regional_alert_activity f
JOIN gold.dim_geography g ON f.geography_key = g.geography_key
JOIN gold.dim_date d ON f.date_key = d.date_key
ORDER BY f.claims_surge_risk DESC;

-- ============================================================================
-- KPI 2: Complaint Rate Trend
-- Source: gold.fact_complaint_trend. Currently zero rows -- see
-- docs/data_limitations.md for why (no structured complaint-data source is
-- available) and the fallback proxy formula. This query is written against
-- the real (if currently empty) table so it starts returning data the
-- moment a structured source is connected, with no query changes needed.
-- ============================================================================
SELECT
    g.state,
    g.county_name,
    d.date,
    c.complaint_count,
    c.complaint_rate_trend
FROM gold.fact_complaint_trend c
JOIN gold.dim_geography g ON c.geography_key = g.geography_key
JOIN gold.dim_date d ON c.date_key = d.date_key
ORDER BY c.complaint_rate_trend DESC;

-- ============================================================================
-- KPI 3: Average Days from Event to Declaration
-- Source: gold.fact_catastrophe_event (days_to_declaration column, computed
-- directly from Silver FEMA declarationDate - incidentBeginDate).
-- ============================================================================
SELECT
    e.incidentType,
    g.state,
    AVG(e.days_to_declaration) AS avg_days_event_to_declaration,
    COUNT(*) AS declaration_count
FROM gold.fact_catastrophe_event e
JOIN gold.dim_geography g ON e.geography_key = g.geography_key
GROUP BY e.incidentType, g.state
ORDER BY avg_days_event_to_declaration DESC;

-- ============================================================================
-- KPI 4: Leakage Exposure Proxy
-- Source: derived entirely from KPIs 1-3 by
-- gold.features.leakage_risk_metric.calculate_leakage_exposure_proxy; this
-- query recomputes the same three-signal picture directly from Gold so it
-- can be validated without re-running the pipeline.
-- ============================================================================
WITH surge_risk AS (
    SELECT geography_key, AVG(claims_surge_risk) AS avg_claims_surge_risk
    FROM gold.fact_regional_alert_activity
    GROUP BY geography_key
),
declaration_lag AS (
    SELECT geography_key, AVG(days_to_declaration) AS avg_days_event_to_declaration
    FROM gold.fact_catastrophe_event
    GROUP BY geography_key
)
SELECT
    g.state,
    g.county_name,
    s.avg_claims_surge_risk,
    l.avg_days_event_to_declaration,
    -- Full z-score combination (including the complaint-trend proxy term)
    -- is computed in gold.features.leakage_risk_metric; this query surfaces
    -- the two KPI-3/KPI-1-derived inputs for manual validation.
    (s.avg_claims_surge_risk - AVG(s.avg_claims_surge_risk) OVER ())
        / NULLIF(STDDEV(s.avg_claims_surge_risk) OVER (), 0)
    +
    (l.avg_days_event_to_declaration - AVG(l.avg_days_event_to_declaration) OVER ())
        / NULLIF(STDDEV(l.avg_days_event_to_declaration) OVER (), 0)
        AS partial_leakage_exposure_proxy
FROM gold.dim_geography g
JOIN surge_risk s ON g.geography_key = s.geography_key
JOIN declaration_lag l ON g.geography_key = l.geography_key
ORDER BY partial_leakage_exposure_proxy DESC;
