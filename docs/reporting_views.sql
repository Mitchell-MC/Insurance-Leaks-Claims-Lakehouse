-- Power BI Reporting Views
--
-- One view per dashboard element described in dashboard_usage_guide.md,
-- built directly on the Gold tables. Power BI connects to these views (not
-- the raw fact/dim tables) via a Databricks SQL warehouse
-- (infra/sql_warehouse.tf), so dashboard logic lives here in version-
-- controlled SQL rather than in Power BI's own query editor.

-- ============================================================================
-- View: v_elevated_pressure_regions
-- Card: "regions at elevated catastrophe pressure"
-- ============================================================================
CREATE OR REPLACE VIEW gold.v_elevated_pressure_regions AS
SELECT
    g.state,
    g.county_name,
    f.claims_surge_risk,
    d.date
FROM gold.fact_regional_alert_activity f
JOIN gold.dim_geography g ON f.geography_key = g.geography_key
JOIN gold.dim_date d ON f.date_key = d.date_key
WHERE f.claims_surge_risk >= 0.7  -- Reason: top ~30% of the [0,1] normalized
                                    -- range, a simple fixed threshold chosen
                                    -- for interpretability over a percentile
                                    -- cutoff that shifts as more regions are
                                    -- added; revisit once real multi-region
                                    -- history exists to calibrate against.
ORDER BY f.claims_surge_risk DESC;

-- ============================================================================
-- View: v_complaint_trend_acceleration
-- Card: "complaint trend acceleration"
-- Uses the KPI-2 fallback proxy -- see docs/data_limitations.md. Once a
-- structured complaint source lands, this view's WHERE/ORDER stay the same;
-- only the underlying fact_complaint_trend population changes.
-- ============================================================================
CREATE OR REPLACE VIEW gold.v_complaint_trend_acceleration AS
SELECT
    g.state,
    g.county_name,
    d.date,
    c.complaint_count,
    c.complaint_rate_trend
FROM gold.fact_complaint_trend c
JOIN gold.dim_geography g ON c.geography_key = g.geography_key
JOIN gold.dim_date d ON c.date_key = d.date_key
WHERE c.complaint_rate_trend > 0
ORDER BY c.complaint_rate_trend DESC;

-- ============================================================================
-- View: v_catastrophe_pressure_map
-- Map: catastrophe pressure by state/county, drill-through to event/complaint context
-- ============================================================================
CREATE OR REPLACE VIEW gold.v_catastrophe_pressure_map AS
SELECT
    g.state,
    g.county_name,
    g.county_geoid,
    f.claims_surge_risk,
    e.matched_storm_event_count,
    e.total_damage_property_usd,
    e.days_to_declaration
FROM gold.fact_regional_alert_activity f
JOIN gold.dim_geography g ON f.geography_key = g.geography_key
LEFT JOIN gold.fact_catastrophe_event e ON e.geography_key = g.geography_key;

-- ============================================================================
-- View: v_activity_trend_over_time
-- Trend visuals: active alerts, historical declarations, complaint movement
-- ============================================================================
CREATE OR REPLACE VIEW gold.v_activity_trend_over_time AS
SELECT
    d.date,
    g.state,
    AVG(f.claims_surge_risk) AS avg_claims_surge_risk,
    COUNT(DISTINCT e.disasterNumber) AS declaration_count,
    COALESCE(SUM(c.complaint_count), 0) AS complaint_count
FROM gold.dim_date d
JOIN gold.fact_regional_alert_activity f ON f.date_key = d.date_key
JOIN gold.dim_geography g ON f.geography_key = g.geography_key
LEFT JOIN gold.fact_catastrophe_event e
    ON e.date_key = d.date_key AND e.geography_key = g.geography_key
LEFT JOIN gold.fact_complaint_trend c
    ON c.date_key = d.date_key AND c.geography_key = g.geography_key
GROUP BY d.date, g.state
ORDER BY d.date;

-- ============================================================================
-- View: v_roi_summary
-- ROI section: estimated value from earlier adjuster deployment.
-- Formula and named assumptions documented in docs/roi_assumptions.md --
-- this view only joins the inputs the formula needs.
-- ============================================================================
CREATE OR REPLACE VIEW gold.v_roi_summary AS
SELECT
    g.state,
    AVG(e.days_to_declaration) AS avg_days_to_declaration,
    COUNT(DISTINCT e.disasterNumber) AS declaration_count,
    SUM(e.total_damage_property_usd) AS total_damage_property_usd
FROM gold.fact_catastrophe_event e
JOIN gold.dim_geography g ON e.geography_key = g.geography_key
GROUP BY g.state;
