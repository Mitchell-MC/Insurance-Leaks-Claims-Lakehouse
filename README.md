# Insurance Claims Leakage & Catastrophe Response Analytics Lakehouse

A Databricks lakehouse that ingests FEMA disaster declarations, NOAA storm
events, and NWS weather alerts to model catastrophe-driven insurance claims
leakage risk, published through a Power BI executive dashboard.

See [docs/interview_walkthrough.md](docs/interview_walkthrough.md) for a
5-minute guided tour, or [docs/architecture.md](docs/architecture.md) for
the full pipeline diagram.
[docs/architecture_ideal_vs_actual.md](docs/architecture_ideal_vs_actual.md)
contrasts the target architecture with what is actually built, and why each
divergence exists.

## Status

All 8 phases of the build are complete:

- **Phase 0** — environment/infra scaffolded (`infra/`); Terraform apply
  and GitHub remote still pending an Azure subscription — see `infra/README.md`.
- **Phase 1** — business framing & KPI scoping: `docs/charter.md`,
  `docs/kpi_definitions.md`, `docs/scoping_memo.md`.
- **Phase 2** — Bronze ingestion: `src/lakehouse/ingestion/` (FEMA, NOAA
  1996-present, NWS alerts, Census geography), each attaching
  `_ingestion_metadata`.
- **Phase 3** — Silver standardization: `src/lakehouse/silver/` (state-code
  reconciliation, dedup, severity banding, `_dq_metadata`), known gaps in
  `docs/data_limitations.md`.
- **Phase 4** — distributed processing: `src/lakehouse/processing/`
  (region/date-window join, rolling 7d/30d window metrics,
  `claims_surge_risk` scoring), benchmarked in
  `docs/partitioning_benchmark_memo.md`.
- **Phase 5** — Gold star schema: `src/lakehouse/gold/` (5 dimensions, 3
  facts, `leakage_exposure_proxy`), validated by `docs/kpi_validation_queries.sql`.
  State-grain facts key off `dim_geography_state`; see
  `docs/interview_walkthrough.md`'s "Modeling approach" for why.
- **Phase 6** — orchestration: `src/lakehouse/orchestration/` +
  `src/lakehouse/main.py` (CLI entry point) + `infra/jobs.tf` (two
  independently-scheduled Databricks Jobs), reasoning in
  `docs/batch_vs_streaming_memo.md`.
- **Phase 7** — Power BI spec: `docs/reporting_views.sql`,
  `docs/roi_assumptions.md`, `docs/dashboard_usage_guide.md`,
  `infra/sql_warehouse.tf`. The `.pbix` itself is a manual Power BI Desktop
  step against these views — not something this repo can author.
- **Phase 8** — portfolio packaging: this README, `docs/architecture.md`,
  `docs/interview_walkthrough.md`, `docs/productionization_next_steps.md`.

All Python code is unit-tested locally (mocked HTTP + a local Spark
session — see `src/lakehouse/conftest.py`); running against real Databricks
storage is pending the Phase 0 Terraform apply. Terraform files are written
but not applied or `terraform validate`-checked (no `terraform` binary in
this dev environment) — review before applying. Set
`LAKEHOUSE_NWS_USER_AGENT` to a real contact string before calling the live
NWS API.

## Repository layout

```
src/lakehouse/
    config/           Typed, environment-driven settings (LakehouseSettings)
    ingestion/        Bronze: BaseIngestor + one feature slice per source
    silver/           Silver: BaseSilverTransformer + data_quality checks +
                       one feature slice per source, us_state_codes.py
    processing/        Phase 4: regional join, rolling window metrics,
                       catastrophe pressure score
    gold/             Gold: dimensions/, facts/, leakage_risk_metric
    orchestration/    Stage runners (ingest/silver/process/gold) + run_logger
    main.py           CLI entry point (`lakehouse <stage>`), the console
                       script infra/jobs.tf's Databricks Jobs invoke
infra/                Terraform: Azure resource group, Databricks workspace,
                       Unity Catalog schemas, verification cluster, two
                       scheduled jobs, Power BI SQL warehouse
.github/workflows/    CI: ruff, mypy, pytest on every PR into develop
docs/                 Charter, KPI definitions, architecture/decision memos,
                       data limitations, dashboard spec, interview walkthrough
```

## Setup

```bash
uv venv
uv sync --all-groups
uv pip install -e .
```

## Development

```bash
uv run pytest
uv run ruff check .
uv run ruff format .
uv run mypy .
```

## Running a pipeline stage locally

```bash
uv run lakehouse ingest --source historical   # FEMA + NOAA + geography
uv run lakehouse ingest --source nws_alerts_snapshots
uv run lakehouse silver
uv run lakehouse process
uv run lakehouse gold
```

Requires a real Databricks-attached SparkSession and Unity Catalog storage
to actually read/write; see `infra/README.md` for provisioning.

## Branching

`main` is the stable/production branch, `develop` is the integration branch.
Feature branches: `feature/*`, `fix/*`, `docs/*`, `refactor/*`, branched from
`develop`.
