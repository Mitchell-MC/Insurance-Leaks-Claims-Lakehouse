# Insurance Claims Leakage & Catastrophe Response Analytics Lakehouse

A Databricks lakehouse that ingests FEMA disaster declarations, NOAA storm
events, and NWS weather alerts to model catastrophe-driven insurance claims
leakage risk, published through a Power BI executive dashboard.

## Status

Phase 0 (environment, account & infra bootstrap) scaffolded — Terraform
apply and GitHub remote still pending, see `infra/README.md`. Phase 1
(business framing & KPI scoping) complete, see `docs/charter.md`,
`docs/kpi_definitions.md`, and `docs/scoping_memo.md`.

## Repository layout

```
src/lakehouse/    Python package: config, ingestion, silver, processing, gold,
                   orchestration, reporting modules (vertical-slice architecture)
infra/            Terraform: Azure resource group, Databricks workspace, Unity
                   Catalog catalog/schemas, verification cluster
.github/workflows/ CI: ruff, mypy, pytest on every PR into develop
docs/             Project charter, KPI definitions, architecture memos
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

## Branching

`main` is the stable/production branch, `develop` is the integration branch.
Feature branches: `feature/*`, `fix/*`, `docs/*`, `refactor/*`, branched from
`develop`.
