Repository Map

This document explains the current structure and operational purpose of the `planning_os` repository.

The goal of this repo is to build a production-style analytics system for ingestion, transformation, validation, and analysis using Iowa liquor sales data.

---

Root Directory

These files are the primary entrypoints and operational scripts.

`enter.sh`
Activates the project local Python virtual environment (`.venv`) and loads environment context.

`run.sh`
Primary task runner for project workflows. Supports:
- `doctor`
- `snow-test`
- `snow-sql-test`
- `dbt-debug`
- `ingest`
- `transform`
- `test`
- `pipeline` (ingest -> snapshot -> transform -> test), including optional date-window parameters.

`doctor.sh`
Environment diagnostics and setup validation.

`requirements.txt`
Pinned Python and dbt-related dependencies.

`README.md`
Project overview, architecture, operational health criteria, and run procedures.

---

dbt/

Transformation and semantic modeling layer.

Current structure includes:

`dbt/models/sources/`
- source definitions and freshness checks
- `raw.yml` defines `RAW_IOWA_LIQUOR` and freshness thresholds

`dbt/models/staging/`
- cleaned and typed raw-source models
- `stg_iowa_liquor.sql` and associated tests

`dbt/models/int/`
- intermediate business logic and derived metrics
- `int_iowa_liquor_sales.sql`

`dbt/models/marts/`
- facts, dimensions, and monitoring models
- `fct_liquor_sales` (atomic grain)
- `fct_store_daily_sales` (store-day aggregate)
- `dim_store` (Type 1 current-state from snapshot)
- `dim_item` (Type 1 current-state from snapshot)
- `mon_pipeline_health` (operational health summary)
- `schema.yml` model-level metadata and tests

`dbt/snapshots/`
- Type 2 historical entity tracking
- `snap_store.sql`
- `snap_item.sql`
- `snapshots.yml` metadata for snapshot docs

`dbt/tests/`
- singular data tests including:
  - business-rule checks
  - fact-to-mart reconciliation checks
  - grain integrity checks
  - raw-to-staging date preservation checks

`dbt/models/docs/`
- centralized reusable column definitions (`column_definitions.md`) used via `{{ doc(...) }}` references.

`dbt/models/exposures.yml`
- notebook exposures and model lineage for analysis dependencies.

---

ingestion/

Python ingestion pipeline and Snowflake loading helpers.

`ingestion/run_ingestion.py`
CLI ingestion runner with source/date window parameters.

`ingestion/sources/iowa_liquor.py`
Socrata API fetch logic with batching and optional date filters.

`ingestion/common/snowflake.py`
Snowflake connection, table creation, insert, and date-range deletion helpers.

---

notebooks/

Exploratory analysis and validation notebooks.

`01_store_performance_analysis.ipynb`
Exploratory notebook for market structure and store productivity.

`02_sku_velocity_analysis.ipynb`
Primary analysis notebook for SKU rationalization, catalog productivity, and inventory velocity.

`03_category_growth_analysis.ipynb`
Primary analysis notebook for tequila category growth, vendor share shifts, and price segment mix.

`query_exploration_notebook.ipynb`
Ad hoc validation and local exploration notebook (including pipeline health checks); not intended as a shared analysis artifact.

---

docs/

Project documentation and references.

`PROJECT_START.md`
Project startup guidance.

`MODEL_CHECKLIST.md`
Model design checklist (grain, key, business question, layer placement).

`REPO_MAP.md`
This document.

---

scripts/

Automation and workflow helper scripts for setup/bootstrap and local workflows.

---

logs/

Runtime/query logs for local diagnostics and troubleshooting.

---

.venv/

Local Python virtual environment (not tracked in Git).

---

Current Project State

The repository is beyond scaffold stage and currently supports:

Completed:
- ingestion with parameterized date-window loading
- dbt layered modeling (staging -> intermediate -> marts)
- atomic and aggregated fact models
- Type 1 current-state dimensions backed by Type 2 snapshots
- custom data quality tests (business rules, grain, reconciliation, completeness)
- return-aware data quality policy for sales sign handling
- anomalous returns monitoring model for rare positive RINV rows
- SKU velocity hardening to exclude returns from trailing-window ranking logic
- source freshness checks
- pipeline health model
- parameterized pipeline execution in `run.sh`
- notebook exposures and centralized dbt column docs

Near-term roadmap:
- maintain current historical coverage through 2025+ and extend it as newer source data becomes available
- operationalize weekly warning/anomaly monitoring checks
- continue enrichment roadmap (chain category, SKU planning tiers)
- scheduling hardening and alerting once monitoring cadence is stable

---

Design Principles

This repository is structured around:
- explicit grain and model contracts
- reproducible environments and deterministic workflows
- validation-first iteration
- minimal drift between code, docs, and operations
- long-term maintainability and operational clarity
