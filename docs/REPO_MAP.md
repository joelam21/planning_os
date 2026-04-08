# Repository Map

Fast orientation for the `planning_os` repository.

Use this document to find where things live.
Use [`README.md`](../README.md) for business framing, architecture, current state, quality gates, and analytical outcomes.

---

## Top-Level Entry Points

`enter.sh`  
Activates the local project environment.

`run.sh`  
Unified local workflow runner for ingestion, dbt, testing, and the full pipeline.

`doctor.sh`  
Environment validation and drift checks.

`requirements.txt`  
Curated direct dependencies.

`requirements-lock.txt`  
Exact pinned environment for reproducibility.

`.github/workflows/dbt-ci.yml`  
GitHub Actions CI workflow for dbt validation in the CI schema.

---

## Main Project Areas

### `dbt/`
Transformation and semantic modeling layer.

`dbt/models/staging/`  
Typed, cleaned source-shaped models.

`dbt/models/int/`  
Intermediate business logic, deduplication, historical versioning, pricing logic, and monitoring helpers.

`dbt/models/marts/`  
Analyst-facing facts and dimensions.

`dbt/models/monitoring/`  
Operational health and monitoring models.

`dbt/snapshots/`  
Type 2 historical snapshots for stores and items.

`dbt/tests/`  
Singular business-rule, grain, reconciliation, and data-quality tests.

`dbt/models/docs/`  
Reusable documentation blocks for columns and canonical metrics.

`dbt/models/exposures.yml`  
Notebook exposure definitions and lineage hooks.

### `analysis/`
Reusable analytical layer behind the notebooks.

`analysis/sql/`  
Parameterized SQL templates used by notebooks.

`analysis/python/charts.py`  
Shared plotting functions.

`analysis/python/notebook_helpers.py`  
Notebook utility functions for SQL rendering and execution.

### `ingestion/`
Python ingestion pipeline and Snowflake load helpers.

### `notebooks/`
Primary analytical artifacts:

- `01_category_growth_analysis.ipynb`
- `02_sku_velocity_analysis.ipynb`
- `03_store_performance_analysis.ipynb`
- `query_exploration_notebook.ipynb` for ad hoc validation

### `docs/`
Supporting project documentation.

- `MODEL_CHECKLIST.md` — durable model design guardrails
- `PROJECT_START.md` — original project design notes
- `REPO_MAP.md` — this file

### `setup/`
Snowflake object and role setup SQL.

### `scratch_sql/`
Local analysis and validation queries not treated as shared production artifacts.

### `logs/`
Local runtime and query logs.

---

## Most Important Models

`fct_liquor_sales`  
Atomic invoice-line fact.

`fct_store_daily_sales`  
Store-day aggregate fact.

`fct_sku_velocity`  
Trailing-window SKU productivity model.

`fct_replenishment_forecast`  
Baseline replenishment recommendation model.

`dim_store`  
Current-state store dimension.

`dim_item`  
Current-state item dimension.

`dim_item_business_history`  
Final historical item dimension for analyst-facing business use.

`int_item_business_history`  
Historical version construction from business-date change points.

`int_item_business_pricing_history`  
Historical pricing, package normalization, and price-position derivation.

---

## How To Read The Repo

If you are new to the project, the fastest path is:

1. Read [`README.md`](../README.md)
2. Look at the primary marts in `dbt/models/marts/`
3. Review the flagship notebook in `notebooks/01_category_growth_analysis.ipynb`
4. Inspect `analysis/sql/` and `analysis/python/` to see how notebook logic is externalized
