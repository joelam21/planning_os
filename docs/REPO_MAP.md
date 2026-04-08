# Repository Map

This document explains the current structure and operational purpose of the `planning_os` repository.

The goal of this repo is to build a production-style analytics system for ingestion, transformation, validation, and analysis using Iowa Liquor Sales data — demonstrating how raw transactional data can be modeled into trusted, decision-ready analytical artifacts.

---

## Root Directory

Primary entrypoints and operational scripts.

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
- `pipeline` (ingest → snapshot → transform → test), including optional date-window parameters

`doctor.sh`
Environment diagnostics and setup validation.

`requirements.txt`
Curated direct Python and dbt-related dependencies for normal installation.

`requirements-lock.txt`
Exact pinned dependency set used to reproduce a known working environment.

`README.md`
Project overview, business use cases, architecture, key findings, known data limitations, and pipeline health criteria.

`.github/workflows/dbt-ci.yml`
GitHub Actions workflow that validates dbt in an isolated CI schema via debug, staged runs, snapshots, marts build, singular tests, and source freshness.

---

## dbt/

Transformation and semantic modeling layer.

### dbt/models/sources/
- Source definitions and freshness checks
- `raw.yml` defines `RAW_IOWA_LIQUOR` and freshness thresholds

### dbt/models/staging/
- Cleaned and typed raw-source models
- `stg_iowa_liquor.sql` and associated tests

### dbt/models/int/
- Intermediate business logic and derived metrics
- `int_iowa_liquor_sales.sql` — cleaned sales with return handling
- `int_iowa_liquor_sales_deduped.sql` — deduplicated invoice lines
- `int_iowa_liquor_sales_duplicate_audit.sql` — audit model for duplicate detection
- `int_item_business_history.sql` — historical item attribute versioning with business-effective dating and change-point detection
- `int_item_business_pricing_history.sql` — package-size normalization, normalized pricing, and price position segmentation layered on historical item versions
- `int_store_attributes.sql` — store attributes with chain classification
- `int_anomalous_returns.sql` — monitoring model for positive RINV records
- `int_unlabeled_negative_returns.sql` — monitoring model for unlabeled negative rows

### dbt/models/marts/
- Facts, dimensions, and monitoring models — materialized as tables for query performance

**Fact tables:**
- `fct_liquor_sales` — atomic grain (invoice line), preserves source signs for returns
- `fct_store_daily_sales` — store × day aggregate
- `fct_sku_velocity` — trailing-12-week SKU Pareto classification across volume and revenue; excludes RINV rows
- `fct_replenishment_forecast` — baseline weekly replenishment recommendation using recent depletion trends

**Dimension tables:**
- `dim_store` — Type 1 current-state, derived from `snap_store`
- `dim_item` — Type 1 current-state, derived from `snap_item`
- `dim_item_business_history` — final analyst-facing Type 2 historical item dimension exposed from the intermediate pricing/history layer

`schema.yml` — model-level metadata, column documentation, and tests

### dbt/models/monitoring/
- `mon_pipeline_health.sql` — operational health summary view aggregating source freshness, pipeline run status, and data quality signal into a single monitoring artifact

### dbt/snapshots/
- Type 2 historical entity tracking
- `snap_store.sql` — full store attribute history (system-time based)
- `snap_item.sql` — full item attribute history (system-time based)
- `snapshots.yml` — metadata for snapshot documentation

### dbt/tests/
Singular data tests enforcing business rules, grain integrity, reconciliation, and data quality constraints:

- `assert_fct_store_daily_matches_fact_aggregates.sql` — reconciles store daily aggregates against atomic fact
- `assert_fct_store_daily_no_duplicate_store_day.sql` — grain integrity check for store × day uniqueness
- `assert_negative_values_must_be_returns.sql` — ensures negative sales/volume values only appear on RINV rows
- `assert_no_dates_lost_in_staging.sql` — validates date coverage is preserved through the staging layer
- `assert_retail_gte_cost.sql` — warns when retail price falls below cost (threshold-based, warn-only)
- `assert_sku_velocity_nonnegative_metrics.sql` — ensures velocity metrics are non-negative after RINV exclusion
- `assert_sku_velocity_pct_bounds.sql` — validates cumulative percentage columns stay within 0-100 bounds
- `recommended_ship_qty.sql` — validates replenishment recommendation quantities are non-negative

### dbt/models/docs/
- Centralized reusable column definitions (`column_definitions.md`) used via `{{ doc(...) }}` references

### dbt/models/exposures.yml
- Notebook exposures and model lineage for analysis dependencies

---

## analysis/

Reusable analytical layer backing the Jupyter notebooks. Business logic is maintained outside notebook cells so it stays versioned and testable independently.

### analysis/sql/
Parameterized SQL templates for all major analytical views. Each template is rendered at runtime with window parameters (month_start, trailing_weeks, category filters, vendor filters).

Key templates include:
- `category_family_growth.sql` — YoY and trailing window growth by category family
- `category_family_drilldown.sql` — category-level breakdown within a family
- `category_vendor_monthly_trend.sql` — monthly vendor revenue for top-N vendors within a category family; smoothing is applied in the chart layer, not in SQL
- `category_family_vendor_share_3y.sql` — vendor share donut data for before/after comparison
- `category_vendor_volume_growth.sql` — vendor × bottle volume growth detail
- `category_family_vendor_price_segment_mix.sql` — vendor revenue by price position segment
- `category_family_vendor_price_segment_mix_compare.sql` — before/after price segment comparison
- `category_item_growth.sql` — item-level growth with detail rows and grand total
- `category_chain_growth.sql` — chain-level growth grouped by chain value with grand total
- `sku_velocity_tier_summary.sql` — tier distribution summary by volume and revenue
- `sku_velocity_tier_matrix.sql` — SKU count and revenue by volume × revenue tier cell
- `sku_velocity_tier_divergent.sql` — item-level detail for off-diagonal tier cells
- `sku_velocity_tier_by_category.sql` — tier distribution broken down by category family
- `sku_velocity_full_scatter.sql` — full catalog scatter data with tier and group classification
- `category_family_item_growth.sql` — item-level growth within a category family with Top N + Other structure
- `category_family_vendor_category_mix.sql` — vendor revenue breakdown by category within a family
- `category_family_vendor_growth.sql` — vendor-level growth summary within a category family
- `category_family_vendor_price_segment_mix_grouped.sql` — vendor price segment mix with simplified tier grouping
- `category_family_vendor_store_channel_mix.sql` — vendor revenue breakdown by store channel within a family
- `category_family_vendor_store_channel_mix_compare.sql` — before/after store channel mix comparison by vendor
- `category_name_vendor_monthly_trend.sql` — monthly vendor revenue trend within a specific category name
- `store_chain_performance.sql` — store-level chain performance and productivity
- `store_channel_performance.sql` — store channel structure and revenue concentration

### analysis/python/
- `charts.py` — reusable chart functions for all analysis notebooks. Functions handle title formatting, color palette, legend placement, axis scaling, and annotation consistently across charts.
- `notebook_helpers.py` — SQL template reading, rendering, and execution helpers; project root detection; Snowflake engine construction.

---

## ingestion/

Python ingestion pipeline and Snowflake loading helpers.

`ingestion/run_ingestion.py`
CLI ingestion runner with source and date window parameters.

`ingestion/sources/iowa_liquor.py`
Socrata API fetch logic with batching and optional date filters.

`ingestion/common/snowflake.py`
Snowflake connection, table creation, insert, and date-range deletion helpers.

Additional utility files in the ingestion directory include `config.py` (configuration helpers) and `sample.py` (data sampling utilities).

---

## notebooks/

Analysis and validation notebooks. The primary analysis notebooks are backed by the reusable SQL and chart layers in `analysis/` — business logic does not live inside notebook cells.

`03_store_performance_analysis.ipynb`
Market structure and store productivity analysis. Explores revenue concentration across store formats, chain vs. independent dynamics, and store-level performance patterns.

`02_sku_velocity_analysis.ipynb`
SKU rationalization, catalog productivity, and inventory velocity. Dual-dimension Pareto classification across volume and revenue, tier alignment matrix, item-level archetype analysis, category family breakdown, and full catalog scatter visualization.

`01_category_growth_analysis.ipynb`
Tequila category growth, vendor share dynamics, and price segment mix. Covers category family context, vendor share evolution 2021-2025, CAGR comparison, before/after price position segmentation, item-level drill-down, and chain-level analysis.

`query_exploration_notebook.ipynb`
Ad hoc validation and local exploration notebook including pipeline health checks. Not intended as a shared analysis artifact.

---

## docs/

Project documentation.

`REPO_MAP.md` — this document
`PROJECT_START.md` — grain decisions, drift rules, and design constraints captured at project start
`MODEL_CHECKLIST.md` — model design checklist enforcing grain, primary key, and business question articulation before building

---

## setup/

Snowflake infrastructure and access setup scripts.

`snowflake_objects.sql` — creates the Snowflake warehouse, database, and schemas required to run the project: `PLANNING_OS.RAW` (ingestion landing zone), `PLANNING_OS.DEV` (dbt transformation output), and `PLANNING_OS.CI` (CI/CD isolated schema). Includes auto-suspend configuration for cost efficiency and `IF NOT EXISTS` guards for safe re-execution. Run before `snowflake_roles.sql`.

`snowflake_roles.sql` — creates project roles and grants warehouse, database, and schema privileges for admin, development, CI, and read-only access patterns.

---

## scripts/

Automation and workflow helper scripts for setup, bootstrap, and local workflows.

---

## logs/

Runtime and query logs for local diagnostics and troubleshooting.

---

## .venv/

Local Python virtual environment (not tracked in Git).

---

## Current Project State

**Completed:**
- Parameterized ingestion pipeline with date-window loading into Snowflake
- dbt layered modeling: staging → intermediate → marts (tables) → snapshots
- Atomic and aggregated fact models
- Type 1 current-state dimensions backed by Type 2 snapshots
- Engineered dimensions: category families, store chains, price position segments
- Custom data quality tests: business rules, grain integrity, reconciliation, date completeness
- Return-aware data quality policy for sales sign handling
- Deduplicated intermediate invoice layer protecting fact grain integrity
- Anomalous returns monitoring for rare positive RINV rows
- SKU velocity hardening to exclude returns from trailing-window ranking logic
- Source freshness checks and pipeline health monitoring model
- Parameterized pipeline execution via `run.sh`
- GitHub Actions dbt CI workflow using an isolated CI schema
- Notebook exposures and centralized dbt column documentation
- Category growth and vendor share analysis — tequila market 2021-2025
- SKU rationalization and catalog productivity analysis — statewide market
- Store performance and channel structure analysis — chain vs. independent revenue productivity
- Reusable chart and SQL template layers backing all primary analysis notebooks

**Next steps:**
- Add Airflow orchestration — DAG to automate the weekly ingestion → dbt build → dbt test → monitoring sequence
- Expand CI beyond `dbt parse` — add `dbt build` against a CI-specific schema with slim invocation
- Synthetic demand layer — simulate consumer demand from sell-in patterns to enable inventory position modeling
- NRF 4-5-4 fiscal calendar dimension — enable period-comparable analysis across the standard retail planning calendar
- Store-level planning simulation — model replenishment at the store level for a subset of stores across different demand profiles

---

## Design Principles

This repository is structured around:
- Explicit grain and model contracts documented before building
- Reproducible environments and deterministic workflows
- Validation-first iteration — tests are part of the build, not an afterthought
- ELT over ETL — raw data preserved in Snowflake for flexibility as business logic evolves
- Minimal drift between code, documentation, and operations
- Business logic versioned outside notebooks — SQL templates and chart helpers are independently maintainable
- Long-term maintainability and operational clarity over short-term convenience
