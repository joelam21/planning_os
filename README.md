# planning_os

## Overview
`planning_os` is an end-to-end analytics system designed to simulate a production-grade data platform.

It demonstrates how raw data can be ingested, modeled, and transformed into business insights using a modern data stack:

- Python (data ingestion)
- Snowflake (data warehouse)
- dbt (transformations and modeling)
- Jupyter (analysis and visualization)

The project uses Iowa Liquor Sales data as a working dataset to explore retail purchasing behavior.

---

## Objective
Build a scalable analytics system that:

- Ingests external data into a warehouse
- Models data using dimensional design (facts & dimensions)
- Applies data quality testing
- Enables downstream analysis and insight generation

---

## Architecture

```
API → Python Ingestion → Snowflake (RAW)
     → dbt (staging → intermediate → marts)
     → Analysis (Jupyter / BI)
```

### Components

- **Ingestion**: Python-based pipeline with parameterized date ranges and batching
- **Warehouse**: Snowflake (RAW and DEV schemas)
- **Transformations**: dbt models
  - staging → cleaned raw data
  - intermediate → structured transformations
  - marts → fact and dimension tables
- **Analysis**: Jupyter notebooks for exploratory analysis and visualization

---

## Data Model

### Fact Tables
- `fct_liquor_sales` → atomic grain (invoice line)
- `fct_store_daily_sales` → aggregated grain (store × day)

### Dimension Tables
- `dim_store`
  - Type 1 (current-state)
  - latest record per `store_number`
  - future enhancement: SCD Type 2
- `dim_item`

---

## Key Insights (So Far)

- Market shows a hybrid structure:
  - dominant chains
  - fragmented long tail
- Independent stores collectively represent significant volume
- Per-store performance varies by retail model (warehouse vs grocery vs convenience)
- Dataset reflects **sell-in (store purchases)**, not POS consumer demand

---

## Important Data Context

This dataset represents **store purchases from the state (wholesale / sell-in)**.

Implications:
- Daily data reflects ordering behavior, not consumer demand
- Missing or low-volume days are expected
- Weekly or monthly aggregation is more appropriate for analysis

---

## Current State

- Ingestion pipeline working with parameterized runs
- dbt models implemented and tested
- Dimensional model established
- Initial analysis notebook created
- Data currently limited (partial historical coverage)

---

## Quickstart

```bash
# Enter environment
source ./enter.sh

# Run environment checks
./doctor.sh

# List available commands
./run.sh help

# Validate dbt configuration
./run.sh dbt-debug
```

---

## Next Steps

- Expand historical data coverage (month-by-month ingestion)
- Improve chain classification as dataset grows
- Implement idempotent incremental ingestion
- Add orchestration (Airflow or equivalent)
- Explore time series and forecasting

---

## Repository Structure

See: `docs/REPO_MAP.md`

## Pipeline Health

### What constitutes a healthy weekly run

A run is considered **healthy** when all of the following pass:

| Check | Tool | Threshold | Failure type |
|---|---|---|---|
| Source freshness | `dbt source freshness` | loaded within 7 days | WARN > 7d / ERROR > 14d |
| All schema tests | `dbt test` | PASS=N, WARN=0, ERROR=0 | ERROR blocks merge |
| Grain integrity | `assert_fct_store_daily_no_duplicate_store_day` | 0 rows returned | ERROR |
| Reconciliation | `assert_fct_store_daily_matches_fact_aggregates` | 0 rows returned | ERROR |
| Date coverage | `assert_no_dates_lost_in_staging` | 0 rows returned | ERROR |
| Business rules | `assert_no_negative_*` | 0 rows returned | ERROR |
| Pipeline health view | `MON_PIPELINE_HEALTH.freshness_status` | PASS | WARN/ERROR triggers manual review |

### Run sequence

```bash
dbt source freshness
dbt build
snow sql -c my_snowflake -q "select * from PLANNING_OS.DEV.MON_PIPELINE_HEALTH"
```
