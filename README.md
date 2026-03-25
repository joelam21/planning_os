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
- `dim_store` → Type 1 (current-state), derived from `snap_store`
- `dim_item` → Type 1 (current-state), derived from `snap_item`

### Snapshots
- `snap_store` → Type 2 (SCD), full store attribute history
- `snap_item` → Type 2 (SCD), full item attribute history

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
- Harden automated scheduling and failure notifications
- Add lightweight CI for dbt parse/validation checks
- Evolve chain classification logic as data volume grows
- Expand analysis with time series and product-mix segmentation

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

### Scheduled weekly run (example)

Use the parameterized pipeline command to run a bounded ingestion window plus snapshot/transform/test:

```bash
./run.sh pipeline \
  --source iowa_liquor \
  --start-date 2021-08-01 \
  --end-date 2021-08-07 \
  --batch-size 1000 \
  --max-batches 300
```

### Dimension Design: Type 1 vs Type 2

**Current-state dimensions (Type 1)**
- `dim_store` → latest known attributes per store, derived from `snap_store`
- `dim_item` → latest known attributes per item, derived from `snap_item`

**Historical snapshots (Type 2)**
- `snap_store` → full attribute history per store with `dbt_valid_from` / `dbt_valid_to` effective dating
- `snap_item` → full attribute history per item with `dbt_valid_from` / `dbt_valid_to` effective dating

**Use the current-state dimension when:**
- joining to facts for standard reporting
- you need a single current label per entity

**Use the snapshot directly when:**
- you need to know what attributes were true on a specific date
- you are analyzing trends in entity attributes over time
- you are auditing how a store or item was classified historically

Example: to join a fact row to the store attributes that were true *at the time of the transaction*:
```sql
select
    f.invoice_item_number,
    f.order_date,
    f.sale_dollars,
    s.store_name,
    s.chain
from fct_liquor_sales f
left join snap_store s
    on f.store_number = s.store_number
   and f.order_date >= s.dbt_valid_from
   and (f.order_date < s.dbt_valid_to or s.dbt_valid_to is null)
```
