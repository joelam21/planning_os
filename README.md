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

## Flagship Use Case: SKU Rationalization and Replenishment Planning

This project’s strongest business use case is SKU rationalization: identifying which products drive the majority of statewide volume and revenue, and which products add operational complexity with limited commercial return.

### Business question

If a retailer or distributor had to simplify the active catalog, which SKUs should be protected as core assortment, which should be reviewed, and which likely belong in the long tail?

### Analytical approach

I built a dbt mart that classifies each SKU using a trailing 12-week Pareto analysis across two dimensions:
- volume contribution
- revenue contribution

This separates products that are operationally important from products that are financially important, instead of collapsing both questions into a single rank.

Supporting models then extend that analysis into planning workflows:
- SKU velocity segmentation for assortment and catalog productivity decisions
- replenishment baseline forecasting for weekly shipment recommendations

### What the model enables

- Identifies the small set of SKUs responsible for the majority of statewide movement
- Quantifies the long tail of low-productivity items that consume shelf space and working capital
- Distinguishes between items that are high-volume, high-revenue, or both
- Creates a bridge from descriptive analytics into operational decision support

### Business takeaway

The catalog follows a classic Pareto pattern: a relatively small share of SKUs drives a disproportionate share of sales, while a long tail of products contributes little volume or revenue.

That makes SKU rationalization a credible operating lever:
- protect core items that anchor demand
- review marginal SKUs that add assortment complexity without meaningful payoff
- use recent depletion patterns to size replenishment more intentionally

### Portfolio value

This use case demonstrates more than dashboarding. It shows an end-to-end workflow:
- ingest external data
- model atomic and aggregated facts in Snowflake with dbt
- apply data tests and historical snapshots
- translate warehouse models into a planning recommendation

The analysis artifact for this use case lives in `notebooks/02_sku_velocity_analysis.ipynb`, backed by the `fct_sku_velocity` and `fct_replenishment_forecast` marts in dbt.

## Early Findings

- The market shows a classic long-tail catalog pattern: a small share of SKUs drives a disproportionate share of statewide volume and revenue.
- Store performance is heterogeneous across retail formats, with different operating models showing materially different revenue productivity.
- Independent stores remain commercially meaningful as a group even when large chains dominate individual rankings.
- Because the dataset reflects wholesale sell-in rather than consumer sell-through, weekly aggregation is more reliable than daily demand-style interpretation.
- Negative sales and bottle values observed in late July 2022 were validated as return invoices (RINV-), not transformation errors.

---

## Architecture

```text
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
- `fct_sku_velocity` → trailing-12-week SKU Pareto classification across volume and revenue
- `fct_replenishment_forecast` → baseline weekly replenishment recommendation using recent depletion trends

### Dimension Tables
- `dim_store` → Type 1 (current-state), derived from `snap_store`
- `dim_item` → Type 1 (current-state), derived from `snap_item`

### Snapshots
- `snap_store` → Type 2 (SCD), full store attribute history
- `snap_item` → Type 2 (SCD), full item attribute history

---


## Important Data Context

This dataset represents **store purchases from the state (wholesale / sell-in)**.

Implications:
- Daily data reflects ordering behavior, not consumer demand
- Missing or low-volume days are expected
- Weekly or monthly aggregation is more appropriate for analysis

Returns handling:
- Source data includes legitimate return invoices, identified by invoice_item_number starting with RINV-
- Return rows are typically negative but rare positive reversals/corrections may occur
- Data quality test ensures negative values only appear on RINV invoices (non-returns cannot be negative)
- Anomalous returns (positive RINV records) are identified in `int_anomalous_returns` model and monitored periodically; these are rare but preserved in fact table to maintain data lineage

---

## Current State

- Parameterized ingestion pipeline running into Snowflake
- dbt project with staging, intermediate, marts, tests, and snapshots
- Current-state dimensions built from Type 2 historical snapshots
- Store performance analysis completed
- SKU rationalization mart completed
- Replenishment forecast mart completed
- Historical data coverage is still being expanded incrementally

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

- Expand historical coverage through full 2021 and 2022
- Add formal dbt exposures for the SKU and replenishment notebooks
- Strengthen README case-study framing with visual outputs from the notebooks
- Add orchestration and alerting for scheduled pipeline runs
- Continue extending planning-layer marts beyond descriptive reporting

---

## Repository Structure

See: `docs/REPO_MAP.md`

## Pipeline Health

### What constitutes a healthy weekly run

A run is considered **healthy** when all of the following pass:

| Check | Tool | Threshold | Failure type |
|---|---|---|---|
| Source freshness | `dbt source freshness` | loaded within 7 days | WARN > 7d / ERROR > 14d |
| All schema tests | `dbt test` | ERROR=0; documented warn-only exceptions allowed | ERROR blocks merge |
| Grain integrity | `assert_fct_store_daily_no_duplicate_store_day` | 0 rows returned | ERROR |
| Reconciliation | `assert_fct_store_daily_matches_fact_aggregates` | 0 rows returned | ERROR |
| Date coverage | `assert_no_dates_lost_in_staging` | 0 rows returned | ERROR |
| Business rules | `assert_negative_values_must_be_returns` | 0 rows returned | ERROR |
| Anomaly monitoring | `int_anomalous_returns` | Review periodically | INFORMATIONAL |
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

