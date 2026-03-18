# planning_os

Repository structure: see docs/REPO_MAP.md

Purpose: End-to-end analytics platform for ingesting, modeling, and analyzing operational data. The system is designed to simulate a production-grade data stack, including raw data ingestion (Python), transformation layers (dbt), and warehouse storage (Snowflake).

## Architecture (initial)
- Warehouse: Snowflake
- Transforms: dbt
- Ingestion: Python (project-local)

## Current State
- Ingestion framework supports multiple sources (sample + Iowa Liquor API)
- Parameterized ingestion (date range, batch size, max batches)
- Supports authenticated API ingestion via environment variables
- Raw layer tables created in Snowflake (RAW_*)
- Idempotent reload behavior for controlled backfills

## Quickstart
1) Enter environment:```bash
source ./enter.sh
```
2) Doctor check:```bash
./doctor.sh
```
3) List commands:```bash
./run.sh help
```
4) Validate dbt configuration:```bash
./run.sh dbt-debug
```
