-- ============================================================================
-- OPS.PIPELINE_RUN_HISTORY setup reference
--
-- Recommended setup command:
--   dbt run-operation setup_ops_schema
--
-- This file mirrors the Snowflake-safe DDL used by the dbt macro so the checked-in
-- SQL matches the objects actually created in Snowflake.
-- ============================================================================

create schema if not exists planning_os.ops;

create table if not exists planning_os.ops.pipeline_run_history (
    -- Identifiers
    dag_id varchar(256) not null,
    run_id varchar(256) not null,
    logical_date timestamp_ntz,
    run_type varchar(64),

    -- Window parameters
    window_mode varchar(64),
    source varchar(64),
    start_date date,
    end_date date,
    window_days number,

    -- Ingestion batch parameters
    batch_size number,
    max_batches number,

    -- Landed data metrics
    ingested_row_count number,
    ingested_min_order_date date,
    ingested_max_order_date date,
    ingested_loaded_at_lag_hours number,

    -- Pipeline health
    freshness_status varchar(64),
    snapshot_status varchar(64),

    -- dbt execution
    dbt_build_status varchar(64),

    -- Final run state (success/failed/degraded)
    final_dag_state varchar(64) not null,

    -- Timing
    dag_start_ts timestamp_ntz,
    dag_end_ts timestamp_ntz,
    duration_seconds number,

    -- Metadata
    created_at timestamp_ntz default current_timestamp(),

    -- Snowflake-safe uniqueness constraint
    unique (run_id)
);



create or replace view planning_os.ops.v_pipeline_run_status as
select
    run_id,
    dag_id,
    logical_date,
    run_type,
    window_mode,
    source,
    start_date,
    end_date,
    window_days,
    batch_size,
    max_batches,
    ingested_row_count,
    ingested_min_order_date,
    ingested_max_order_date,
    ingested_loaded_at_lag_hours,
    freshness_status,
    snapshot_status,
    dbt_build_status,
    final_dag_state,
    duration_seconds,

    -- Derived flags for filtering
    case when final_dag_state = 'failed' then true else false end as is_failed,
    case when final_dag_state = 'degraded' then true else false end as is_degraded,
    case when freshness_status = 'WARN' or snapshot_status = 'WARN' then true else false end as has_warn,

    created_at

from planning_os.ops.pipeline_run_history
order by created_at desc;

comment on view planning_os.ops.v_pipeline_run_status is
  'Analyst-facing view of pipeline run history with derived flags for failed/degraded runs';
-- ============================================================================
-- OBJECT SUMMARY
--
-- Setup method: dbt run-operation setup_ops_schema
-- Macro location: dbt/macros/setup_ops_schema.sql
--
-- Table schema:
-- - Identifiers: dag_id, run_id, logical_date, run_type
-- - Window: window_mode, source, start_date, end_date, window_days
-- - Ingestion: batch_size, max_batches, ingested_row_count, ingested_min/max_order_date, lag_hours
-- - Health: freshness_status, snapshot_status, dbt_build_status
-- - State: final_dag_state (success/failed/degraded)
-- - Timing: dag_start_ts, dag_end_ts, duration_seconds
-- - Metadata: created_at (timestamp_ntz default current_timestamp)
-- - Constraint: run_id unique
--
-- View schema (v_pipeline_run_status):
-- - All table columns +
-- - is_failed: boolean (final_dag_state = 'failed')
-- - is_degraded: boolean (final_dag_state = 'degraded')
-- - has_warn: boolean (freshness_status or snapshot_status = 'WARN')
--
-- ============================================================================
-- QUERY EXAMPLES
-- ============================================================================

-- Recent 10 runs
-- select run_id, logical_date, final_dag_state, duration_seconds
-- from ops.pipeline_run_history
-- order by created_at desc
-- limit 10;

-- Failed runs
-- select run_id, logical_date, final_dag_state, dag_start_ts, dag_end_ts
-- from ops.v_pipeline_run_status
-- where is_failed = true
-- order by created_at desc
-- limit 20;

-- Degraded or warning runs
-- select run_id, logical_date, freshness_status, snapshot_status, dbt_build_status
-- from ops.v_pipeline_run_status
-- where is_degraded = true or has_warn = true
-- order by created_at desc
-- limit 20;

-- Average duration by source
-- select source, count(*) as run_count, avg(duration_seconds) as avg_seconds
-- from ops.pipeline_run_history
-- where final_dag_state = 'success'
-- group by source
-- order by avg_seconds desc;

-- Success rate by source (last 30 days)
-- select
--   source,
--   count(*) as total_runs,
--   sum(case when final_dag_state = 'success' then 1 else 0 end) as successful,
--   round(100.0 * sum(case when final_dag_state = 'success' then 1 else 0 end) / count(*), 2) as success_rate_pct
-- from ops.pipeline_run_history
-- where created_at >= current_date - 30
-- group by source
-- order by success_rate_pct asc;
