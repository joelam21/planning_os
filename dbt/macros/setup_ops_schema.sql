{% macro setup_ops_schema() %}
  {%- set sql -%}
    -- Create OPS schema
    create schema if not exists {{ target.database }}.ops;

    -- Create pipeline run history table
    create table if not exists {{ target.database }}.ops.pipeline_run_history (
        dag_id varchar(256) not null,
        run_id varchar(256) not null,
        logical_date timestamp_ntz,
        run_type varchar(64),
        window_mode varchar(64),
        source varchar(64),
        start_date date,
        end_date date,
        window_days number,
        batch_size number,
        max_batches number,
        ingested_row_count number,
        ingested_min_order_date date,
        ingested_max_order_date date,
        ingested_loaded_at_lag_hours number,
        freshness_status varchar(64),
        snapshot_status varchar(64),
        dbt_build_status varchar(64),
        final_dag_state varchar(64) not null,
        dag_start_ts timestamp_ntz,
        dag_end_ts timestamp_ntz,
        duration_seconds number,
        created_at timestamp_ntz default current_timestamp(),
        unique (run_id)
    );

    -- Create analyst view
    create or replace view {{ target.database }}.ops.v_pipeline_run_status as
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
        case when final_dag_state = 'failed' then true else false end as is_failed,
        case when final_dag_state = 'degraded' then true else false end as is_degraded,
        case when freshness_status = 'WARN' or snapshot_status = 'WARN' then true else false end as has_warn,
        created_at
    from {{ target.database }}.ops.pipeline_run_history
    order by created_at desc;
  {%- endset -%}

  {% do run_query(sql) %}

  {% do log("✓ OPS schema, table, and view created successfully", info=true) %}
{% endmacro %}
