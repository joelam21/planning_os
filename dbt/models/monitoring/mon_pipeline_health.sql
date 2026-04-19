{{
    config(
        materialized='view'
    )
}}

with raw as (
    select
        max(loaded_at)  as raw_max_loaded_at,
        min(loaded_at)  as raw_min_loaded_at,
        count(*)        as raw_row_count
    from {{ source('raw', 'RAW_IOWA_LIQUOR') }}
),

fact as (
    select
        max(loaded_at)   as fact_max_loaded_at,
        min(order_date)  as fact_min_order_date,
        max(order_date)  as fact_max_order_date,
        count(*)         as fact_row_count
    from {{ ref('fct_liquor_sales') }}
),

daily as (
    select
        min(order_date)  as daily_min_order_date,
        max(order_date)  as daily_max_order_date,
        count(*)         as daily_store_day_count
    from {{ ref('fct_store_daily_sales') }}
),

snapshots as (
    select
        (
            select count(*)
            from {{ ref('snap_item') }}
            where dbt_valid_to is null
        ) as snap_item_current_row_count,
        (
            select max(dbt_valid_from)
            from {{ ref('snap_item') }}
        ) as snap_item_max_dbt_valid_from,
        (
            select count(*)
            from {{ ref('snap_store') }}
            where dbt_valid_to is null
        ) as snap_store_current_row_count,
        (
            select max(dbt_valid_from)
            from {{ ref('snap_store') }}
        ) as snap_store_max_dbt_valid_from
),

decision_support as (
    select
        (
            select count(*)
            from {{ ref('fct_sku_velocity') }}
        ) as sku_velocity_row_count,
        (
            select count(*)
            from {{ ref('fct_replenishment_forecast') }}
        ) as replenishment_forecast_row_count
)

select
    current_timestamp()                                               as observed_at,

    -- raw layer
    raw.raw_max_loaded_at,
    raw.raw_row_count,
    datediff('day', raw.raw_max_loaded_at, current_timestamp())      as raw_lag_days,

    -- freshness status (mirrors raw.yml thresholds: warn 7d, error 14d)
    case
        when datediff('day', raw.raw_max_loaded_at, current_timestamp()) > 14 then 'ERROR'
        when datediff('day', raw.raw_max_loaded_at, current_timestamp()) > 7  then 'WARN'
        else 'PASS'
    end                                                               as freshness_status,

    -- snapshot layer
    snapshots.snap_item_current_row_count,
    snapshots.snap_item_max_dbt_valid_from,
    snapshots.snap_store_current_row_count,
    snapshots.snap_store_max_dbt_valid_from,
    greatest(
        datediff('day', snapshots.snap_item_max_dbt_valid_from, current_timestamp()),
        datediff('day', snapshots.snap_store_max_dbt_valid_from, current_timestamp())
    )                                                                  as snapshot_lag_days,
    case
        when greatest(
            datediff('day', snapshots.snap_item_max_dbt_valid_from, current_timestamp()),
            datediff('day', snapshots.snap_store_max_dbt_valid_from, current_timestamp())
        ) > 14 then 'ERROR'
        when greatest(
            datediff('day', snapshots.snap_item_max_dbt_valid_from, current_timestamp()),
            datediff('day', snapshots.snap_store_max_dbt_valid_from, current_timestamp())
        ) > 7 then 'WARN'
        else 'PASS'
    end                                                               as snapshot_status,

    -- fact layer
    fact.fact_row_count,
    fact.fact_min_order_date,
    fact.fact_max_order_date,

    -- daily mart
    daily.daily_store_day_count,
    daily.daily_min_order_date,
    daily.daily_max_order_date,

    -- decision-support marts
    decision_support.sku_velocity_row_count,
    decision_support.replenishment_forecast_row_count

from raw
cross join fact
cross join daily
cross join snapshots
cross join decision_support