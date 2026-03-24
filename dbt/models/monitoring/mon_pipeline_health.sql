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

    -- fact layer
    fact.fact_row_count,
    fact.fact_min_order_date,
    fact.fact_max_order_date,

    -- daily mart
    daily.daily_store_day_count,
    daily.daily_min_order_date,
    daily.daily_max_order_date

from raw
cross join fact
cross join daily