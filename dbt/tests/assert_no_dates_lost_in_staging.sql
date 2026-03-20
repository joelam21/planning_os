-- Every date present in the raw source must appear in staging
-- A missing date means rows were silently filtered or dropped during transformation
with raw_dates as (
    select distinct cast(date as date) as order_date
    from {{ source('raw', 'RAW_IOWA_LIQUOR') }}
    where date is not null
),

staged_dates as (
    select distinct order_date
    from {{ ref('stg_iowa_liquor') }}
)

select raw_dates.order_date
from raw_dates
left join staged_dates using (order_date)
where staged_dates.order_date is null