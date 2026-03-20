-- state_bottle_retail should be >= state_bottle_cost
-- Modeled as a warning: real-world data may have known exceptions
-- Violations should be investigated, not silently ignored
{{ config(severity='warn') }}

select *
from {{ ref('stg_iowa_liquor') }}
where
    state_bottle_retail is not null
    and state_bottle_cost is not null
    and state_bottle_retail < state_bottle_cost