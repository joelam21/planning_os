-- Fail if forecasted ship quantity is negative.
select *
from {{ ref('fct_replenishment_forecast') }}
where recommended_ship_qty < 0