with baseline_forecast as (
    select
        item_number,
        trailing_4wk_avg_units,
        trailing_8wk_avg_units,
        greatest(0, trailing_4wk_avg_units, trailing_8wk_avg_units) as selected_baseline_units,
        round(greatest(0, trailing_4wk_avg_units, trailing_8wk_avg_units) * 1.2, 0) as recommended_ship_qty
    from {{ ref('int_replenishment_rolling_metrics') }}
)

select *
from baseline_forecast