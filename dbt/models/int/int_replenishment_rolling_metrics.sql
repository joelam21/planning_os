with max_date as (
    select date_trunc('week', max(order_date)) as current_week_start
    from {{ ref('fct_liquor_sales') }}
),

rolling_metrics as (
    select
        weekly_history.item_number,
        avg(
            case
                when weekly_history.week_start >= dateadd(week, -4, max_date.current_week_start)
                    then weekly_history.weekly_units_sold
            end
        ) as trailing_4wk_avg_units,
        avg(weekly_history.weekly_units_sold) as trailing_8wk_avg_units
    from {{ ref('int_replenishment_weekly_depletions') }} as weekly_history
    cross join max_date
    group by 1
)

select *
from rolling_metrics