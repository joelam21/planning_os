with state_totals as (
    select
        sum(total_units_sold) as state_total_units,
        sum(total_revenue) as state_total_revenue,
        count(*) as active_sku_count
    from {{ ref('int_sku_velocity_base_12wk') }}
),

ranked_items as (
    select
        sku.item_number,
        sku.total_units_sold,
        sku.total_revenue,
        sum(sku.total_units_sold) over (
            order by sku.total_units_sold desc, sku.item_number
            rows between unbounded preceding and current row
        ) as cumulative_units,
        sum(sku.total_revenue) over (
            order by sku.total_revenue desc, sku.item_number
            rows between unbounded preceding and current row
        ) as cumulative_revenue,
        totals.state_total_units,
        totals.state_total_revenue,
        totals.active_sku_count
    from {{ ref('int_sku_velocity_base_12wk') }} as sku
    cross join state_totals as totals
)

select *
from ranked_items