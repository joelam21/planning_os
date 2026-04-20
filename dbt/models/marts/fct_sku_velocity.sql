with sku_velocity as (
    select
        ranked.item_number,
        dim_item.item_description,
        dim_item.category_name,
        ranked.total_units_sold,
        ranked.total_revenue,
        ranked.cumulative_units,
        ranked.cumulative_revenue,
        ranked.state_total_units,
        ranked.state_total_revenue,
        ranked.active_sku_count,
        ranked.cumulative_volume_pct,
        ranked.cumulative_revenue_pct,
        ranked.sku_tier_volume,
        ranked.sku_tier_revenue
    from {{ ref('int_sku_velocity_classified') }} as ranked
    left join {{ ref('dim_item') }} as dim_item
        on ranked.item_number = dim_item.item_number
)

select
    item_number,
    item_description,
    category_name,
    total_units_sold,
    total_revenue,
    cumulative_units,
    cumulative_revenue,
    state_total_units,
    state_total_revenue,
    active_sku_count,
    cumulative_volume_pct,
    cumulative_revenue_pct,
    sku_tier_volume,
    sku_tier_revenue
from sku_velocity