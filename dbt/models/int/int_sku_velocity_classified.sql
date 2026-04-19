with classified_items as (
    select
        item_number,
        total_units_sold,
        total_revenue,
        cumulative_units,
        cumulative_revenue,
        state_total_units,
        state_total_revenue,
        active_sku_count,
        (cumulative_units / nullif(state_total_units, 0))::float as cumulative_volume_pct,
        (cumulative_revenue / nullif(state_total_revenue, 0))::float as cumulative_revenue_pct,
        case
            when (cumulative_units / nullif(state_total_units, 0)) <= 0.80
                then 'Tier 1: Core (Top 80% Volume)'
            when (cumulative_units / nullif(state_total_units, 0)) <= 0.95
                then 'Tier 2: Niche (Next 15% Volume)'
            else 'Tier 3: Zombie (Bottom 5% Volume)'
        end as sku_tier_volume,
        case
            when (cumulative_revenue / nullif(state_total_revenue, 0)) <= 0.80
                then 'Tier 1: Core (Top 80% Revenue)'
            when (cumulative_revenue / nullif(state_total_revenue, 0)) <= 0.95
                then 'Tier 2: Niche (Next 15% Revenue)'
            else 'Tier 3: Zombie (Bottom 5% Revenue)'
        end as sku_tier_revenue
    from {{ ref('int_sku_velocity_ranked') }}
)

select *
from classified_items