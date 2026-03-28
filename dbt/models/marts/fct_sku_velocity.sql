with trailing_12_weeks as (
    select
        item_number,
        sum(bottles_sold) as total_units_sold,
        sum(sale_dollars) as total_revenue
    from {{ ref('fct_liquor_sales') }}
    where order_date >= dateadd(
        week,
        -12,
        (select max(order_date) from {{ ref('fct_liquor_sales') }})
    )
    group by item_number
),

state_totals as (
    select
        sum(total_units_sold) as state_total_units,
        sum(total_revenue) as state_total_revenue,
        count(*) as active_sku_count
    from trailing_12_weeks
),

ranked_items as (
    select
        t.item_number,
        t.total_units_sold,
        t.total_revenue,
        sum(t.total_units_sold) over (
            order by t.total_units_sold desc, t.item_number
            rows between unbounded preceding and current row
        ) as cumulative_units,
        s.state_total_units,
        s.state_total_revenue,
        s.active_sku_count
    from trailing_12_weeks t
    cross join state_totals s
),

item_classification as (
    select
        r.item_number,
        i.item_description,
        i.category_name,
        r.total_units_sold,
        r.total_revenue,
        r.cumulative_units,
        r.state_total_units,
        r.state_total_revenue,
        r.active_sku_count,
        (r.cumulative_units / nullif(r.state_total_units, 0))::float as cumulative_pct,
        (r.total_units_sold / nullif(r.state_total_units, 0))::float as unit_share_pct,
        (r.total_revenue / nullif(r.state_total_revenue, 0))::float as revenue_share_pct,
        case
            when (r.cumulative_units / nullif(r.state_total_units, 0)) <= 0.80
                then 'Tier 1: Core (Top 80% Volume)'
            when (r.cumulative_units / nullif(r.state_total_units, 0)) <= 0.95
                then 'Tier 2: Niche (Next 15% Volume)'
            else 'Tier 3: Zombie (Bottom 5% Volume)'
        end as sku_tier
    from ranked_items r
    left join {{ ref('dim_item') }} i
        on r.item_number = i.item_number
)

select *
from item_classification
order by total_units_sold desc