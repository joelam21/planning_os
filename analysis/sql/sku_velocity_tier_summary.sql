with params as (
  select
    to_date('{month_start}') as anchor_date,
    {trailing_weeks} as trailing_weeks
),
trailing_window as (
  select
    item_number,
    sum(bottles_sold) as total_units_sold,
    sum(sale_dollars) as total_revenue
  from {database}.{schema}.fct_liquor_sales
  cross join params p
  where order_date >= dateadd(week, -p.trailing_weeks, p.anchor_date)
    and order_date < dateadd(day, 1, p.anchor_date)
    and upper(invoice_item_number) not like 'RINV-%'
  group by item_number
),
state_totals as (
  select
    sum(total_units_sold) as state_total_units,
    sum(total_revenue)    as state_total_revenue,
    count(*)              as active_sku_count
  from trailing_window
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
    sum(t.total_revenue) over (
      order by t.total_revenue desc, t.item_number
      rows between unbounded preceding and current row
    ) as cumulative_revenue,
    s.state_total_units,
    s.state_total_revenue,
    s.active_sku_count
  from trailing_window t
  cross join state_totals s
),
item_classification as (
  select
    r.item_number,
    r.total_units_sold,
    r.total_revenue,
    r.state_total_units,
    r.state_total_revenue,
    r.active_sku_count,
    case
      when (r.cumulative_units / nullif(r.state_total_units, 0)) <= 0.80
        then 'Tier 1: Core (Top 80% Volume)'
      when (r.cumulative_units / nullif(r.state_total_units, 0)) <= 0.95
        then 'Tier 2: Niche (Next 15% Volume)'
      else 'Tier 3: Zombie (Bottom 5% Volume)'
    end as sku_tier_volume,
    case
      when (r.cumulative_revenue / nullif(r.state_total_revenue, 0)) <= 0.80
        then 'Tier 1: Core (Top 80% Revenue)'
      when (r.cumulative_revenue / nullif(r.state_total_revenue, 0)) <= 0.95
        then 'Tier 2: Niche (Next 15% Revenue)'
      else 'Tier 3: Zombie (Bottom 5% Revenue)'
    end as sku_tier_revenue
  from ranked_items r
)
select
  'volume' as tier_dimension,
  sku_tier_volume as sku_tier,
  count(*) as sku_count,
  sum(total_units_sold) as units_sold,
  sum(total_revenue) as revenue,
  count(*) / nullif(max(active_sku_count), 0)::float as pct_of_catalog,
  sum(total_units_sold) / nullif(max(state_total_units), 0)::float as pct_of_volume,
  sum(total_revenue) / nullif(max(state_total_revenue), 0)::float as pct_of_revenue
from item_classification
group by sku_tier_volume

union all

select
  'revenue' as tier_dimension,
  sku_tier_revenue as sku_tier,
  count(*) as sku_count,
  sum(total_units_sold) as units_sold,
  sum(total_revenue) as revenue,
  count(*) / nullif(max(active_sku_count), 0)::float as pct_of_catalog,
  sum(total_units_sold) / nullif(max(state_total_units), 0)::float as pct_of_volume,
  sum(total_revenue) / nullif(max(state_total_revenue), 0)::float as pct_of_revenue
from item_classification
group by sku_tier_revenue

order by tier_dimension, sku_tier;
