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
    sum(total_revenue)    as state_total_revenue
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
    s.state_total_revenue
  from trailing_window t
  cross join state_totals s
),
item_classification as (
  select
    r.item_number,
    coalesce(i.category_family, 'Other') as category_family,
    r.total_units_sold,
    r.total_revenue,
    case
      when (r.cumulative_units / nullif(r.state_total_units, 0)) <= 0.80
        then 'Tier 1: Core'
      when (r.cumulative_units / nullif(r.state_total_units, 0)) <= 0.95
        then 'Tier 2: Niche'
      else 'Tier 3: Zombie'
    end as volume_tier,
    case
      when (r.cumulative_revenue / nullif(r.state_total_revenue, 0)) <= 0.80
        then 'Tier 1: Core'
      when (r.cumulative_revenue / nullif(r.state_total_revenue, 0)) <= 0.95
        then 'Tier 2: Niche'
      else 'Tier 3: Zombie'
    end as revenue_tier
  from ranked_items r
  left join {database}.{schema}.dim_item i on r.item_number = i.item_number
),
category_totals as (
  select
    category_family,
    count(*)              as cat_sku_count,
    sum(total_units_sold) as cat_units,
    sum(total_revenue)    as cat_revenue
  from item_classification
  group by category_family
)
select
  'volume' as tier_dimension,
  ic.category_family,
  ic.volume_tier as sku_tier,
  count(*)              as sku_count,
  sum(ic.total_units_sold) as units_sold,
  sum(ic.total_revenue)    as revenue,
  count(*) / nullif(max(ct.cat_sku_count), 0)::float           as pct_of_catalog,
  sum(ic.total_units_sold) / nullif(max(ct.cat_units), 0)::float as pct_of_volume,
  sum(ic.total_revenue) / nullif(max(ct.cat_revenue), 0)::float  as pct_of_revenue
from item_classification ic
join category_totals ct on ic.category_family = ct.category_family
group by tier_dimension, ic.category_family, ic.volume_tier

union all

select
  'revenue' as tier_dimension,
  ic.category_family,
  ic.revenue_tier as sku_tier,
  count(*)              as sku_count,
  sum(ic.total_units_sold) as units_sold,
  sum(ic.total_revenue)    as revenue,
  count(*) / nullif(max(ct.cat_sku_count), 0)::float           as pct_of_catalog,
  sum(ic.total_units_sold) / nullif(max(ct.cat_units), 0)::float as pct_of_volume,
  sum(ic.total_revenue) / nullif(max(ct.cat_revenue), 0)::float  as pct_of_revenue
from item_classification ic
join category_totals ct on ic.category_family = ct.category_family
group by tier_dimension, ic.category_family, ic.revenue_tier

order by tier_dimension, category_family, sku_tier;
