-- All active SKUs with tier classification and scatter group label.
-- total_units_sold > 0 AND total_revenue > 0 required — log scale cannot handle zeros.
with params as (
  select
    to_date('{month_start}') as anchor_date,
    {trailing_weeks} as trailing_weeks
),
trailing_window as (
  select
    item_number,
    sum(bottles_sold) as total_units_sold,
    sum(sale_dollars)  as total_revenue
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
    i.item_description,
    i.category_name,
    i.category_family,
    i.bottle_volume_ml,
    r.total_units_sold,
    r.total_revenue,
    round(r.total_revenue / nullif(r.total_units_sold, 0), 2) as avg_selling_price,
    case
      when (r.cumulative_units / nullif(r.state_total_units, 0)) <= 0.80  then 'Tier 1: Core'
      when (r.cumulative_units / nullif(r.state_total_units, 0)) <= 0.95  then 'Tier 2: Niche'
      else 'Tier 3: Zombie'
    end as volume_tier,
    case
      when (r.cumulative_revenue / nullif(r.state_total_revenue, 0)) <= 0.80  then 'Tier 1: Core'
      when (r.cumulative_revenue / nullif(r.state_total_revenue, 0)) <= 0.95  then 'Tier 2: Niche'
      else 'Tier 3: Zombie'
    end as revenue_tier
  from ranked_items r
  left join {database}.{schema}.dim_item i on r.item_number = i.item_number
)
select
  item_number,
  item_description,
  coalesce(category_family, 'Other') as category_family,
  category_name,
  bottle_volume_ml,
  volume_tier,
  revenue_tier,
  total_units_sold,
  total_revenue,
  avg_selling_price,
  case
    when volume_tier = 'Tier 1: Core'   and revenue_tier = 'Tier 1: Core'   then 'Core / Core'
    when volume_tier = 'Tier 3: Zombie' and revenue_tier = 'Tier 1: Core'   then 'Fragile Premium'
    when volume_tier = 'Tier 1: Core'   and revenue_tier = 'Tier 3: Zombie' then 'High Volume / Low Revenue'
    when volume_tier = 'Tier 3: Zombie' and revenue_tier = 'Tier 3: Zombie' then 'Zombie / Zombie'
    else 'Other'
  end as scatter_group
from item_classification
where total_units_sold > 0
  and total_revenue > 0
order by scatter_group, total_revenue desc;
