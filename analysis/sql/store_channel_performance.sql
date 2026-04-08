-- Store channel breakdown: market share and productivity by retail format.
-- Uses Nielsen/Circana-aligned store_channel classifications from dim_store.
-- Params: month_start, trailing_weeks, database, schema
with params as (
  select
    to_date('{month_start}') as anchor_date,
    {trailing_weeks}         as trailing_weeks
),
trailing_window as (
  select
    f.store_number,
    sum(f.total_sales_dollars) as store_total_sales,
    sum(f.total_bottles_sold)  as store_total_bottles
  from {database}.{schema}.fct_store_daily_sales f
  cross join params p
  where f.order_date >= dateadd(week, -p.trailing_weeks, p.anchor_date)
    and f.order_date <  dateadd(day, 1, p.anchor_date)
  group by f.store_number
),
store_with_channel as (
  select
    t.store_number,
    t.store_total_sales,
    coalesce(s.store_channel, 'Unknown') as store_channel
  from trailing_window t
  left join {database}.{schema}.dim_store s
    on t.store_number = s.store_number
),
market_total as (
  select sum(store_total_sales) as total_market_sales
  from store_with_channel
)
select
  c.store_channel,
  sum(c.store_total_sales)                                        as total_sales,
  count(distinct c.store_number)                                  as store_count,
  sum(c.store_total_sales) / nullif(count(distinct c.store_number), 0) as avg_sales_per_store,
  sum(c.store_total_sales) / nullif(max(m.total_market_sales), 0) as pct_of_market
from store_with_channel c
cross join market_total m
group by c.store_channel
order by total_sales desc;
