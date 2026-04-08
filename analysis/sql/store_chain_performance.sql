-- Chain-level market share and store productivity.
-- Aggregates sales into top N chains vs Other over a trailing window.
-- Params: month_start, trailing_weeks, top_n_chains, database, schema
with params as (
  select
    to_date('{month_start}') as anchor_date,
    {trailing_weeks}         as trailing_weeks
),
trailing_window as (
  select
    f.store_number,
    sum(f.total_sales_dollars)  as store_total_sales,
    sum(f.total_bottles_sold)   as store_total_bottles
  from {database}.{schema}.fct_store_daily_sales f
  cross join params p
  where f.order_date >= dateadd(week, -p.trailing_weeks, p.anchor_date)
    and f.order_date <  dateadd(day, 1, p.anchor_date)
  group by f.store_number
),
store_with_chain as (
  select
    t.store_number,
    t.store_total_sales,
    coalesce(s.chain, 'UNKNOWN') as chain
  from trailing_window t
  left join {database}.{schema}.dim_store s
    on t.store_number = s.store_number
),
chain_totals as (
  select
    chain,
    sum(store_total_sales)       as chain_total_sales,
    count(distinct store_number) as store_count
  from store_with_chain
  group by chain
),
chain_ranked as (
  select
    chain,
    chain_total_sales,
    store_count,
    row_number() over (order by chain_total_sales desc) as sales_rank
  from chain_totals
),
market_total as (
  select sum(chain_total_sales) as total_market_sales
  from chain_totals
),
chain_grouped as (
  select
    case
      when sales_rank <= {top_n_chains} then chain
      else 'Other'
    end as chain_group,
    chain_total_sales,
    store_count
  from chain_ranked
)
select
  chain_group,
  sum(chain_total_sales)                                                   as total_sales,
  sum(store_count)                                                         as store_count,
  sum(chain_total_sales) / nullif(sum(store_count), 0)                    as avg_sales_per_store,
  sum(chain_total_sales) / nullif(max(m.total_market_sales), 0)           as pct_of_market
from chain_grouped
cross join market_total m
group by chain_group
order by total_sales desc;
