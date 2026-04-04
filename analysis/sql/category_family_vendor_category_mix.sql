with params as (
  select
    to_date('{month_start}') as month_start,
    '{category_family}' as category_family
),
bounds as (
  select
    dateadd(month, -11, month_start) as t12m_start,
    dateadd(month, 1, month_start) as t12m_end,
    dateadd(year, -1, dateadd(month, -11, month_start)) as prior_t12m_start,
    dateadd(year, -1, dateadd(month, 1, month_start)) as prior_t12m_end
  from params
),
base_filtered as (
  select
    d.category_family,
    d.category_name,
    coalesce(cast(d.vendor_number as varchar), 'Unknown Vendor Number') as vendor_number,
    coalesce(d.vendor_name, 'Unknown Vendor') as vendor_name,
    f.order_date,
    f.sale_dollars
  from {database}.{schema}.fct_liquor_sales f
  join {database}.{schema}.dim_item d
    on f.item_number = d.item_number
  cross join params p
  where d.category_family = p.category_family
),
vendor_category_sales as (
  select
    category_family,
    category_name,
    vendor_number,
    vendor_name,
    sum(case when order_date >= b.t12m_start and order_date < b.t12m_end then sale_dollars else 0 end) as sales_t12m,
    sum(case when order_date >= b.prior_t12m_start and order_date < b.prior_t12m_end then sale_dollars else 0 end) as sales_prior_t12m
  from base_filtered
  cross join bounds b
  group by 1, 2, 3, 4
),
vendor_totals as (
  select
    vendor_number,
    vendor_name,
    sum(sales_t12m) as vendor_sales_t12m,
    row_number() over (
      order by sum(sales_t12m) desc, vendor_name, vendor_number
    ) as vendor_rank
  from vendor_category_sales
  group by 1, 2
),
detail_rows as (
  select
    vcs.category_name,
    vcs.vendor_number,
    vcs.vendor_name,
    vt.vendor_rank,
    vcs.sales_t12m,
    vcs.sales_prior_t12m,
    case
      when vcs.sales_prior_t12m = 0 then null
      else round(((vcs.sales_t12m - vcs.sales_prior_t12m) / vcs.sales_prior_t12m) * 100, 2)
    end as t12m_yoy_pct
  from vendor_category_sales vcs
  join vendor_totals vt
    on vcs.vendor_number = vt.vendor_number
   and vcs.vendor_name = vt.vendor_name
)
select
  'detail' as row_type,
  category_name,
  vendor_number,
  vendor_name,
  vendor_rank,
  sales_t12m,
  sales_prior_t12m,
  t12m_yoy_pct
from detail_rows

union all

select
  'grand_total' as row_type,
  'Grand Total' as category_name,
  null as vendor_number,
  null as vendor_name,
  null as vendor_rank,
  sum(sales_t12m) as sales_t12m,
  sum(sales_prior_t12m) as sales_prior_t12m,
  case
    when sum(sales_prior_t12m) = 0 then null
    else round(((sum(sales_t12m) - sum(sales_prior_t12m)) / sum(sales_prior_t12m)) * 100, 2)
  end as t12m_yoy_pct
from detail_rows

order by
  case when row_type = 'detail' then 0 else 1 end,
  vendor_rank nulls last,
  sales_t12m desc nulls last,
  category_name;
