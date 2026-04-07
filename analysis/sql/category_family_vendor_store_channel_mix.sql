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
    h.category_family,
    coalesce(s.store_channel, 'Unknown') as store_channel,
    coalesce(cast(h.vendor_number as varchar), 'Unknown Vendor Number') as vendor_number,
    coalesce(h.vendor_name, 'Unknown Vendor') as vendor_name,
    f.order_date,
    f.sale_dollars,
    f.bottles_sold
  from {database}.{schema}.fct_liquor_sales f
  join {database}.{schema}.dim_item_business_history h
    on f.item_number = h.item_number
   and f.order_date >= h.business_valid_from
   and (h.business_valid_to is null or f.order_date <= h.business_valid_to)
  left join {database}.{schema}.dim_store s
    on f.store_number = s.store_number
  cross join params p
  where h.category_family = p.category_family
),
vendor_names as (
  select
    vendor_number,
    vendor_name
  from (
    select
      vendor_number,
      vendor_name,
      row_number() over (partition by vendor_number order by count(*) desc, vendor_name) as rn
    from base_filtered
    group by 1, 2
  )
  where rn = 1
),
vendor_channel_sales as (
  select
    category_family,
    store_channel,
    vendor_number,
    sum(case when order_date >= b.t12m_start and order_date < b.t12m_end then sale_dollars else 0 end) as sales_t12m,
    sum(case when order_date >= b.prior_t12m_start and order_date < b.prior_t12m_end then sale_dollars else 0 end) as sales_prior_t12m,
    sum(case when order_date >= b.t12m_start and order_date < b.t12m_end then bottles_sold else 0 end) as units_t12m
  from base_filtered
  cross join bounds b
  group by 1, 2, 3
),
vendor_totals as (
  select
    vcs.vendor_number,
    vn.vendor_name,
    sum(vcs.sales_t12m) as vendor_sales_t12m,
    sum(vcs.units_t12m) as vendor_units_t12m,
    row_number() over (
      order by sum(vcs.sales_t12m) desc, vn.vendor_name, vcs.vendor_number
    ) as vendor_rank
  from vendor_channel_sales vcs
  join vendor_names vn on vcs.vendor_number = vn.vendor_number
  group by 1, 2
),
detail_rows as (
  select
    vcs.store_channel,
    vcs.vendor_number,
    vt.vendor_name,
    vt.vendor_rank,
    vcs.sales_t12m,
    vcs.sales_prior_t12m,
    vcs.units_t12m,
    case
      when vcs.sales_prior_t12m = 0 then null
      else round(((vcs.sales_t12m - vcs.sales_prior_t12m) / vcs.sales_prior_t12m) * 100, 2)
    end as t12m_yoy_pct,
    case
      when vcs.units_t12m = 0 then null
      else round(vcs.sales_t12m / vcs.units_t12m, 2)
    end as avg_selling_price
  from vendor_channel_sales vcs
  join vendor_totals vt
    on vcs.vendor_number = vt.vendor_number
)
select
  'detail' as row_type,
  store_channel,
  vendor_number,
  vendor_name,
  vendor_rank,
  sales_t12m,
  sales_prior_t12m,
  units_t12m,
  avg_selling_price,
  t12m_yoy_pct
from detail_rows

union all

select
  'grand_total' as row_type,
  'Grand Total' as store_channel,
  null as vendor_number,
  null as vendor_name,
  null as vendor_rank,
  sum(sales_t12m) as sales_t12m,
  sum(sales_prior_t12m) as sales_prior_t12m,
  sum(units_t12m) as units_t12m,
  case
    when sum(units_t12m) = 0 then null
    else round(sum(sales_t12m) / sum(units_t12m), 2)
  end as avg_selling_price,
  case
    when sum(sales_prior_t12m) = 0 then null
    else round(((sum(sales_t12m) - sum(sales_prior_t12m)) / sum(sales_prior_t12m)) * 100, 2)
  end as t12m_yoy_pct
from detail_rows

order by
  case when row_type = 'detail' then 0 else 1 end,
  vendor_rank nulls last,
  sales_t12m desc nulls last,
  store_channel;
