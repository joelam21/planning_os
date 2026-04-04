with params as (
  select
    to_date('{month_start}') as month_start,
    '{category_family}' as category_family
),
bounds as (
  select
    month_start as m_start,
    dateadd(month, 1, month_start) as m_end,
    dateadd(year, -1, month_start) as prior_m_start,
    dateadd(year, -1, dateadd(month, 1, month_start)) as prior_m_end,
    dateadd(month, -2, month_start) as t3m_start,
    dateadd(month, 1, month_start) as t3m_end,
    dateadd(year, -1, dateadd(month, -2, month_start)) as prior_t3m_start,
    dateadd(year, -1, dateadd(month, 1, month_start)) as prior_t3m_end,
    dateadd(month, -11, month_start) as t12m_start,
    dateadd(month, 1, month_start) as t12m_end,
    dateadd(year, -1, dateadd(month, -11, month_start)) as prior_t12m_start,
    dateadd(year, -1, dateadd(month, 1, month_start)) as prior_t12m_end
  from params
),
base_filtered as (
  select
    d.category_family,
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
segmented_sales as (
  select
    category_family,
    vendor_number,
    vendor_name,
    grouping(category_family) as grp_category_family,
    grouping(vendor_number) as grp_vendor_number,
    grouping(vendor_name) as grp_vendor_name,
    sum(case when f.order_date >= b.m_start and f.order_date < b.m_end then f.sale_dollars else 0 end) as sales_month,
    sum(case when f.order_date >= b.prior_m_start and f.order_date < b.prior_m_end then f.sale_dollars else 0 end) as sales_prior_month,
    sum(case when f.order_date >= b.t3m_start and f.order_date < b.t3m_end then f.sale_dollars else 0 end) as sales_t3m,
    sum(case when f.order_date >= b.prior_t3m_start and f.order_date < b.prior_t3m_end then f.sale_dollars else 0 end) as sales_prior_t3m,
    sum(case when f.order_date >= b.t12m_start and f.order_date < b.t12m_end then f.sale_dollars else 0 end) as sales_t12m,
    sum(case when f.order_date >= b.prior_t12m_start and f.order_date < b.prior_t12m_end then f.sale_dollars else 0 end) as sales_prior_t12m
  from base_filtered f
  cross join bounds b
  group by grouping sets (
    (category_family, vendor_number, vendor_name),
    (category_family),
    ()
  )
)
select
  case
    when grp_category_family = 1 then 'grand_total'
    when grp_vendor_number = 0 and grp_vendor_name = 0 then 'subtotal_by_vendor'
    else 'subtotal_by_category'
  end as row_type,
  case
    when grp_category_family = 1 then 'Grand Total'
    when grp_vendor_number = 0 then 'Subtotal by Vendor: ' || category_family
    else 'Subtotal: ' || category_family
  end as category_name,
  case when grp_vendor_number = 1 then null else vendor_number end as vendor_number,
  case when grp_vendor_name = 1 then null else vendor_name end as vendor_name,
  null as bottle_volume_ml,
  sales_month,
  sales_prior_month,
  case
    when sales_prior_month = 0 then null
    else round(((sales_month - sales_prior_month) / sales_prior_month) * 100, 2)
  end as month_yoy_pct,
  sales_t3m,
  sales_prior_t3m,
  case
    when sales_prior_t3m = 0 then null
    else round(((sales_t3m - sales_prior_t3m) / sales_prior_t3m) * 100, 2)
  end as t3m_yoy_pct,
  sales_t12m,
  sales_prior_t12m,
  case
    when sales_prior_t12m = 0 then null
    else round(((sales_t12m - sales_prior_t12m) / sales_prior_t12m) * 100, 2)
  end as t12m_yoy_pct
from segmented_sales
order by
  case
    when grp_category_family = 0 and grp_vendor_number = 0 then 0
    when grp_category_family = 0 and grp_vendor_number = 1 then 1
    else 2
  end,
  sales_t12m desc nulls last,
  vendor_number;
