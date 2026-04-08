/*
  category_vendor_volume_growth.sql
  -------------------------------------------------------
  Purpose:
    Produces vendor and bottle-volume growth views for a selected category_name,
    including month, trailing-3-month, and trailing-12-month sales with YoY deltas,
    plus subtotal and grand-total grouping-set rows.

  Parameters:
    month_start: Anchor month (YYYY-MM-01) used to build month/T3M/T12M windows.
    category_name: Category filter for vendor-volume analysis.
    vendor_number_filter: CSV vendor filter list or ALL for no vendor filtering.
    bottle_volume_ml_filter: CSV bottle-volume filter list or ALL for no volume filtering.
    database: Database containing marts and dimensions referenced in the query.
    schema: Schema containing fct_liquor_sales and dim_item.

  Returns:
    Grain varies by row_type: detail (category_name, vendor_number, vendor_name,
    bottle_volume_ml), vendor subtotal, bottle-volume subtotal, category subtotal,
    and grand total.
    Key columns include sales_month, sales_t3m, sales_t12m and corresponding YoY percentages.

  Used by:
    notebooks/01_category_growth_analysis.ipynb
*/

with params as (
  select
    to_date('{month_start}') as month_start,
    '{category_name}' as category_name,
    '{vendor_number_filter}' as vendor_number_filter,
    '{bottle_volume_ml_filter}' as bottle_volume_ml_filter
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
vendor_filter as (
  select
    distinct trim(value) as vendor_number,
    regexp_replace(trim(value), '^0+', '') as vendor_number_norm
  from params, table(split_to_table(vendor_number_filter, ','))
  where upper(trim(vendor_number_filter)) <> 'ALL'
    and trim(value) <> ''
),
volume_filter as (
  select distinct trim(value) as bottle_volume_ml
  from params, table(split_to_table(bottle_volume_ml_filter, ','))
  where upper(trim(bottle_volume_ml_filter)) <> 'ALL'
    and trim(value) <> ''
),
base_filtered as (
  select
    d.category_name,
    coalesce(cast(d.vendor_number as varchar), 'Unknown Vendor Number') as vendor_number,
    coalesce(d.vendor_name, 'Unknown Vendor') as vendor_name,
    coalesce(cast(d.bottle_volume_ml as varchar), 'Unknown Bottle Volume') as bottle_volume_ml,
    f.order_date,
    f.sale_dollars
  from {database}.{schema}.fct_liquor_sales f
  join {database}.{schema}.dim_item d
    on f.item_number = d.item_number
  cross join params p
  where d.category_name = p.category_name
    and (
      upper(trim(p.vendor_number_filter)) = 'ALL'
      or coalesce(cast(d.vendor_number as varchar), 'Unknown Vendor Number') in (select vendor_number from vendor_filter)
      or regexp_replace(coalesce(cast(d.vendor_number as varchar), 'Unknown Vendor Number'), '^0+', '') in (select vendor_number_norm from vendor_filter)
    )
    and (
      upper(trim(p.bottle_volume_ml_filter)) = 'ALL'
      or coalesce(cast(d.bottle_volume_ml as varchar), 'Unknown Bottle Volume') in (select bottle_volume_ml from volume_filter)
    )
),
segmented_sales as (
  select
    category_name,
    vendor_number,
    vendor_name,
    bottle_volume_ml,
    grouping(category_name) as grp_category,
    grouping(vendor_number) as grp_vendor_number,
    grouping(vendor_name) as grp_vendor_name,
    grouping(bottle_volume_ml) as grp_bottle_volume_ml,
    sum(case when f.order_date >= b.m_start and f.order_date < b.m_end then f.sale_dollars else 0 end) as sales_month,
    sum(case when f.order_date >= b.prior_m_start and f.order_date < b.prior_m_end then f.sale_dollars else 0 end) as sales_prior_month,
    sum(case when f.order_date >= b.t3m_start and f.order_date < b.t3m_end then f.sale_dollars else 0 end) as sales_t3m,
    sum(case when f.order_date >= b.prior_t3m_start and f.order_date < b.prior_t3m_end then f.sale_dollars else 0 end) as sales_prior_t3m,
    sum(case when f.order_date >= b.t12m_start and f.order_date < b.t12m_end then f.sale_dollars else 0 end) as sales_t12m,
    sum(case when f.order_date >= b.prior_t12m_start and f.order_date < b.prior_t12m_end then f.sale_dollars else 0 end) as sales_prior_t12m
  from base_filtered f
  cross join bounds b
  group by grouping sets (
    (category_name, vendor_number, vendor_name, bottle_volume_ml),
    (category_name, vendor_number, vendor_name),
    (category_name, bottle_volume_ml),
    (category_name),
    ()
  )
)
select
  case
    when grp_category = 1 then 'grand_total'
    when grp_vendor_number = 0 and grp_bottle_volume_ml = 0 and grp_vendor_name = 0 then 'detail'
    when grp_vendor_number = 0 and grp_bottle_volume_ml = 1 then 'subtotal_by_vendor'
    when grp_vendor_number = 1 and grp_bottle_volume_ml = 0 then 'subtotal_by_bottle_volume'
    else 'subtotal_by_category'
  end as row_type,
  case
    when grp_category = 1 then 'Grand Total'
    when grp_vendor_number = 0 and grp_bottle_volume_ml = 1 then 'Subtotal by Vendor: ' || category_name
    when grp_vendor_number = 1 and grp_bottle_volume_ml = 0 then 'Subtotal by Bottle Volume: ' || category_name
    when grp_vendor_number = 1 and grp_bottle_volume_ml = 1 then 'Subtotal: ' || category_name
    else category_name
  end as category_name,
  case when grp_vendor_number = 1 then null else vendor_number end as vendor_number,
  case when grp_vendor_name = 1 then null else vendor_name end as vendor_name,
  case when grp_bottle_volume_ml = 1 then null else bottle_volume_ml end as bottle_volume_ml,
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
    when grp_category = 0 and grp_vendor_number = 0 and grp_bottle_volume_ml = 0 then 0
    when grp_category = 0 and grp_vendor_number = 0 and grp_bottle_volume_ml = 1 then 1
    when grp_category = 0 and grp_vendor_number = 1 and grp_bottle_volume_ml = 0 then 2
    when grp_category = 0 and grp_vendor_number = 1 and grp_bottle_volume_ml = 1 then 3
    else 4
  end,
  sales_t12m desc nulls last,
  vendor_number,
  bottle_volume_ml;
