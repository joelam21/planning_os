with params as (
  select
    to_date('{month_start}') as month_start,
    '{category_family}' as category_family,
    '{vendor_number_filter}' as vendor_number_filter,
    '{bottle_volume_ml_filter}' as bottle_volume_ml_filter
),
bounds as (
  select
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
    coalesce(d.item_description, 'Unknown Item') as item_name,
    coalesce(d.vendor_name, 'Unknown Vendor') as vendor_name,
    f.order_date,
    f.sale_dollars
  from planning_os.dev.fct_liquor_sales f
  join planning_os.dev.dim_item d
    on f.item_number = d.item_number
  cross join params p
  where d.category_family = p.category_family
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
item_sales as (
  select
    item_name,
    vendor_name,
    sum(case when order_date >= b.t12m_start and order_date < b.t12m_end then sale_dollars else 0 end) as sales_t12m,
    sum(case when order_date >= b.prior_t12m_start and order_date < b.prior_t12m_end then sale_dollars else 0 end) as sales_prior_t12m
  from base_filtered
  cross join bounds b
  group by 1, 2
),
detail_rows as (
  select
    'detail' as row_type,
    item_name,
    vendor_name,
    sales_t12m,
    sales_prior_t12m,
    case
      when sales_prior_t12m = 0 then null
      else round(((sales_t12m - sales_prior_t12m) / sales_prior_t12m) * 100, 2)
    end as t12m_yoy_pct
  from item_sales
)
select * from detail_rows

union all

select
  'grand_total' as row_type,
  'Grand Total' as item_name,
  null as vendor_name,
  sum(sales_t12m) as sales_t12m,
  sum(sales_prior_t12m) as sales_prior_t12m,
  case
    when sum(sales_prior_t12m) = 0 then null
    else round(((sum(sales_t12m) - sum(sales_prior_t12m)) / sum(sales_prior_t12m)) * 100, 2)
  end as t12m_yoy_pct
from detail_rows

order by
  case when row_type = 'detail' then 0 else 1 end,
  sales_t12m desc nulls last,
  vendor_name,
  item_name;
