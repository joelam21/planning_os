with params as (
  select
    to_date('{month_start}') as month_start,
    '{category_family}' as category_family,
    '{package_size_tier}' as package_size_tier
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
    case
      when h.price_position_segment in ('budget', 'standard') then 'budget_standard'
      when h.price_position_segment in ('premium', 'super_premium', 'ultra_premium', 'luxury', 'icon_collectible') then 'premium_plus'
      when h.price_position_segment in ('premium_bulk', 'value_oversized', 'bundle_pack') then 'bulk_or_bundle'
      when h.price_position_segment in ('trial_value', 'trial_premium', 'trial_luxury') then 'trial_size'
      else h.price_position_segment
    end as price_position_segment,
    coalesce(cast(h.vendor_number as varchar), 'Unknown Vendor Number') as vendor_number,
    coalesce(h.vendor_name, 'Unknown Vendor') as vendor_name,
    f.order_date,
    f.sale_dollars,
    f.bottles_sold
  from planning_os.dev.fct_liquor_sales f
  join planning_os.dev.dim_item_business_history h
    on f.item_number = h.item_number
   and f.order_date >= h.business_valid_from
   and (h.business_valid_to is null or f.order_date <= h.business_valid_to)
  cross join params p
  where h.category_family = p.category_family
    and (
      p.package_size_tier = ''
      or h.package_size_tier = p.package_size_tier
    )
),
vendor_segment_sales as (
  select
    category_family,
    price_position_segment,
    vendor_number,
    vendor_name,
    sum(case when order_date >= b.t12m_start and order_date < b.t12m_end then sale_dollars else 0 end) as sales_t12m,
    sum(case when order_date >= b.prior_t12m_start and order_date < b.prior_t12m_end then sale_dollars else 0 end) as sales_prior_t12m,
    sum(case when order_date >= b.t12m_start and order_date < b.t12m_end then bottles_sold else 0 end) as units_t12m
  from base_filtered
  cross join bounds b
  group by 1, 2, 3, 4
),
vendor_totals as (
  select
    vendor_number,
    vendor_name,
    sum(sales_t12m) as vendor_sales_t12m,
    sum(units_t12m) as vendor_units_t12m,
    row_number() over (
      order by sum(sales_t12m) desc, vendor_name, vendor_number
    ) as vendor_rank
  from vendor_segment_sales
  group by 1, 2
),
detail_rows as (
  select
    vss.price_position_segment,
    vss.vendor_number,
    vss.vendor_name,
    vt.vendor_rank,
    vss.sales_t12m,
    vss.sales_prior_t12m,
    vss.units_t12m,
    case
      when vss.sales_prior_t12m = 0 then null
      else round(((vss.sales_t12m - vss.sales_prior_t12m) / vss.sales_prior_t12m) * 100, 2)
    end as t12m_yoy_pct,
    case
      when vss.units_t12m = 0 then null
      else round(vss.sales_t12m / vss.units_t12m, 2)
    end as avg_selling_price
  from vendor_segment_sales vss
  join vendor_totals vt
    on vss.vendor_number = vt.vendor_number
   and vss.vendor_name = vt.vendor_name
)
select
  'detail' as row_type,
  price_position_segment,
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
  'Grand Total' as price_position_segment,
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
  price_position_segment;
