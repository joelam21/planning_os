with params as (
  select
    to_date('{month_start}') as month_start,
    '{category_family}' as category_family,
    '{package_size_tier}' as package_size_tier,
    {trend_years} as trend_years
),
periods as (
  select
    seq4() as period_order,
    dateadd(year, -seq4(), p.month_start) as anchor_month_start
  from params p,
       table(generator(rowcount => p.trend_years))
),
selected_periods as (
  select *
  from periods
  qualify row_number() over (order by period_order asc) = 1
     or row_number() over (order by period_order desc) = 1
),
base_filtered as (
  select
    p.period_order,
    year(p.anchor_month_start) as period_year,
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
    f.sale_dollars,
    f.bottles_sold
  from planning_os.dev.fct_liquor_sales f
  join planning_os.dev.dim_item_business_history h
    on f.item_number = h.item_number
   and f.order_date >= h.business_valid_from
   and (h.business_valid_to is null or f.order_date <= h.business_valid_to)
  join selected_periods p
    on f.order_date >= dateadd(month, -11, p.anchor_month_start)
   and f.order_date < dateadd(month, 1, p.anchor_month_start)
  cross join params x
  where h.category_family = x.category_family
    and (
      x.package_size_tier = ''
      or h.package_size_tier = x.package_size_tier
    )
),
vendor_period_totals as (
  select
    period_order,
    period_year,
    vendor_number,
    vendor_name,
    sum(sale_dollars) as sales_t12m,
    sum(bottles_sold) as units_t12m
  from base_filtered
  group by 1, 2, 3, 4
),
latest_period_top_vendors as (
  select
    vendor_number,
    vendor_name,
    row_number() over (
      order by sales_t12m desc, vendor_name, vendor_number
    ) as vendor_rank
  from vendor_period_totals
  where period_order = 0
  qualify vendor_rank <= 3
),
segment_sales as (
  select
    b.period_order,
    b.period_year,
    coalesce(t.vendor_number, 'Other') as vendor_number,
    coalesce(t.vendor_name, 'Other Vendors') as vendor_name,
    coalesce(t.vendor_rank, 4) as vendor_rank,
    b.price_position_segment,
    sum(b.sale_dollars) as sales_t12m,
    sum(b.bottles_sold) as units_t12m
  from base_filtered b
  left join latest_period_top_vendors t
    on b.vendor_number = t.vendor_number
   and b.vendor_name = t.vendor_name
  group by 1, 2, 3, 4, 5, 6
)
select
  period_order,
  period_year,
  vendor_number,
  vendor_name,
  vendor_rank,
  price_position_segment,
  sales_t12m,
  units_t12m,
  case
    when units_t12m = 0 then null
    else round(sales_t12m / units_t12m, 2)
  end as avg_selling_price
from segment_sales
order by
  period_order desc,
  vendor_rank,
  sales_t12m desc,
  price_position_segment;
