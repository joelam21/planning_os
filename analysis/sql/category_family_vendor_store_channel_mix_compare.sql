with params as (
  select
    to_date('{month_start}') as month_start,
    '{category_family}' as category_family,
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
    coalesce(s.store_channel, 'Unknown') as store_channel,
    coalesce(cast(h.vendor_number as varchar), 'Unknown Vendor Number') as vendor_number,
    coalesce(h.vendor_name, 'Unknown Vendor') as vendor_name,
    f.sale_dollars,
    f.bottles_sold
  from {database}.{schema}.fct_liquor_sales f
  join {database}.{schema}.dim_item_business_history h
    on f.item_number = h.item_number
   and f.order_date >= h.business_valid_from
   and (h.business_valid_to is null or f.order_date <= h.business_valid_to)
  left join {database}.{schema}.dim_store s
    on f.store_number = s.store_number
  join selected_periods p
    on f.order_date >= dateadd(month, -11, p.anchor_month_start)
   and f.order_date < dateadd(month, 1, p.anchor_month_start)
  cross join params x
  where h.category_family = x.category_family
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
vendor_period_totals as (
  select
    period_order,
    period_year,
    vendor_number,
    sum(sale_dollars) as sales_t12m,
    sum(bottles_sold) as units_t12m
  from base_filtered
  group by 1, 2, 3
),
latest_period_top_vendors as (
  select
    vpt.vendor_number,
    vn.vendor_name,
    row_number() over (
      order by vpt.sales_t12m desc, vn.vendor_name, vpt.vendor_number
    ) as vendor_rank
  from vendor_period_totals vpt
  join vendor_names vn on vpt.vendor_number = vn.vendor_number
  where vpt.period_order = 0
  qualify vendor_rank <= 3
),
channel_sales as (
  select
    b.period_order,
    b.period_year,
    coalesce(t.vendor_number, 'Other') as vendor_number,
    coalesce(t.vendor_name, 'Other Vendors') as vendor_name,
    coalesce(t.vendor_rank, 4) as vendor_rank,
    b.store_channel,
    sum(b.sale_dollars) as sales_t12m,
    sum(b.bottles_sold) as units_t12m
  from base_filtered b
  left join latest_period_top_vendors t
    on b.vendor_number = t.vendor_number
  group by 1, 2, 3, 4, 5, 6
)
select
  period_order,
  period_year,
  vendor_number,
  vendor_name,
  vendor_rank,
  store_channel,
  sales_t12m,
  units_t12m,
  case
    when units_t12m = 0 then null
    else round(sales_t12m / units_t12m, 2)
  end as avg_selling_price
from channel_sales
order by
  period_order desc,
  vendor_rank,
  sales_t12m desc,
  store_channel;
