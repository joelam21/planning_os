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
base_filtered as (
  select
    d.category_family,
    coalesce(cast(d.vendor_number as varchar), 'Unknown Vendor Number') as vendor_number,
    coalesce(d.vendor_name, 'Unknown Vendor') as vendor_name,
    f.order_date,
    f.sale_dollars
  from planning_os.dev.fct_liquor_sales f
  join planning_os.dev.dim_item d
    on f.item_number = d.item_number
  cross join params p
  where d.category_family = p.category_family
),
vendor_period_sales as (
  select
    p.period_order,
    year(p.anchor_month_start) as period_year,
    to_char(p.anchor_month_start, 'Mon YYYY') as period_label,
    b.vendor_number,
    b.vendor_name,
    sum(
      case
        when b.order_date >= dateadd(month, -11, p.anchor_month_start)
         and b.order_date < dateadd(month, 1, p.anchor_month_start)
        then b.sale_dollars
        else 0
      end
    ) as sales_t12m
  from base_filtered b
  cross join periods p
  group by 1, 2, 3, 4, 5
)
select
  period_order,
  period_year,
  period_label,
  vendor_number,
  vendor_name,
  sales_t12m
from vendor_period_sales
where sales_t12m > 0
order by
  period_order,
  sales_t12m desc,
  vendor_name,
  vendor_number;
