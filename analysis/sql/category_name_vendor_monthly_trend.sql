with params as (
  select
    to_date('{window_start}') as window_start,
    to_date('{window_end}') as window_end,
    '{category_name}' as category_name,
    {top_n_vendors} as top_n_vendors
),
base_sales as (
  select
    date_trunc('month', f.order_date) as sales_month,
    d.category_name as scope_name,
    coalesce(cast(d.vendor_number as varchar), 'Unknown Vendor Number') as vendor_number,
    coalesce(d.vendor_name, 'Unknown Vendor') as vendor_name,
    f.sale_dollars
  from planning_os.dev.fct_liquor_sales f
  join planning_os.dev.dim_item d
    on f.item_number = d.item_number
  cross join params p
  where d.category_name = p.category_name
    and f.order_date >= p.window_start
    and f.order_date < dateadd(month, 1, p.window_end)
),
top_vendors as (
  select
    vendor_number,
    vendor_name,
    sum(sale_dollars) as vendor_sales_window,
    row_number() over (
      order by sum(sale_dollars) desc, vendor_name, vendor_number
    ) as vendor_rank
  from base_sales
  group by 1, 2
  qualify vendor_rank <= (select max(top_n_vendors) from params)
)
select
  b.sales_month,
  b.scope_name,
  b.vendor_number,
  b.vendor_name,
  tv.vendor_rank,
  sum(b.sale_dollars) as sales_dollars
from base_sales b
join top_vendors tv
  on b.vendor_number = tv.vendor_number
 and b.vendor_name = tv.vendor_name
group by 1, 2, 3, 4, 5
order by sales_month, vendor_rank, sales_dollars desc;
