

select
    order_date,
    store_number,

    sum(bottles_sold) as total_bottles_sold,
    sum(sale_dollars) as total_sales_dollars,
    sum(volume_sold_liters) as total_volume_sold_liters,
    sum(volume_sold_gallons) as total_volume_sold_gallons,
    sum(estimated_total_cost) as total_estimated_cost,
    sum(estimated_gross_profit) as total_estimated_gross_profit,
    count(*) as invoice_line_count
from {{ ref('fct_liquor_sales') }}
group by
    order_date,
    store_number