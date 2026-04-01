-- Source-data quality audit for duplicate invoice_item_number values observed upstream.

select
    invoice_item_number,
    count(*) as duplicate_row_count,
    min(loaded_at) as min_loaded_at,
    max(loaded_at) as max_loaded_at,
    count(distinct order_date) as distinct_order_date_count,
    count(distinct store_number) as distinct_store_number_count,
    count(distinct item_number) as distinct_item_number_count,
    count(distinct sale_dollars) as distinct_sale_dollars_count,
    count(distinct bottles_sold) as distinct_bottles_sold_count
from {{ ref('int_iowa_liquor_sales') }}
group by invoice_item_number
having count(*) > 1
