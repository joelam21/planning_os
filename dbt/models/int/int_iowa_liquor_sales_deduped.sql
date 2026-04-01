-- Keeps the latest loaded version of each invoice line while preserving raw/source-shaped staging upstream.

with ranked_source as (
    select
        *,
        row_number() over (
            partition by invoice_item_number
            order by loaded_at desc, order_date desc, store_number desc, item_number desc
        ) as dedupe_rank
    from {{ ref('int_iowa_liquor_sales') }}
)

select
    invoice_item_number,
    order_date,
    store_number,
    store_name,
    address,
    city,
    zip_code,
    store_location,
    county_number,
    county,
    category,
    category_name,
    vendor_number,
    vendor_name,
    item_number,
    item_description,
    pack,
    bottle_volume_ml,
    state_bottle_cost,
    state_bottle_retail,
    bottles_sold,
    sale_dollars,
    volume_sold_liters,
    volume_sold_gallons,
    loaded_at,
    gross_profit_per_bottle,
    estimated_total_cost,
    estimated_gross_profit,
    avg_revenue_per_bottle,
    revenue_per_liter
from ranked_source
where dedupe_rank = 1
