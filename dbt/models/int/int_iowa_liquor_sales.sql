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

    state_bottle_retail - state_bottle_cost as gross_profit_per_bottle,
    state_bottle_cost * bottles_sold as estimated_total_cost,
    sale_dollars - (state_bottle_cost * bottles_sold) as estimated_gross_profit,
    case
        when bottles_sold = 0 then null
        else sale_dollars / bottles_sold
    end as avg_revenue_per_bottle,
    case
        when volume_sold_liters = 0 then null
        else sale_dollars / volume_sold_liters
    end as revenue_per_liter
from {{ ref('stg_iowa_liquor') }}
