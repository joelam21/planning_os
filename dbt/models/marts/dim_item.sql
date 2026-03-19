select distinct
    item_number,
    item_description,
    category,
    category_name,
    vendor_number,
    vendor_name,
    bottle_volume_ml,
    pack,
    state_bottle_cost,
    state_bottle_retail
from {{ ref('int_iowa_liquor_sales') }}
where item_number is not null
