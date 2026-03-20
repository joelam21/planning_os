-- dim_item is currently modeled as a Type 1 dimension:
-- keep the latest known record per item_number for current-state analysis.
-- Future enhancement: consider SCD Type 2 if product attributes need historical tracking.

with ranked as (
    select
        item_number,
        item_description,
        category,
        category_name,
        vendor_number,
        vendor_name,
        bottle_volume_ml,
        pack,
        state_bottle_cost,
        state_bottle_retail,
        loaded_at,
        row_number() over (
            partition by item_number
            order by loaded_at desc
        ) as rn
    from {{ ref('int_iowa_liquor_sales') }}
    where item_number is not null
)

select
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
from ranked
where rn = 1
