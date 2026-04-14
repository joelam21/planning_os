-- Canonical current-state item attributes.
-- This model owns latest-record selection and lightweight reusable enrichment
-- so snapshots and dimensions do not duplicate current-item preparation logic.

with latest_item_attributes as (
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
        row_number() over (
            partition by item_number
            order by order_date desc nulls last, loaded_at desc
        ) as rn
    from {{ ref('int_iowa_liquor_sales') }}
    where item_number is not null
),

current_item_attributes as (
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
    from latest_item_attributes
    where rn = 1
)

select
    c.item_number,
    c.item_description,
    c.category,
    c.category_name,
    coalesce(m.category_family, 'Other') as category_family,
    c.vendor_number,
    c.vendor_name,
    c.bottle_volume_ml,
    c.pack,
    c.state_bottle_cost,
    c.state_bottle_retail
from current_item_attributes c
left join {{ ref('category_family_map') }} m
    on c.category = m.category