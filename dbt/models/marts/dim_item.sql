-- dim_item is the current-state Type 1 dimension.
-- It keeps only the latest known attributes per item_number from the snapshot.
-- Historical item attributes are available via snap_item.

with base as (
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
    from {{ ref('snap_item') }}
    where dbt_valid_to is null
),

mapped as (
    select
        b.item_number,
        b.item_description,
        b.category,
        b.category_name,
        coalesce(m.category_family, 'Other') as category_family,
        b.vendor_number,
        b.vendor_name,
        b.bottle_volume_ml,
        b.pack,
        b.state_bottle_cost,
        b.state_bottle_retail
    from base b
    left join {{ ref('category_family_map') }} m
        on b.category = m.category
)

select * from mapped