-- dim_item is the current-state Type 1 dimension.
-- It keeps only the latest known attributes per item_number from the snapshot.
-- Historical item attributes are available via snap_item.

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