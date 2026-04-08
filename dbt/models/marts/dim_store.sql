-- dim_store is the current-state Type 1 dimension.
-- It keeps only the latest known attributes per store_number from the snapshot.
-- Historical store attributes are available via snap_store.

select
    store_number,
    store_name,
    chain,
    store_channel,
    address,
    city,
    zip_code,
    store_location,
    county_number,
    county
from {{ ref('snap_store') }}
where dbt_valid_to is null