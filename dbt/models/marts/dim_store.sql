select distinct
    store_number,
    store_name,
    address,
    city,
    zip_code,
    store_location,
    county_number,
    county
from {{ ref('int_iowa_liquor_sales') }}
where store_number is not null
