{% snapshot snap_store %}

{{
    config(
        target_schema='DEV',
        unique_key='store_number',
        strategy='check',
        check_cols=[
            'store_name',
            'chain',
            'address',
            'city',
            'zip_code',
            'store_location',
            'county_number',
            'county'
        ]
    )
}}

select
    store_number,
    store_name,
    chain,
    address,
    city,
    zip_code,
    store_location,
    county_number,
    county
from {{ ref('int_store_attributes') }}

{% endsnapshot %}