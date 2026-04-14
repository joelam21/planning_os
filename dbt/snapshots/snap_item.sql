{% snapshot snap_item %}

{{
    config(
        target_schema=target.schema,
        unique_key='item_number',
        strategy='check',
        check_cols=[
            'item_description',
            'category',
            'category_name',
            'category_family',
            'vendor_number',
            'vendor_name',
            'bottle_volume_ml',
            'pack',
            'state_bottle_cost',
            'state_bottle_retail'
        ]
    )
}}

select
    item_number,
    item_description,
    category,
    category_name,
    category_family,
    vendor_number,
    vendor_name,
    bottle_volume_ml,
    pack,
    state_bottle_cost,
    state_bottle_retail
from {{ ref('int_item_attributes_current') }}

{% endsnapshot %}