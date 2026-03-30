{% snapshot snap_item %}

{{
    config(
        target_schema='DEV',
        unique_key='item_number',
        strategy='check',
        check_cols=[
            'item_description',
            'category',
            'category_name',
            'vendor_number',
            'vendor_name',
            'bottle_volume_ml',
            'pack',
            'state_bottle_cost',
            'state_bottle_retail'
        ]
    )
}}

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
from latest_item_attributes
where rn = 1

{% endsnapshot %}