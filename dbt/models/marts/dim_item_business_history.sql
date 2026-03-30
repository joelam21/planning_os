-- Business-effective item history built from transactional business dates.
-- This captures attribute change points over order_date (not snapshot run time).

with daily_item_attributes as (
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
        order_date,
        loaded_at,
        row_number() over (
            partition by item_number, order_date
            order by loaded_at desc
        ) as rn
    from {{ ref('int_iowa_liquor_sales') }}
    where item_number is not null
      and order_date is not null
),

daily_latest as (
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
        order_date,
        loaded_at
    from daily_item_attributes
    where rn = 1
),

with_change_flags as (
    select
        *,
        lag(item_description) over (partition by item_number order by order_date, loaded_at) as prev_item_description,
        lag(category) over (partition by item_number order by order_date, loaded_at) as prev_category,
        lag(category_name) over (partition by item_number order by order_date, loaded_at) as prev_category_name,
        lag(vendor_number) over (partition by item_number order by order_date, loaded_at) as prev_vendor_number,
        lag(vendor_name) over (partition by item_number order by order_date, loaded_at) as prev_vendor_name,
        lag(bottle_volume_ml) over (partition by item_number order by order_date, loaded_at) as prev_bottle_volume_ml,
        lag(pack) over (partition by item_number order by order_date, loaded_at) as prev_pack,
        lag(state_bottle_cost) over (partition by item_number order by order_date, loaded_at) as prev_state_bottle_cost,
        lag(state_bottle_retail) over (partition by item_number order by order_date, loaded_at) as prev_state_bottle_retail
    from daily_latest
),

change_points as (
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
        order_date as business_valid_from,
        loaded_at as source_loaded_at
    from with_change_flags
    where
        prev_item_description is null
        or coalesce(item_description, '') <> coalesce(prev_item_description, '')
        or coalesce(category, '') <> coalesce(prev_category, '')
        or coalesce(category_name, '') <> coalesce(prev_category_name, '')
        or coalesce(vendor_number, '') <> coalesce(prev_vendor_number, '')
        or coalesce(vendor_name, '') <> coalesce(prev_vendor_name, '')
        or coalesce(bottle_volume_ml, -1) <> coalesce(prev_bottle_volume_ml, -1)
        or coalesce(pack, -1) <> coalesce(prev_pack, -1)
        or coalesce(state_bottle_cost, -1) <> coalesce(prev_state_bottle_cost, -1)
        or coalesce(state_bottle_retail, -1) <> coalesce(prev_state_bottle_retail, -1)
),

with_valid_to as (
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
        business_valid_from,
        dateadd(
            day,
            -1,
            lead(business_valid_from) over (partition by item_number order by business_valid_from)
        ) as business_valid_to,
        source_loaded_at
    from change_points
)

select
    h.item_number,
    h.item_description,
    h.category,
    h.category_name,
    coalesce(m.category_family, 'Other') as category_family,
    h.vendor_number,
    h.vendor_name,
    h.bottle_volume_ml,
    h.pack,
    h.state_bottle_cost,
    h.state_bottle_retail,
    h.business_valid_from,
    h.business_valid_to,
    case when h.business_valid_to is null then true else false end as is_current,
    h.source_loaded_at
from with_valid_to h
left join {{ ref('category_family_map') }} m
    on h.category = m.category