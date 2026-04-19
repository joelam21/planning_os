{{
    config(
        materialized='incremental',
        unique_key='invoice_item_number',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

select
    invoice_item_number,
    order_date,
    store_number,
    item_number,
    vendor_number,
    category,

    bottles_sold,
    sale_dollars,
    volume_sold_liters,
    volume_sold_gallons,
    estimated_total_cost,
    estimated_gross_profit,

    upper(invoice_item_number) like 'RINV-%' as is_return,

    loaded_at

from {{ ref('int_iowa_liquor_sales_deduped') }}
{% if is_incremental() %}
    where loaded_at >= (
        select dateadd('day', -3, max(loaded_at))
        from {{ this }}
    )
{% endif %}
