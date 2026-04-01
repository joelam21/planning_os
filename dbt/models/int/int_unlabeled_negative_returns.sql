-- dbt/models/int/int_unlabeled_negative_returns.sql
-- Identifies rows with negative values that do not use an RINV- invoice prefix.
-- These are treated as source-data anomalies and monitored for frequency/impact.

select
    invoice_item_number,
    order_date,
    store_number,
    item_number,
    bottles_sold,
    sale_dollars,
    volume_sold_liters,
    volume_sold_gallons,
    'unlabeled_negative_return' as anomaly_type,
    loaded_at

from {{ ref('fct_liquor_sales') }}

where
    (
        sale_dollars < 0
        or bottles_sold < 0
        or volume_sold_liters < 0
        or volume_sold_gallons < 0
    )
    and upper(invoice_item_number) not like 'RINV-%'