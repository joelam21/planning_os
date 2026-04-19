-- dbt/models/int/int_anomalous_returns.sql
-- Identifies return invoices (RINV-) with unexpected positive values
-- These are flagged for monitoring but not excluded from fact table
-- to preserve data lineage; periodically review frequency and impact

select
    invoice_item_number,
    order_date,
    store_number,
    item_number,
    bottles_sold,
    sale_dollars,
    volume_sold_liters,
    volume_sold_gallons,
    'positive_return' as anomaly_type,
    loaded_at

from {{ ref('int_iowa_liquor_sales_deduped') }}

where
    upper(invoice_item_number) like 'RINV-%'
    and (
        sale_dollars > 0
        or bottles_sold > 0
        or volume_sold_liters > 0
        or volume_sold_gallons > 0
    )
