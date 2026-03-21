-- Reconcile store-day aggregates in fct_store_daily_sales against base fact rows.
-- Any returned row indicates aggregation drift.

with fact as (
    select
        store_number,
        order_date,
        sum(bottles_sold) as fact_total_bottles_sold,
        sum(sale_dollars) as fact_total_sales_dollars,
        sum(volume_sold_liters) as fact_total_volume_sold_liters,
        sum(volume_sold_gallons) as fact_total_volume_sold_gallons,
        sum(estimated_total_cost) as fact_total_estimated_cost,
        sum(estimated_gross_profit) as fact_total_estimated_gross_profit,
        count(*) as fact_invoice_line_count
    from {{ ref('fct_liquor_sales') }}
    group by 1, 2
),
daily as (
    select
        store_number,
        order_date,
        total_bottles_sold,
        total_sales_dollars,
        total_volume_sold_liters,
        total_volume_sold_gallons,
        total_estimated_cost,
        total_estimated_gross_profit,
        invoice_line_count
    from {{ ref('fct_store_daily_sales') }}
)

select
    coalesce(f.store_number, d.store_number) as store_number,
    coalesce(f.order_date, d.order_date) as order_date,
    f.fact_total_bottles_sold,
    d.total_bottles_sold,
    f.fact_total_sales_dollars,
    d.total_sales_dollars,
    f.fact_total_volume_sold_liters,
    d.total_volume_sold_liters,
    f.fact_total_volume_sold_gallons,
    d.total_volume_sold_gallons,
    f.fact_total_estimated_cost,
    d.total_estimated_cost,
    f.fact_total_estimated_gross_profit,
    d.total_estimated_gross_profit,
    f.fact_invoice_line_count,
    d.invoice_line_count
from fact f
full outer join daily d
    on f.store_number = d.store_number
   and f.order_date = d.order_date
where
    coalesce(f.fact_total_bottles_sold, 0) != coalesce(d.total_bottles_sold, 0)
    or coalesce(f.fact_total_sales_dollars, 0) != coalesce(d.total_sales_dollars, 0)
    or coalesce(f.fact_total_volume_sold_liters, 0) != coalesce(d.total_volume_sold_liters, 0)
    or coalesce(f.fact_total_volume_sold_gallons, 0) != coalesce(d.total_volume_sold_gallons, 0)
    or coalesce(f.fact_total_estimated_cost, 0) != coalesce(d.total_estimated_cost, 0)
    or coalesce(f.fact_total_estimated_gross_profit, 0) != coalesce(d.total_estimated_gross_profit, 0)
    or coalesce(f.fact_invoice_line_count, 0) != coalesce(d.invoice_line_count, 0)