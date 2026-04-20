{{ config(tags=['critical']) }}

-- Every fact row must join to exactly one row in int_item_business_history.
-- Zero matches means a fact item has no history record (data gap).
-- Multiple matches means validity windows overlap (caught separately, but cross-validated here).
--
-- Failures here indicate either missing item history or a broken point-in-time join,
-- both of which would corrupt any analysis that uses historical item attributes.

with fact_join_counts as (
    select
        f.invoice_item_number,
        f.item_number,
        f.order_date,
        count(h.item_number) as matching_history_rows
    from {{ ref('fct_liquor_sales') }} f
    left join {{ ref('int_item_business_history') }} h
        on  f.item_number = h.item_number
        and f.order_date >= h.business_valid_from
        and (h.business_valid_to is null or f.order_date <= h.business_valid_to)
    group by f.invoice_item_number, f.item_number, f.order_date
)

select
    invoice_item_number,
    item_number,
    order_date,
    matching_history_rows
from fact_join_counts
where matching_history_rows != 1
