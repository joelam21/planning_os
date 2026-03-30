-- dbt/tests/assert_negative_values_must_be_returns.sql
select *
from {{ ref('fct_liquor_sales') }}
where
  (sale_dollars < 0 or bottles_sold < 0 or volume_sold_liters < 0 or volume_sold_gallons < 0)
  and upper(invoice_item_number) not like 'RINV-%'