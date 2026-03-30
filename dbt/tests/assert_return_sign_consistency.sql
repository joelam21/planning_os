-- dbt/tests/assert_return_sign_consistency.sql
select *
from {{ ref('fct_liquor_sales') }}
where
  upper(invoice_item_number) like 'RINV-%'
  and (
    sale_dollars >= 0
    or bottles_sold >= 0
    or volume_sold_liters >= 0
    or volume_sold_gallons >= 0
  )