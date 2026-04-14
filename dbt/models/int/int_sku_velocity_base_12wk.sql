with trailing_12_weeks as (
    select
        item_number,
        sum(bottles_sold) as total_units_sold,
        sum(sale_dollars) as total_revenue
    from {{ ref('fct_liquor_sales') }}
    where order_date >= dateadd(
        week,
        -12,
        (select max(order_date) from {{ ref('fct_liquor_sales') }})
    )
    and upper(invoice_item_number) not like 'RINV-%'
    group by item_number
)

select *
from trailing_12_weeks